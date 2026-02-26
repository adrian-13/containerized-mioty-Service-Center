import csv
import io
import json
import logging
import math
import os
import base64
import queue
import re
import subprocess
import shutil
import threading
import time
import tempfile
import zipfile
from collections import deque
import urllib.parse
import urllib.error
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, Response, session, send_file, has_request_context, make_response
from functools import wraps
from typing import List, Dict, Any
import bssci_config
try:
    import psycopg
except Exception:
    psycopg = None

# Global service instance references
tls_server_instance = None
mqtt_client_instance = None

# Uptime tracking
bs_uptime_events = {}
_last_known_bs_status = {}
_influx_snapshot_thread = None
_influx_snapshot_stop = threading.Event()
_timescale_schema_lock = threading.Lock()
_timescale_schema_ready = False
_TIMESCALE_UPLINK_QUEUE_MAXSIZE = max(100, int(getattr(bssci_config, "TIMESCALE_UPLINK_QUEUE_MAXSIZE", 10000) or 10000))
_TIMESCALE_UPLINK_BATCH_SIZE = max(1, int(getattr(bssci_config, "TIMESCALE_UPLINK_BATCH_SIZE", 200) or 200))
_TIMESCALE_UPLINK_MAX_RETRIES = max(0, int(getattr(bssci_config, "TIMESCALE_UPLINK_WRITE_MAX_RETRIES", 5) or 5))
_TIMESCALE_UPLINK_RETRY_BASE_SECONDS = max(
    0.1, float(getattr(bssci_config, "TIMESCALE_UPLINK_RETRY_BASE_SECONDS", 0.5) or 0.5)
)
_TIMESCALE_UPLINK_RETRY_MAX_SECONDS = max(
    _TIMESCALE_UPLINK_RETRY_BASE_SECONDS,
    float(getattr(bssci_config, "TIMESCALE_UPLINK_RETRY_MAX_SECONDS", 10.0) or 10.0),
)
_timescale_uplink_queue = queue.Queue(maxsize=_TIMESCALE_UPLINK_QUEUE_MAXSIZE)
_timescale_uplink_thread = None
_timescale_uplink_stop = threading.Event()
_timescale_uplink_lock = threading.Lock()
_timescale_uplink_stats = {
    "queued": 0,
    "written": 0,
    "failed_batches": 0,
    "dropped": 0,
    "dropped_write_failures": 0,
    "dropped_invalid": 0,
    "retry_attempts": 0,
    "retried_batches": 0,
    "backpressure_events": 0,
    "queue_high_water": 0,
    "last_retry_ts": 0.0,
    "last_retry_delay_sec": 0.0,
    "last_error": "",
    "last_write_ts": 0.0,
    "write_latency_last_ms": 0.0,
    "write_latency_avg_ms": 0.0,
    "write_latency_max_ms": 0.0,
    "write_latency_samples": 0,
    "write_latency_total_ms": 0.0,
    "max_retries": _TIMESCALE_UPLINK_MAX_RETRIES,
    "batch_size": _TIMESCALE_UPLINK_BATCH_SIZE,
}
_timescale_uplink_stats_lock = threading.Lock()

def record_bs_event(eui, event_type):
    eui = eui.lower()
    if eui not in bs_uptime_events:
        bs_uptime_events[eui] = []
    bs_uptime_events[eui].append({
        "event": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    if len(bs_uptime_events[eui]) > 500:
        bs_uptime_events[eui] = bs_uptime_events[eui][-500:]

def _track_bs_status_changes(current_statuses):
    global _last_known_bs_status
    for eui, status in current_statuses.items():
        prev = _last_known_bs_status.get(eui)
        if prev != status:
            if status == "connected":
                record_bs_event(eui, "connected")
            elif prev == "connected" and status != "connected":
                record_bs_event(eui, "disconnected")
    _last_known_bs_status = dict(current_statuses)

def _normalize_uptime_event(value):
    """Normalize various event/status payloads to connected/disconnected."""
    raw = str(value or "").strip().lower()
    if raw in {"connected", "connect", "up", "online", "true", "1"}:
        return "connected"
    if raw in {"disconnected", "disconnect", "down", "offline", "false", "0"}:
        return "disconnected"
    return raw

def _extract_influx_uptime_events(csv_text):
    """
    Parse Influx CSV output into {eui: [{event, timestamp}, ...]}.
    Accepts common column names:
      - time: _time|time|timestamp
      - eui: eui|bs_eui|base_station|base_station_eui|gateway|host
      - event: event|status|_value|value
    """
    lines = []
    for line in csv_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        lines.append(line)
    if not lines:
        return {}

    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    result = {}

    for row in reader:
        time_value = (
            row.get("_time")
            or row.get("time")
            or row.get("timestamp")
            or ""
        ).strip()
        eui_value = (
            row.get("eui")
            or row.get("bs_eui")
            or row.get("base_station")
            or row.get("base_station_eui")
            or row.get("gateway")
            or row.get("host")
            or ""
        ).strip().lower()
        event_value = (
            row.get("event")
            or row.get("status")
            or row.get("_value")
            or row.get("value")
            or ""
        ).strip()

        if not eui_value or not time_value or not event_value:
            continue

        normalized = _normalize_uptime_event(event_value)
        if normalized not in {"connected", "disconnected"}:
            continue

        result.setdefault(eui_value, []).append({
            "event": normalized,
            "timestamp": time_value
        })

    for eui in list(result.keys()):
        result[eui].sort(key=lambda item: item.get("timestamp", ""))
    return result

def _build_default_influx_uptime_query():
    bucket = bssci_config.INFLUXDB_BUCKET
    measurement = bssci_config.INFLUX_UPTIME_MEASUREMENT
    field = bssci_config.INFLUX_UPTIME_FIELD
    eui_tag = bssci_config.INFLUX_UPTIME_EUI_TAG
    if not bucket:
        return ""

    # Query expects event/status value in _value and eui in tag column.
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -24h)\n'
        f'  |> filter(fn: (r) => r._measurement == "{measurement}")\n'
        f'  |> filter(fn: (r) => r._field == "{field}")\n'
        f'  |> keep(columns: ["_time", "_value", "{eui_tag}"])\n'
        f'  |> rename(columns: {{"{eui_tag}": "eui"}})\n'
        f'  |> sort(columns: ["_time"])'
    )

def _get_influx_uptime_events():
    """
    Returns dict:
      {
        success: bool,
        uptime_events: {...},
        source: str,
        error?: str
      }
    """
    influx_url = bssci_config.INFLUXDB_URL.rstrip("/")
    influx_org = bssci_config.INFLUXDB_ORG
    influx_token = bssci_config.INFLUXDB_TOKEN

    if not influx_url or not influx_org or not influx_token:
        return {
            "success": False,
            "source": "influxdb",
            "error": "InfluxDB configuration is incomplete (URL/ORG/TOKEN)."
        }

    flux_query = bssci_config.INFLUX_UPTIME_QUERY or _build_default_influx_uptime_query()
    if not flux_query:
        return {
            "success": False,
            "source": "influxdb",
            "error": "No Influx uptime query configured (bucket/query missing)."
        }

    query_url = f"{influx_url}/api/v2/query?org={urllib.parse.quote(influx_org)}"
    req = urllib.request.Request(
        query_url,
        method="POST",
        data=flux_query.encode("utf-8"),
        headers={
            "Authorization": f"Token {influx_token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        }
    )

    ssl_ctx = None
    if not bssci_config.INFLUXDB_VERIFY_SSL:
        ssl_ctx = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as response:
            payload = response.read().decode("utf-8", errors="ignore")
        events = _extract_influx_uptime_events(payload)
        return {
            "success": True,
            "source": "influxdb",
            "uptime_events": events
        }
    except Exception as exc:
        return {
            "success": False,
            "source": "influxdb",
            "error": str(exc)
        }

def _influx_write_is_ready():
    return bool(
        bssci_config.INFLUXDB_URL
        and bssci_config.INFLUXDB_ORG
        and bssci_config.INFLUXDB_BUCKET
        and bssci_config.INFLUXDB_TOKEN
    )

def _lp_escape_measurement(value):
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")

def _lp_escape_tag(value):
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace(" ", "\\ ")
        .replace("=", "\\=")
    )

def _lp_escape_field_key(value):
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")

def _lp_encode_field_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return repr(value)
    if value is None:
        return None
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'

def _build_influx_line(measurement, tags, fields, timestamp_ns=None):
    line = _lp_escape_measurement(measurement)

    tag_parts = []
    for key, value in (tags or {}).items():
        if value is None:
            continue
        tag_parts.append(f"{_lp_escape_tag(key)}={_lp_escape_tag(value)}")
    if tag_parts:
        line += "," + ",".join(tag_parts)

    field_parts = []
    for key, value in (fields or {}).items():
        encoded = _lp_encode_field_value(value)
        if encoded is None:
            continue
        field_parts.append(f"{_lp_escape_field_key(key)}={encoded}")
    if not field_parts:
        return None

    line += " " + ",".join(field_parts)
    if timestamp_ns is not None:
        line += f" {int(timestamp_ns)}"
    return line

def _write_influx_lines(lines):
    if not _influx_write_is_ready():
        return False, "InfluxDB write config missing (URL/ORG/BUCKET/TOKEN)."
    if not lines:
        return False, "No line protocol payload to write."

    influx_url = bssci_config.INFLUXDB_URL.rstrip("/")
    write_url = (
        f"{influx_url}/api/v2/write"
        f"?org={urllib.parse.quote(bssci_config.INFLUXDB_ORG)}"
        f"&bucket={urllib.parse.quote(bssci_config.INFLUXDB_BUCKET)}"
        "&precision=ns"
    )
    payload = "\n".join(lines).encode("utf-8")
    req = urllib.request.Request(
        write_url,
        method="POST",
        data=payload,
        headers={
            "Authorization": f"Token {bssci_config.INFLUXDB_TOKEN}",
            "Content-Type": "text/plain; charset=utf-8",
            "Accept": "application/json",
        }
    )

    ssl_ctx = None
    if not bssci_config.INFLUXDB_VERIFY_SSL:
        ssl_ctx = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=5):
            pass
        return True, None
    except Exception as exc:
        return False, str(exc)

def _sanitize_inventory_field_key(key):
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(key or "").strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
    return sanitized

def _normalize_inventory_fields(data):
    normalized = {}
    for key, value in (data or {}).items():
        field_key = _sanitize_inventory_field_key(key)
        if not field_key:
            continue
        if isinstance(value, (dict, list)):
            normalized[field_key] = json.dumps(value, separators=(",", ":"), ensure_ascii=True)
        elif isinstance(value, (str, int, float, bool)) or value is None:
            normalized[field_key] = value
        else:
            normalized[field_key] = str(value)
    return normalized

def _default_tenant_id():
    raw = str(getattr(bssci_config, "TIMESCALE_DEFAULT_TENANT", "default") or "default").strip().lower()
    raw = re.sub(r"[^a-z0-9:_-]+", "-", raw)
    raw = raw.strip("-_:")
    return raw or "default"

def _sanitize_tenant_id(value):
    candidate = str(value or "").strip().lower()
    candidate = re.sub(r"[^a-z0-9:_-]+", "-", candidate)
    candidate = candidate.strip("-_:")
    if not candidate:
        return ""
    return candidate[:64]

def _normalize_tenant_id(value, fallback=None):
    base = _default_tenant_id() if fallback is None else str(fallback or "").strip().lower()
    base = base or _default_tenant_id()
    candidate = _sanitize_tenant_id(value)
    if not candidate:
        return base
    return candidate

def _tenant_id_from_sensor(sensor):
    return _normalize_tenant_id((sensor or {}).get("tenant_id"), fallback=_default_tenant_id())

def _tenant_id_from_base_station(bs_data):
    return _normalize_tenant_id((bs_data or {}).get("tenant_id"), fallback=_default_tenant_id())

def _tenant_matches(record_tenant, active_tenant):
    record_value = _normalize_tenant_id(record_tenant, fallback=_default_tenant_id())
    active_value = _normalize_tenant_id(active_tenant, fallback=_default_tenant_id())
    return record_value == active_value

def _active_tenant_id():
    fallback = _default_tenant_id()
    if not has_request_context():
        return fallback

    session_tenant = _normalize_tenant_id(session.get("tenant_id"), fallback=fallback)
    requested_tenant = request.headers.get("X-Tenant-Id")
    if requested_tenant and str(session.get("role", "")).strip().lower() == "admin":
        return _normalize_tenant_id(requested_tenant, fallback=session_tenant)
    return session_tenant or fallback

def _normalize_user_role(role):
    normalized = str(role or "viewer").strip().lower()
    return normalized or "viewer"

def _normalize_user_tenant_for_role(role, tenant_id, fallback=None):
    if _normalize_user_role(role) == "admin":
        return ""
    return _normalize_tenant_id(tenant_id, fallback=fallback or _default_tenant_id())

def _user_belongs_to_tenant(user, tenant_id, fallback=None):
    if not isinstance(user, dict):
        return False
    role = _normalize_user_role(user.get("role", "viewer"))
    if role == "admin":
        return False
    user_tenant = _normalize_user_tenant_for_role(
        role,
        user.get("tenant_id"),
        fallback=fallback or _default_tenant_id(),
    )
    return _tenant_matches(user_tenant, tenant_id)

def _timescale_is_ready():
    if not getattr(bssci_config, "TIMESCALE_ENABLED", False):
        return False, "Timescale disabled."
    if psycopg is None:
        return False, "psycopg driver not installed."
    required = {
        "TIMESCALE_HOST": getattr(bssci_config, "TIMESCALE_HOST", ""),
        "TIMESCALE_DB": getattr(bssci_config, "TIMESCALE_DB", ""),
        "TIMESCALE_USER": getattr(bssci_config, "TIMESCALE_USER", ""),
        "TIMESCALE_PASSWORD": getattr(bssci_config, "TIMESCALE_PASSWORD", ""),
    }
    missing = [key for key, value in required.items() if not str(value or "").strip()]
    if missing:
        return False, f"Timescale config missing: {', '.join(missing)}"
    return True, None

def _timescale_connect():
    ok, err = _timescale_is_ready()
    if not ok:
        return None, err
    try:
        conn = psycopg.connect(
            host=getattr(bssci_config, "TIMESCALE_HOST", "timescaledb"),
            port=int(getattr(bssci_config, "TIMESCALE_PORT", 5432)),
            dbname=getattr(bssci_config, "TIMESCALE_DB", "bssci"),
            user=getattr(bssci_config, "TIMESCALE_USER", "bssci_user"),
            password=getattr(bssci_config, "TIMESCALE_PASSWORD", ""),
            sslmode=getattr(bssci_config, "TIMESCALE_SSLMODE", "disable"),
            connect_timeout=5,
        )
        conn.autocommit = True
        return conn, None
    except Exception as exc:
        return None, str(exc)

def _timescale_apply_policies(cur):
    retention_enabled = bool(getattr(bssci_config, "TIMESCALE_RETENTION_ENABLED", True))
    compression_enabled = bool(getattr(bssci_config, "TIMESCALE_COMPRESSION_ENABLED", True))
    telemetry_retention_days = max(1, int(getattr(bssci_config, "TIMESCALE_TELEMETRY_RETENTION_DAYS", 90)))
    inventory_retention_days = max(1, int(getattr(bssci_config, "TIMESCALE_INVENTORY_RETENTION_DAYS", 365)))
    compression_after_days = max(1, int(getattr(bssci_config, "TIMESCALE_COMPRESSION_AFTER_DAYS", 7)))

    if compression_enabled:
        for table_name, segment_by in (
            ("telemetry_uplink", "tenant_id,sensor_eui,base_station_eui"),
            ("inventory_events", "tenant_id,entity_type,eui"),
            ("inventory_snapshot_points", "tenant_id,entity_type,eui"),
        ):
            try:
                cur.execute(f"""
                    ALTER TABLE {table_name} SET (
                        timescaledb.compress,
                        timescaledb.compress_orderby = 'ts DESC',
                        timescaledb.compress_segmentby = '{segment_by}'
                    )
                """)
            except Exception:
                pass

        for table_name in ("telemetry_uplink", "inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute(
                    "SELECT add_compression_policy(%s, make_interval(days => %s), if_not_exists => TRUE)",
                    (table_name, compression_after_days),
                )
            except Exception:
                pass
    else:
        for table_name in ("telemetry_uplink", "inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute("SELECT remove_compression_policy(%s, if_exists => TRUE)", (table_name,))
            except Exception:
                pass

    if retention_enabled:
        try:
            cur.execute(
                "SELECT add_retention_policy('telemetry_uplink', make_interval(days => %s), if_not_exists => TRUE)",
                (telemetry_retention_days,),
            )
        except Exception:
            pass
        for table_name in ("inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute(
                    "SELECT add_retention_policy(%s, make_interval(days => %s), if_not_exists => TRUE)",
                    (table_name, inventory_retention_days),
                )
            except Exception:
                pass
    else:
        for table_name in ("telemetry_uplink", "inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute("SELECT remove_retention_policy(%s, if_exists => TRUE)", (table_name,))
            except Exception:
                pass

def _ensure_timescale_schema(conn):
    global _timescale_schema_ready
    if _timescale_schema_ready:
        return
    with _timescale_schema_lock:
        if _timescale_schema_ready:
            return
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenants (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO tenants (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, ("default", "Default Tenant"))
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory_events (
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    entity_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    eui TEXT NOT NULL,
                    actor TEXT NOT NULL DEFAULT 'system',
                    event TEXT NOT NULL,
                    has_payload BOOLEAN NOT NULL DEFAULT FALSE,
                    payload_size INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT 'service_center_ui',
                    payload JSONB
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_events_tenant_ts ON inventory_events (tenant_id, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_events_eui_ts ON inventory_events (eui, ts DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory_snapshot_points (
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    entity_type TEXT NOT NULL,
                    eui TEXT NOT NULL,
                    status TEXT,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_points_tenant_ts ON inventory_snapshot_points (tenant_id, ts DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory_snapshot_latest (
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    entity_type TEXT NOT NULL,
                    eui TEXT NOT NULL,
                    status TEXT,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, entity_type, eui)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_latest_updated ON inventory_snapshot_latest (tenant_id, updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_uplink (
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    sensor_eui TEXT NOT NULL,
                    base_station_eui TEXT,
                    snr DOUBLE PRECISION,
                    rssi DOUBLE PRECISION,
                    packet_loss_pct DOUBLE PRECISION,
                    packet_cnt BIGINT,
                    msg_type TEXT NOT NULL DEFAULT 'ul',
                    payload JSONB
                )
            """)
            cur.execute("ALTER TABLE telemetry_uplink ADD COLUMN IF NOT EXISTS packet_cnt BIGINT")
            cur.execute("ALTER TABLE telemetry_uplink ADD COLUMN IF NOT EXISTS msg_type TEXT NOT NULL DEFAULT 'ul'")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_ts ON telemetry_uplink (tenant_id, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_sensor_ts ON telemetry_uplink (tenant_id, sensor_eui, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_bs_ts ON telemetry_uplink (tenant_id, base_station_eui, ts DESC)")
            try:
                cur.execute("SELECT create_hypertable('inventory_events', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                pass
            try:
                cur.execute("SELECT create_hypertable('inventory_snapshot_points', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                pass
            try:
                cur.execute("SELECT create_hypertable('telemetry_uplink', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                # Keep plain table if Timescale extension privileges are unavailable.
                pass
            _timescale_apply_policies(cur)
        _timescale_schema_ready = True

def _timescale_telemetry_enabled():
    return bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)) and bool(
        getattr(bssci_config, "TIMESCALE_TELEMETRY_WRITE_ENABLED", True)
    )

def _note_timescale_uplink_stats(**kwargs):
    with _timescale_uplink_stats_lock:
        for key, value in kwargs.items():
            _timescale_uplink_stats[key] = value

def _bump_timescale_uplink_stats(key, delta=1):
    with _timescale_uplink_stats_lock:
        _timescale_uplink_stats[key] = int(_timescale_uplink_stats.get(key, 0) or 0) + int(delta)

def _timescale_observe_queue():
    qsize = _timescale_uplink_queue.qsize()
    with _timescale_uplink_stats_lock:
        current = int(_timescale_uplink_stats.get("queue_high_water", 0) or 0)
        if qsize > current:
            _timescale_uplink_stats["queue_high_water"] = qsize
    return qsize


def _observe_timescale_write_latency(latency_ms: float):
    value = max(0.0, float(latency_ms or 0.0))
    with _timescale_uplink_stats_lock:
        total = float(_timescale_uplink_stats.get("write_latency_total_ms", 0.0) or 0.0) + value
        samples = int(_timescale_uplink_stats.get("write_latency_samples", 0) or 0) + 1
        max_seen = max(float(_timescale_uplink_stats.get("write_latency_max_ms", 0.0) or 0.0), value)
        _timescale_uplink_stats["write_latency_total_ms"] = total
        _timescale_uplink_stats["write_latency_samples"] = samples
        _timescale_uplink_stats["write_latency_last_ms"] = round(value, 2)
        _timescale_uplink_stats["write_latency_max_ms"] = round(max_seen, 2)
        _timescale_uplink_stats["write_latency_avg_ms"] = round(total / samples, 2) if samples > 0 else 0.0

def _timescale_retry_delay(attempt_number: int) -> float:
    if attempt_number <= 0:
        return 0.0
    raw_delay = _TIMESCALE_UPLINK_RETRY_BASE_SECONDS * (2 ** (attempt_number - 1))
    return min(_TIMESCALE_UPLINK_RETRY_MAX_SECONDS, raw_delay)

def get_timescale_uplink_runtime_stats():
    with _timescale_uplink_stats_lock:
        stats = dict(_timescale_uplink_stats)
    queue_size = _timescale_uplink_queue.qsize()
    queue_max = int(getattr(_timescale_uplink_queue, "maxsize", 0) or 0)
    stats["queue_size"] = queue_size
    stats["queue_maxsize"] = queue_max
    stats["queue_utilization_pct"] = round((queue_size / queue_max) * 100.0, 2) if queue_max > 0 else None
    return stats

def _timescale_uplink_worker():
    logger.info("Timescale uplink worker started")
    while not _timescale_uplink_stop.is_set():
        try:
            first = _timescale_uplink_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        batch = [first]
        while len(batch) < _TIMESCALE_UPLINK_BATCH_SIZE:
            try:
                batch.append(_timescale_uplink_queue.get_nowait())
            except queue.Empty:
                break

        if not _timescale_telemetry_enabled():
            _bump_timescale_uplink_stats("failed_batches", 1)
            _bump_timescale_uplink_stats("dropped_write_failures", len(batch))
            _note_timescale_uplink_stats(last_error="telemetry writes disabled; batch discarded")
            continue

        rows = []
        for item in batch:
            try:
                ts_value = item.get("ts")
                if isinstance(ts_value, datetime):
                    ts = ts_value.astimezone(timezone.utc)
                elif isinstance(ts_value, (int, float)):
                    value = float(ts_value)
                    if value > 1_000_000_000_000:  # nanoseconds
                        value = value / 1_000_000_000.0
                    ts = datetime.fromtimestamp(value, tz=timezone.utc)
                else:
                    ts = datetime.now(timezone.utc)
                rows.append((
                    ts,
                    _normalize_tenant_id(item.get("tenant_id"), fallback=_default_tenant_id()),
                    str(item.get("sensor_eui") or "").lower(),
                    (str(item.get("base_station_eui") or "").lower() or None),
                    float(item["snr"]) if item.get("snr") is not None else None,
                    float(item["rssi"]) if item.get("rssi") is not None else None,
                    float(item["packet_loss_pct"]) if item.get("packet_loss_pct") is not None else None,
                    int(item["packet_cnt"]) if item.get("packet_cnt") is not None else None,
                    str(item.get("msg_type") or "ul")[:16],
                    json.dumps(item.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                ))
            except Exception as row_exc:
                _bump_timescale_uplink_stats("dropped_invalid", 1)
                _note_timescale_uplink_stats(last_error=f"invalid telemetry row: {row_exc}")
                logger.warning("Timescale telemetry row dropped (invalid payload): %s", row_exc)

        if not rows:
            continue

        write_success = False
        had_retry = False
        final_error = ""

        for attempt in range(_TIMESCALE_UPLINK_MAX_RETRIES + 1):
            conn = None
            attempt_started = time.perf_counter()
            try:
                conn, err = _timescale_connect()
                if conn is None:
                    final_error = f"connect failed: {err}"
                else:
                    _ensure_timescale_schema(conn)
                    with conn.cursor() as cur:
                        tenant_ids = sorted({row[1] for row in rows if str(row[1]).strip()})
                        if tenant_ids:
                            cur.executemany("""
                                INSERT INTO tenants (id, name)
                                VALUES (%s, %s)
                                ON CONFLICT (id) DO NOTHING
                            """, [(tenant_id, tenant_id) for tenant_id in tenant_ids])
                        cur.executemany("""
                            INSERT INTO telemetry_uplink
                                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, rows)
                    write_success = True
                    _observe_timescale_write_latency((time.perf_counter() - attempt_started) * 1000.0)
                    break
            except Exception as exc:
                final_error = str(exc)
            finally:
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass

            if attempt < _TIMESCALE_UPLINK_MAX_RETRIES:
                had_retry = True
                delay = _timescale_retry_delay(attempt + 1)
                _bump_timescale_uplink_stats("retry_attempts", 1)
                _note_timescale_uplink_stats(
                    last_error=final_error,
                    last_retry_ts=time.time(),
                    last_retry_delay_sec=delay,
                )
                logger.warning(
                    "Timescale uplink write retry %s/%s in %.2fs (batch=%s, error=%s)",
                    attempt + 1,
                    _TIMESCALE_UPLINK_MAX_RETRIES,
                    delay,
                    len(rows),
                    final_error,
                )
                time.sleep(delay)

        if write_success:
            _bump_timescale_uplink_stats("written", len(rows))
            _note_timescale_uplink_stats(last_error="", last_write_ts=time.time())
            if had_retry:
                _bump_timescale_uplink_stats("retried_batches", 1)
        else:
            _bump_timescale_uplink_stats("failed_batches", 1)
            _bump_timescale_uplink_stats("dropped_write_failures", len(rows))
            _note_timescale_uplink_stats(last_error=final_error)
            logger.error("Timescale uplink write dropped batch=%s after retries: %s", len(rows), final_error)

    logger.info("Timescale uplink worker stopped")

def _ensure_timescale_uplink_worker_started():
    global _timescale_uplink_thread
    if _timescale_uplink_thread and _timescale_uplink_thread.is_alive():
        return
    with _timescale_uplink_lock:
        if _timescale_uplink_thread and _timescale_uplink_thread.is_alive():
            return
        _timescale_uplink_stop.clear()
        _timescale_uplink_thread = threading.Thread(target=_timescale_uplink_worker, daemon=True)
        _timescale_uplink_thread.start()

def record_runtime_uplink_telemetry(sensor_eui, base_station_eui=None, snr=None, rssi=None, packet_loss_pct=None, payload=None, packet_cnt=None, msg_type="ul", ts=None, tenant_id=None):
    if not _timescale_telemetry_enabled():
        return False, "Timescale telemetry writes disabled."
    sensor_key = str(sensor_eui or "").strip().lower()
    if not sensor_key:
        return False, "sensor_eui is required"
    resolved_tenant = _normalize_tenant_id(
        tenant_id if tenant_id is not None else _resolve_sensor_tenant(sensor_key),
        fallback=_default_tenant_id()
    )
    _ensure_timescale_uplink_worker_started()
    item = {
        "tenant_id": resolved_tenant,
        "sensor_eui": sensor_key,
        "base_station_eui": str(base_station_eui or "").strip().lower(),
        "snr": snr,
        "rssi": rssi,
        "packet_loss_pct": packet_loss_pct,
        "packet_cnt": packet_cnt,
        "msg_type": str(msg_type or "ul").lower(),
        "payload": payload or {},
        "ts": ts if ts is not None else time.time(),
    }
    try:
        _timescale_uplink_queue.put_nowait(item)
        _bump_timescale_uplink_stats("queued", 1)
        _timescale_observe_queue()
        return True, None
    except queue.Full:
        _bump_timescale_uplink_stats("backpressure_events", 1)
        _bump_timescale_uplink_stats("dropped", 1)
        _note_timescale_uplink_stats(last_error="uplink queue full")
        return False, "queue full"

def _timescale_fetch_telemetry_summary(window_minutes=60, bucket_seconds=60, top_limit=10):
    window_minutes = max(1, min(int(window_minutes or 60), 7 * 24 * 60))
    bucket_seconds = max(15, min(int(bucket_seconds or 60), 3600))
    top_limit = max(1, min(int(top_limit or 10), 100))
    ok, err = _timescale_is_ready()
    if not ok:
        return {
            "success": False,
            "enabled": bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)),
            "error": err,
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
        }

    conn, conn_err = _timescale_connect()
    if conn is None:
        return {
            "success": False,
            "enabled": True,
            "error": conn_err,
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
        }

    tenant_id = _active_tenant_id()
    try:
        _ensure_timescale_schema(conn)
        summary = {
            "success": True,
            "enabled": True,
            "error": None,
            "tenant_id": tenant_id,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
        }
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)::BIGINT AS uplink_total,
                    COUNT(DISTINCT sensor_eui)::BIGINT AS sensor_count,
                    (COUNT(DISTINCT base_station_eui) FILTER (WHERE base_station_eui IS NOT NULL AND base_station_eui <> ''))::BIGINT AS base_station_count,
                    MAX(ts) AS last_ts
                FROM telemetry_uplink
                WHERE tenant_id = %s
                  AND ts >= NOW() - (%s * INTERVAL '1 minute')
            """, (tenant_id, window_minutes))
            row = cur.fetchone() or (0, 0, 0, None)
            summary["uplink_total"] = int(row[0] or 0)
            summary["sensor_count"] = int(row[1] or 0)
            summary["base_station_count"] = int(row[2] or 0)
            summary["last_ts"] = row[3].isoformat() if row[3] else None

            bucket_interval = f"{bucket_seconds} seconds"
            cur.execute("""
                SELECT
                    EXTRACT(EPOCH FROM bucket)::BIGINT AS bucket_ts,
                    COUNT(*)::BIGINT AS uplinks,
                    AVG(snr)::DOUBLE PRECISION AS avg_snr,
                    AVG(rssi)::DOUBLE PRECISION AS avg_rssi
                FROM (
                    SELECT time_bucket(%s::interval, ts) AS bucket, snr, rssi
                    FROM telemetry_uplink
                    WHERE tenant_id = %s
                      AND ts >= NOW() - (%s * INTERVAL '1 minute')
                ) t
                GROUP BY bucket
                ORDER BY bucket ASC
            """, (bucket_interval, tenant_id, window_minutes))
            summary["series"] = [
                {
                    "timestamp": int(series_row[0]),
                    "uplinks": int(series_row[1] or 0),
                    "avg_snr": float(series_row[2]) if series_row[2] is not None else None,
                    "avg_rssi": float(series_row[3]) if series_row[3] is not None else None,
                }
                for series_row in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT
                    sensor_eui,
                    COUNT(*)::BIGINT AS uplinks,
                    AVG(snr)::DOUBLE PRECISION AS avg_snr,
                    AVG(rssi)::DOUBLE PRECISION AS avg_rssi,
                    AVG(packet_loss_pct)::DOUBLE PRECISION AS avg_loss_pct,
                    MAX(ts) AS last_seen
                FROM telemetry_uplink
                WHERE tenant_id = %s
                  AND ts >= NOW() - (%s * INTERVAL '1 minute')
                GROUP BY sensor_eui
                ORDER BY uplinks DESC
                LIMIT %s
            """, (tenant_id, window_minutes, top_limit))
            summary["top_sensors"] = [
                {
                    "sensor_eui": str(sensor_row[0] or "").lower(),
                    "uplinks": int(sensor_row[1] or 0),
                    "avg_snr": float(sensor_row[2]) if sensor_row[2] is not None else None,
                    "avg_rssi": float(sensor_row[3]) if sensor_row[3] is not None else None,
                    "avg_packet_loss_pct": float(sensor_row[4]) if sensor_row[4] is not None else None,
                    "last_seen": sensor_row[5].isoformat() if sensor_row[5] else None,
                }
                for sensor_row in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT
                    ts,
                    sensor_eui,
                    base_station_eui,
                    packet_cnt,
                    msg_type,
                    snr,
                    rssi,
                    packet_loss_pct
                FROM telemetry_uplink
                WHERE tenant_id = %s
                  AND ts >= NOW() - (%s * INTERVAL '1 minute')
                ORDER BY ts DESC
                LIMIT 200
            """, (tenant_id, window_minutes))
            summary["recent_messages"] = [
                {
                    "ts": row[0].isoformat() if row[0] else None,
                    "sensor_eui": str(row[1] or "").lower(),
                    "base_station_eui": str(row[2] or "").lower() if row[2] else None,
                    "packet_cnt": int(row[3]) if row[3] is not None else None,
                    "msg_type": str(row[4] or "ul"),
                    "snr": float(row[5]) if row[5] is not None else None,
                    "rssi": float(row[6]) if row[6] is not None else None,
                    "packet_loss_pct": float(row[7]) if row[7] is not None else None,
                }
                for row in (cur.fetchall() or [])
            ]
        return summary
    except Exception as exc:
        return {
            "success": False,
            "enabled": True,
            "error": str(exc),
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _parse_bool_arg(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

def _timescale_fetch_tenant_dump(tenant_id, telemetry_limit=50000, events_limit=50000):
    telemetry_limit = max(1, min(int(telemetry_limit or 50000), 250000))
    events_limit = max(1, min(int(events_limit or 50000), 250000))
    conn, err = _timescale_connect()
    if conn is None:
        return False, err, {}
    try:
        _ensure_timescale_schema(conn)
        dump = {
            "tenant_id": tenant_id,
            "inventory_events": [],
            "inventory_snapshot_points": [],
            "inventory_snapshot_latest": [],
            "telemetry_uplink": [],
        }
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, created_at FROM tenants WHERE id = %s", (tenant_id,))
            row = cur.fetchone()
            if row:
                dump["tenant"] = {"id": row[0], "name": row[1], "created_at": str(row[2])}
            else:
                dump["tenant"] = {"id": tenant_id, "name": tenant_id, "created_at": None}

            cur.execute("""
                SELECT ts, tenant_id, entity_type, action, eui, actor, event, has_payload, payload_size, source, payload
                FROM inventory_events
                WHERE tenant_id = %s
                ORDER BY ts DESC
                LIMIT %s
            """, (tenant_id, events_limit))
            dump["inventory_events"] = [
                {
                    "ts": str(r[0]),
                    "tenant_id": r[1],
                    "entity_type": r[2],
                    "action": r[3],
                    "eui": r[4],
                    "actor": r[5],
                    "event": r[6],
                    "has_payload": bool(r[7]),
                    "payload_size": int(r[8] or 0),
                    "source": r[9],
                    "payload": r[10] or {},
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT ts, tenant_id, entity_type, eui, status, trigger, payload
                FROM inventory_snapshot_points
                WHERE tenant_id = %s
                ORDER BY ts DESC
                LIMIT %s
            """, (tenant_id, events_limit))
            dump["inventory_snapshot_points"] = [
                {
                    "ts": str(r[0]),
                    "tenant_id": r[1],
                    "entity_type": r[2],
                    "eui": r[3],
                    "status": r[4],
                    "trigger": r[5],
                    "payload": r[6] or {},
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT tenant_id, entity_type, eui, status, trigger, payload, updated_at
                FROM inventory_snapshot_latest
                WHERE tenant_id = %s
                ORDER BY updated_at DESC
            """, (tenant_id,))
            dump["inventory_snapshot_latest"] = [
                {
                    "tenant_id": r[0],
                    "entity_type": r[1],
                    "eui": r[2],
                    "status": r[3],
                    "trigger": r[4],
                    "payload": r[5] or {},
                    "updated_at": str(r[6]),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload
                FROM telemetry_uplink
                WHERE tenant_id = %s
                ORDER BY ts DESC
                LIMIT %s
            """, (tenant_id, telemetry_limit))
            dump["telemetry_uplink"] = [
                {
                    "ts": str(r[0]),
                    "tenant_id": r[1],
                    "sensor_eui": r[2],
                    "base_station_eui": r[3],
                    "snr": float(r[4]) if r[4] is not None else None,
                    "rssi": float(r[5]) if r[5] is not None else None,
                    "packet_loss_pct": float(r[6]) if r[6] is not None else None,
                    "packet_cnt": int(r[7]) if r[7] is not None else None,
                    "msg_type": r[8],
                    "payload": r[9] or {},
                }
                for r in (cur.fetchall() or [])
            ]
        return True, None, dump
    except Exception as exc:
        return False, str(exc), {}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _record_inventory_event_to_influx(entity, action, eui, data=None):
    if not bssci_config.INFLUX_INVENTORY_WRITE_ENABLED:
        return False, "Influx inventory writes disabled."
    measurement = bssci_config.INFLUX_INVENTORY_MEASUREMENT or "bssci_inventory_events"
    event_time_ns = int(time.time() * 1_000_000_000)
    actor = session.get("username", "system") if has_request_context() else "system"

    tags = {
        "entity": str(entity or "").lower(),
        "action": str(action or "").lower(),
        "eui": str(eui or "").lower(),
        "actor": actor,
        "source": "service_center_ui",
    }
    fields = {
        "event": f"{entity}_{action}",
        "has_payload": bool(data),
        "payload_size": len(json.dumps(data, ensure_ascii=True)) if data is not None else 0,
    }
    fields.update(_normalize_inventory_fields(data))

    line = _build_influx_line(measurement, tags, fields, event_time_ns)
    if not line:
        return False, "Failed to construct line protocol payload."
    return _write_influx_lines([line])

def _record_inventory_event_to_timescale(entity, action, eui, data=None):
    if not getattr(bssci_config, "TIMESCALE_INVENTORY_WRITE_ENABLED", True):
        return False, "Timescale inventory writes disabled."
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    tenant_id = _active_tenant_id()
    actor = session.get("username", "system") if has_request_context() else "system"
    payload_json = json.dumps(data or {}, separators=(",", ":"), ensure_ascii=True)
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tenants (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (tenant_id, tenant_id))
            cur.execute("""
                INSERT INTO inventory_events
                    (tenant_id, entity_type, action, eui, actor, event, has_payload, payload_size, source, payload)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (
                tenant_id,
                str(entity or "").lower(),
                str(action or "").lower(),
                str(eui or "").lower(),
                actor,
                f"{entity}_{action}",
                bool(data),
                len(payload_json) if data is not None else 0,
                "service_center_ui",
                payload_json,
            ))
        return True, None
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _record_inventory_event(entity, action, eui, data=None):
    sink_results = []
    if bssci_config.INFLUX_INVENTORY_WRITE_ENABLED:
        sink_results.append(("influx",) + _record_inventory_event_to_influx(entity, action, eui, data=data))
    if getattr(bssci_config, "TIMESCALE_INVENTORY_WRITE_ENABLED", True):
        sink_results.append(("timescale",) + _record_inventory_event_to_timescale(entity, action, eui, data=data))

    if not sink_results:
        return False, "All inventory sinks disabled."

    successful = [result for result in sink_results if result[1]]
    if successful:
        return True, None

    errors = [f"{name}: {err}" for name, _, err in sink_results if err]
    return False, "; ".join(errors) if errors else "Inventory write failed."

def _try_record_inventory_event(entity, action, eui, data=None):
    ok, err = _record_inventory_event(entity, action, eui, data=data)
    if not ok and err:
        err_lower = err.lower()
        if "disabled" in err_lower or "config missing" in err_lower:
            return ok, err
        logger.warning("Inventory sink write failed for %s %s %s: %s", entity, action, eui, err)
    return ok, err

def _sensor_runtime_snapshot_by_eui():
    """Build lightweight runtime map for sensors from TLS server structures."""
    snapshot = {}
    global tls_server_instance
    tls_server = tls_server_instance
    if not tls_server:
        return snapshot

    try:
        packet_stats = getattr(tls_server, "sensor_packet_stats", {}) or {}
        for eui_upper, stats in packet_stats.items():
            eui = str(eui_upper or "").strip().upper()
            if not eui:
                continue
            snr_count = int(stats.get("snr_count", 0) or 0)
            rssi_count = int(stats.get("rssi_count", 0) or 0)
            avg_snr = (stats.get("snr_sum", 0.0) / snr_count) if snr_count > 0 else None
            avg_rssi = (stats.get("rssi_sum", 0.0) / rssi_count) if rssi_count > 0 else None
            snapshot[eui] = {
                "packets_received": int(stats.get("packets_received", 0) or 0),
                "packets_lost": int(stats.get("packets_lost", 0) or 0),
                "last_seen": float(stats.get("last_seen", 0) or 0),
                "avg_snr": avg_snr,
                "avg_rssi": avg_rssi,
            }
    except Exception:
        pass

    return snapshot

def _base_station_runtime_snapshot_by_eui():
    """Build runtime map for base station states."""
    result = {}
    global tls_server_instance
    tls_server = tls_server_instance
    if not tls_server:
        return result

    try:
        connected_map = getattr(tls_server, "connected_base_stations", {}) or {}
        for _, bs_eui in list(connected_map.items()):
            eui = str(bs_eui or "").strip().lower()
            if eui:
                result[eui] = {"status": "connected"}
    except Exception:
        pass

    try:
        connecting_map = getattr(tls_server, "connecting_base_stations", {}) or {}
        for _, bs_eui in list(connecting_map.items()):
            eui = str(bs_eui or "").strip().lower()
            if not eui:
                continue
            if eui not in result:
                result[eui] = {"status": "connecting"}
    except Exception:
        pass

    try:
        health = getattr(tls_server, "base_station_health", {}) or {}
        for eui, metrics in health.items():
            key = str(eui or "").strip().lower()
            if not key:
                continue
            result.setdefault(key, {})
            result[key].update({
                "cpu_load": metrics.get("cpuLoad"),
                "mem_load": metrics.get("memLoad"),
                "duty_cycle": metrics.get("dutyCycle"),
                "uptime": metrics.get("uptime"),
            })
    except Exception:
        pass

    return result

def _build_inventory_snapshot_lines(trigger):
    """Create line protocol rows for all configured sensors and base stations."""
    measurement = bssci_config.INFLUX_SNAPSHOT_MEASUREMENT or "bssci_inventory_snapshot"
    timestamp_ns = int(time.time() * 1_000_000_000)
    lines = []

    # Sensors from config
    sensors = _load_all_sensors()

    runtime_sensor_map = _sensor_runtime_snapshot_by_eui()
    runtime_registered = set()
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            runtime_registered = {str(k).upper() for k in tls_server.registered_sensors.keys()}
    except Exception:
        runtime_registered = set()

    for sensor in sensors:
        eui = str(sensor.get("eui", "")).strip().upper()
        if not eui:
            continue
        tenant_id = _tenant_id_from_sensor(sensor)
        runtime = runtime_sensor_map.get(eui, {})
        packets_received = int(runtime.get("packets_received", 0) or 0)
        packets_lost = int(runtime.get("packets_lost", 0) or 0)
        line = _build_influx_line(
            measurement,
            {
                "entity": "sensor",
                "eui": eui.lower(),
                "tenant": tenant_id,
                "trigger": trigger,
            },
            {
                "configured": True,
                "registered": eui in runtime_registered,
                "bidi": bool(sensor.get("bidi", False)),
                "name": str(sensor.get("name", "") or ""),
                "short_addr": str(sensor.get("shortAddr", "") or ""),
                "tags_json": json.dumps(_normalize_sensor_tags(sensor.get("tags", [])), separators=(",", ":"), ensure_ascii=True),
                "tags_count": len(_normalize_sensor_tags(sensor.get("tags", []))),
                "gps_lat": sensor.get("gps_lat"),
                "gps_lng": sensor.get("gps_lng"),
                "packets_received": packets_received,
                "packets_lost": packets_lost,
                "packet_loss_pct": (packets_lost / (packets_received + packets_lost) * 100.0) if (packets_received + packets_lost) > 0 else 0.0,
                "avg_snr": runtime.get("avg_snr"),
                "avg_rssi": runtime.get("avg_rssi"),
                "last_seen_ts": int(runtime.get("last_seen", 0) or 0),
            },
            timestamp_ns
        )
        if line:
            lines.append(line)

    # Base stations from config
    bs_config = load_base_station_config().get("base_stations", {})
    bs_runtime_map = _base_station_runtime_snapshot_by_eui()

    # Count sensors per base station from registration table
    connected_sensors_per_bs = {}
    try:
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            for _, reg_data in (tls_server.registered_sensors or {}).items():
                for reg in reg_data.get("registrations", []) or []:
                    bs_eui = str(reg.get("bsEui", "")).strip().lower()
                    if bs_eui:
                        connected_sensors_per_bs[bs_eui] = connected_sensors_per_bs.get(bs_eui, 0) + 1
    except Exception:
        connected_sensors_per_bs = {}

    for eui, bs_data in (bs_config or {}).items():
        eui_lower = str(eui or "").strip().lower()
        if not eui_lower:
            continue
        tenant_id = _tenant_id_from_base_station(bs_data)
        runtime = bs_runtime_map.get(eui_lower, {})
        status = runtime.get("status", "disconnected")

        line = _build_influx_line(
            measurement,
            {
                "entity": "base_station",
                "eui": eui_lower,
                "tenant": tenant_id,
                "trigger": trigger,
                "status": status,
            },
            {
                "configured": True,
                "name": str(bs_data.get("name", "") or ""),
                "ip": str(bs_data.get("ip", "") or ""),
                "tags_json": json.dumps(bs_data.get("tags", []), separators=(",", ":"), ensure_ascii=True),
                "gps_lat": bs_data.get("gps_lat"),
                "gps_lng": bs_data.get("gps_lng"),
                "connected": status == "connected",
                "connecting": status == "connecting",
                "connected_sensors": int(connected_sensors_per_bs.get(eui_lower, 0)),
                "cpu_load": runtime.get("cpu_load"),
                "mem_load": runtime.get("mem_load"),
                "duty_cycle": runtime.get("duty_cycle"),
                "uptime": int(runtime.get("uptime", 0) or 0),
            },
            timestamp_ns
        )
        if line:
            lines.append(line)

    return lines

def _build_inventory_snapshot_records(trigger):
    timestamp_iso = datetime.now(timezone.utc).isoformat()
    records = []

    sensors = _load_all_sensors()

    runtime_sensor_map = _sensor_runtime_snapshot_by_eui()
    runtime_registered = set()
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            runtime_registered = {str(k).upper() for k in tls_server.registered_sensors.keys()}
    except Exception:
        runtime_registered = set()

    for sensor in sensors:
        eui = str(sensor.get("eui", "")).strip().upper()
        if not eui:
            continue
        tenant_id = _tenant_id_from_sensor(sensor)
        runtime = runtime_sensor_map.get(eui, {})
        packets_received = int(runtime.get("packets_received", 0) or 0)
        packets_lost = int(runtime.get("packets_lost", 0) or 0)
        registered = eui in runtime_registered
        payload = {
            "configured": True,
            "registered": registered,
            "bidi": bool(sensor.get("bidi", False)),
            "name": str(sensor.get("name", "") or ""),
            "short_addr": str(sensor.get("shortAddr", "") or ""),
            "tags": _normalize_sensor_tags(sensor.get("tags", [])),
            "gps_lat": sensor.get("gps_lat"),
            "gps_lng": sensor.get("gps_lng"),
            "packets_received": packets_received,
            "packets_lost": packets_lost,
            "packet_loss_pct": (packets_lost / (packets_received + packets_lost) * 100.0) if (packets_received + packets_lost) > 0 else 0.0,
            "avg_snr": runtime.get("avg_snr"),
            "avg_rssi": runtime.get("avg_rssi"),
            "last_seen_ts": int(runtime.get("last_seen", 0) or 0),
            "snapshot_time": timestamp_iso,
        }
        records.append({
            "tenant_id": tenant_id,
            "entity_type": "sensor",
            "eui": eui.lower(),
            "status": "registered" if registered else "configured",
            "trigger": trigger,
            "payload": payload,
        })

    bs_config = load_base_station_config().get("base_stations", {})
    bs_runtime_map = _base_station_runtime_snapshot_by_eui()

    connected_sensors_per_bs = {}
    try:
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            for _, reg_data in (tls_server.registered_sensors or {}).items():
                for reg in reg_data.get("registrations", []) or []:
                    bs_eui = str(reg.get("bsEui", "")).strip().lower()
                    if bs_eui:
                        connected_sensors_per_bs[bs_eui] = connected_sensors_per_bs.get(bs_eui, 0) + 1
    except Exception:
        connected_sensors_per_bs = {}

    for eui, bs_data in (bs_config or {}).items():
        eui_lower = str(eui or "").strip().lower()
        if not eui_lower:
            continue
        tenant_id = _tenant_id_from_base_station(bs_data)
        runtime = bs_runtime_map.get(eui_lower, {})
        status = runtime.get("status", "disconnected")
        payload = {
            "configured": True,
            "name": str(bs_data.get("name", "") or ""),
            "ip": str(bs_data.get("ip", "") or ""),
            "tags": bs_data.get("tags", []),
            "gps_lat": bs_data.get("gps_lat"),
            "gps_lng": bs_data.get("gps_lng"),
            "connected": status == "connected",
            "connecting": status == "connecting",
            "connected_sensors": int(connected_sensors_per_bs.get(eui_lower, 0)),
            "cpu_load": runtime.get("cpu_load"),
            "mem_load": runtime.get("mem_load"),
            "duty_cycle": runtime.get("duty_cycle"),
            "uptime": int(runtime.get("uptime", 0) or 0),
            "snapshot_time": timestamp_iso,
        }
        records.append({
            "tenant_id": tenant_id,
            "entity_type": "base_station",
            "eui": eui_lower,
            "status": status,
            "trigger": trigger,
            "payload": payload,
        })

    return records

def _sync_inventory_snapshot_to_influx(trigger="manual"):
    lines = _build_inventory_snapshot_lines(trigger=trigger)
    sensor_count = sum(1 for line in lines if ",entity=sensor," in line)
    bs_count = sum(1 for line in lines if ",entity=base_station," in line)
    ok, err = _write_influx_lines(lines)
    return {
        "success": ok,
        "error": err,
        "trigger": trigger,
        "line_count": len(lines),
        "sensor_points": sensor_count,
        "base_station_points": bs_count,
    }

def _sync_inventory_snapshot_to_timescale(trigger="manual"):
    if not getattr(bssci_config, "TIMESCALE_SNAPSHOT_ENABLED", True):
        return {
            "success": False,
            "error": "Timescale snapshot writes disabled.",
            "trigger": trigger,
            "line_count": 0,
            "sensor_points": 0,
            "base_station_points": 0,
        }

    conn, err = _timescale_connect()
    if conn is None:
        return {
            "success": False,
            "error": err,
            "trigger": trigger,
            "line_count": 0,
            "sensor_points": 0,
            "base_station_points": 0,
        }

    records = _build_inventory_snapshot_records(trigger=trigger)
    sensor_count = sum(1 for record in records if record.get("entity_type") == "sensor")
    bs_count = sum(1 for record in records if record.get("entity_type") == "base_station")
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            tenant_ids = sorted({
                _normalize_tenant_id(record.get("tenant_id"), fallback=_default_tenant_id())
                for record in records
            })
            if tenant_ids:
                cur.executemany("""
                    INSERT INTO tenants (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, [(tenant_id, tenant_id) for tenant_id in tenant_ids])

            for record in records:
                tenant_id = _normalize_tenant_id(record.get("tenant_id"), fallback=_default_tenant_id())
                payload_json = json.dumps(record.get("payload") or {}, separators=(",", ":"), ensure_ascii=True)
                cur.execute("""
                    INSERT INTO inventory_snapshot_points
                        (tenant_id, entity_type, eui, status, trigger, payload)
                    VALUES
                        (%s, %s, %s, %s, %s, %s::jsonb)
                """, (
                    tenant_id,
                    record.get("entity_type"),
                    record.get("eui"),
                    record.get("status"),
                    record.get("trigger"),
                    payload_json,
                ))
                cur.execute("""
                    INSERT INTO inventory_snapshot_latest
                        (tenant_id, entity_type, eui, status, trigger, payload, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (tenant_id, entity_type, eui)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        trigger = EXCLUDED.trigger,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                """, (
                    tenant_id,
                    record.get("entity_type"),
                    record.get("eui"),
                    record.get("status"),
                    record.get("trigger"),
                    payload_json,
                ))
        return {
            "success": True,
            "error": None,
            "trigger": trigger,
            "line_count": len(records),
            "sensor_points": sensor_count,
            "base_station_points": bs_count,
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "trigger": trigger,
            "line_count": len(records),
            "sensor_points": sensor_count,
            "base_station_points": bs_count,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _influx_snapshot_worker():
    logger.info("Influx snapshot worker started")
    # Initial snapshot shortly after startup
    time.sleep(3)
    last_influx_sync = 0.0
    last_timescale_sync = 0.0
    while not _influx_snapshot_stop.is_set():
        influx_interval = max(15, int(getattr(bssci_config, "INFLUX_SNAPSHOT_INTERVAL_SECONDS", 60)))
        timescale_interval = max(15, int(getattr(bssci_config, "TIMESCALE_SNAPSHOT_INTERVAL_SECONDS", 60)))
        influx_enabled = bool(getattr(bssci_config, "INFLUX_SNAPSHOT_ENABLED", True))
        timescale_enabled = bool(getattr(bssci_config, "TIMESCALE_SNAPSHOT_ENABLED", True))
        now = time.time()
        if influx_enabled and (last_influx_sync <= 0.0 or (now - last_influx_sync) >= influx_interval):
            result = _sync_inventory_snapshot_to_influx(trigger="interval")
            last_influx_sync = now
            if not result.get("success") and result.get("error"):
                err = str(result.get("error", "")).lower()
                if "config missing" not in err:
                    logger.warning("Periodic Influx snapshot failed: %s", result.get("error"))
        if timescale_enabled and (last_timescale_sync <= 0.0 or (now - last_timescale_sync) >= timescale_interval):
            result_ts = _sync_inventory_snapshot_to_timescale(trigger="interval")
            last_timescale_sync = now
            if not result_ts.get("success") and result_ts.get("error"):
                err = str(result_ts.get("error", "")).lower()
                if "disabled" not in err and "missing" not in err:
                    logger.warning("Periodic Timescale snapshot failed: %s", result_ts.get("error"))
        wait_interval = min(influx_interval, timescale_interval)
        _influx_snapshot_stop.wait(wait_interval)
    logger.info("Influx snapshot worker stopped")

def _ensure_influx_snapshot_worker_started():
    global _influx_snapshot_thread
    if _influx_snapshot_thread and _influx_snapshot_thread.is_alive():
        return
    _influx_snapshot_stop.clear()
    _influx_snapshot_thread = threading.Thread(target=_influx_snapshot_worker, daemon=True)
    _influx_snapshot_thread.start()

def _validate_eui(eui):
    return bool(re.match(r'^[0-9a-f]{16}$', eui.lower()))

def _normalize_sensor_tags(value):
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = re.split(r"[,\|;]", value)
    else:
        raw_items = []

    result = []
    seen = set()
    for item in raw_items:
        tag = str(item or "").strip()
        if not tag:
            continue
        tag_key = tag.lower()
        if tag_key in seen:
            continue
        seen.add(tag_key)
        result.append(tag)
    return result

def _normalize_base_station_route_list(value):
    """Normalize a list/string of base-station EUIs to unique uppercase values."""
    if isinstance(value, str):
        raw_items = re.split(r"[,\|;]", value)
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = []

    result = []
    seen = set()
    for item in raw_items:
        eui = str(item or "").strip().upper()
        if not eui:
            continue
        if not re.match(r"^[0-9A-F]{16}$", eui):
            continue
        if eui in seen:
            continue
        seen.add(eui)
        result.append(eui)
    return result

def _update_sensor_attached_base_stations(sensor_eui, base_station_euis, tenant_id=None):
    """Persist selected attach targets for one sensor inside sensor config."""
    target_eui = str(sensor_eui or "").strip().upper()
    if not target_eui:
        return False

    active_tenant = _normalize_tenant_id(tenant_id or _active_tenant_id(), fallback=_default_tenant_id())
    normalized_targets = _normalize_base_station_route_list(base_station_euis)
    sensors = _load_all_sensors()
    changed = False
    found = False

    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        if str(sensor.get("eui", "")).strip().upper() != target_eui:
            continue
        if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
            continue
        found = True

        current_targets = _normalize_base_station_route_list(sensor.get("attached_base_stations", []))
        if normalized_targets:
            sensor["attached_base_stations"] = normalized_targets
            changed = current_targets != normalized_targets
        else:
            if "attached_base_stations" in sensor:
                sensor.pop("attached_base_stations", None)
                changed = True
        break

    if found and changed:
        _save_all_sensors(sensors)
    return found and changed

def _load_all_sensors():
    try:
        with open(bssci_config.SENSOR_CONFIG_FILE, "r") as f:
            sensors = json.load(f) or []
        if isinstance(sensors, list):
            return sensors
    except Exception:
        pass
    return []

def _save_all_sensors(sensors):
    with open(bssci_config.SENSOR_CONFIG_FILE, "w") as f:
        json.dump(list(sensors or []), f, indent=4)

def _filter_sensors_for_tenant(sensors, tenant_id=None):
    active_tenant = _normalize_tenant_id(tenant_id or _active_tenant_id(), fallback=_default_tenant_id())
    filtered = []
    for sensor in (sensors or []):
        if not isinstance(sensor, dict):
            continue
        if _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
            payload = dict(sensor)
            payload["tenant_id"] = _tenant_id_from_sensor(sensor)
            filtered.append(payload)
    return filtered

def _filter_base_stations_for_tenant(base_stations, tenant_id=None):
    active_tenant = _normalize_tenant_id(tenant_id or _active_tenant_id(), fallback=_default_tenant_id())
    result = {}
    for eui, bs_data in (base_stations or {}).items():
        if not isinstance(bs_data, dict):
            continue
        if _tenant_matches(_tenant_id_from_base_station(bs_data), active_tenant):
            payload = dict(bs_data)
            payload["tenant_id"] = _tenant_id_from_base_station(bs_data)
            result[eui] = payload
    return result

def _resolve_sensor_tenant(sensor_eui):
    sensor_key = str(sensor_eui or "").strip().upper()
    if not sensor_key:
        return _default_tenant_id()
    for sensor in _load_all_sensors():
        if str((sensor or {}).get("eui", "")).strip().upper() == sensor_key:
            return _tenant_id_from_sensor(sensor)
    return _default_tenant_id()

def _normalize_optional_coordinate(value, label):
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if raw == "":
            return None
        value = raw
    try:
        num = float(value)
    except Exception:
        raise ValueError(f"Invalid {label} value")
    if not math.isfinite(num):
        raise ValueError(f"Invalid {label} value")
    return num

def _normalize_gps_coordinates(lat_value, lng_value):
    lat = _normalize_optional_coordinate(lat_value, "latitude")
    lng = _normalize_optional_coordinate(lng_value, "longitude")
    if lat is None and lng is None:
        return None, None
    if lat is None or lng is None:
        raise ValueError("Both latitude and longitude are required")
    if lat < -90 or lat > 90:
        raise ValueError("Latitude must be in range -90..90")
    if lng < -180 or lng > 180:
        raise ValueError("Longitude must be in range -180..180")
    return round(lat, 6), round(lng, 6)

def _coverage_positions_file():
    return "coverage_positions.json"

def _normalize_coverage_position_record(raw_key, raw_position):
    """Normalize one coverage position record to canonical key/payload."""
    if not isinstance(raw_position, dict):
        return None, None

    key_raw = str(raw_key or "").strip()
    key_upper = key_raw.upper()

    key_device_type = ""
    key_eui = ""
    if key_upper.startswith("BS_"):
        key_device_type = "bs"
        key_eui = key_upper.split("_", 1)[1] if "_" in key_upper else ""
    elif key_upper.startswith("SENSOR_"):
        key_device_type = "sensor"
        key_eui = key_upper.split("_", 1)[1] if "_" in key_upper else ""

    raw_device_type = str(raw_position.get("deviceType", "") or "").strip().lower()
    if raw_device_type in {"bs", "base_station"}:
        device_type = "bs"
    elif raw_device_type == "sensor":
        device_type = "sensor"
    else:
        device_type = key_device_type or "sensor"

    eui = _normalize_eui_upper(raw_position.get("eui") or key_eui)
    if not eui:
        return None, None

    raw_pos_type = str(raw_position.get("type", "") or "").strip().lower()
    pos_type = raw_pos_type if raw_pos_type in {"osm", "floorplan"} else "osm"

    normalized = {
        "type": pos_type,
        "deviceType": device_type,
        "eui": eui,
    }

    if pos_type == "osm":
        try:
            lat, lng = _normalize_gps_coordinates(raw_position.get("lat"), raw_position.get("lng"))
        except ValueError:
            return None, None
        if lat is None or lng is None:
            return None, None
        normalized["lat"] = float(lat)
        normalized["lng"] = float(lng)
    else:
        try:
            x = float(raw_position.get("x"))
            y = float(raw_position.get("y"))
        except (TypeError, ValueError):
            return None, None
        normalized["x"] = x
        normalized["y"] = y

    canonical_key = f"{device_type}_{eui}"
    return canonical_key, normalized

def _merge_coverage_position_payload(existing, candidate):
    """Prefer canonical OSM payload when duplicates collide by canonical key."""
    if not isinstance(existing, dict):
        return candidate
    if not isinstance(candidate, dict):
        return existing
    existing_type = str(existing.get("type", "")).lower()
    candidate_type = str(candidate.get("type", "")).lower()
    if existing_type == candidate_type:
        return candidate
    if candidate_type == "osm":
        return candidate
    if existing_type == "osm":
        return existing
    return candidate

def _canonicalize_coverage_positions_state(state):
    if not isinstance(state, dict):
        return {"positions": {}}, True

    raw_positions = state.get("positions", {})
    if not isinstance(raw_positions, dict):
        state["positions"] = {}
        return state, True

    normalized_positions = {}
    changed = False

    for raw_key, raw_position in raw_positions.items():
        canonical_key, normalized_payload = _normalize_coverage_position_record(raw_key, raw_position)
        if not canonical_key:
            changed = True
            continue
        existing = normalized_positions.get(canonical_key)
        merged = _merge_coverage_position_payload(existing, normalized_payload)
        normalized_positions[canonical_key] = merged
        if canonical_key != str(raw_key) or merged != raw_position:
            changed = True

    if normalized_positions != raw_positions:
        changed = True
    state["positions"] = normalized_positions
    return state, changed

def _load_coverage_positions_state():
    positions_file = _coverage_positions_file()
    try:
        if os.path.exists(positions_file):
            with open(positions_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("positions", {})
                    data, changed = _canonicalize_coverage_positions_state(data)
                    if changed:
                        _save_coverage_positions_state(data)
                    return data
    except Exception:
        pass
    return {"positions": {}}

def _save_coverage_positions_state(data):
    positions_file = _coverage_positions_file()
    with open(positions_file, "w") as f:
        json.dump(data, f, indent=2)

def _coverage_position_key(device_type, eui):
    eui_upper = str(eui or "").strip().upper()
    prefix = "bs" if str(device_type or "").lower() == "bs" else "sensor"
    return f"{prefix}_{eui_upper}", eui_upper

def _upsert_device_gps_position(device_type, eui, gps_lat, gps_lng):
    key, eui_upper = _coverage_position_key(device_type, eui)
    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})

    if gps_lat is None or gps_lng is None:
        existing = positions.get(key)
        if isinstance(existing, dict) and existing.get("type") == "osm":
            positions.pop(key, None)
    else:
        positions[key] = {
            "type": "osm",
            "lat": float(gps_lat),
            "lng": float(gps_lng),
            "deviceType": "bs" if str(device_type).lower() == "bs" else "sensor",
            "eui": eui_upper
        }

    _save_coverage_positions_state(state)

def _remove_device_position(device_type, eui):
    key, _ = _coverage_position_key(device_type, eui)
    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})
    if key in positions:
        positions.pop(key, None)
        _save_coverage_positions_state(state)

def _sync_inventory_gps_to_coverage_positions():
    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})
    if not isinstance(positions, dict):
        positions = {}
        state["positions"] = positions

    desired = {}

    # Base stations
    bs_config = load_base_station_config().get("base_stations", {}) or {}
    for bs_eui, bs_data in bs_config.items():
        try:
            gps_lat, gps_lng = _normalize_gps_coordinates((bs_data or {}).get("gps_lat"), (bs_data or {}).get("gps_lng"))
        except ValueError:
            gps_lat, gps_lng = None, None
        key, eui_upper = _coverage_position_key("bs", bs_eui)
        if gps_lat is not None and gps_lng is not None:
            desired[key] = {
                "type": "osm",
                "lat": gps_lat,
                "lng": gps_lng,
                "deviceType": "bs",
                "eui": eui_upper
            }

    # Sensors
    try:
        with open(bssci_config.SENSOR_CONFIG_FILE, "r") as f:
            sensors = json.load(f) or []
    except Exception:
        sensors = []

    for sensor in sensors:
        sensor_eui = str((sensor or {}).get("eui", "")).strip()
        if not sensor_eui:
            continue
        try:
            gps_lat, gps_lng = _normalize_gps_coordinates((sensor or {}).get("gps_lat"), (sensor or {}).get("gps_lng"))
        except ValueError:
            gps_lat, gps_lng = None, None
        key, eui_upper = _coverage_position_key("sensor", sensor_eui)
        if gps_lat is not None and gps_lng is not None:
            desired[key] = {
                "type": "osm",
                "lat": gps_lat,
                "lng": gps_lng,
                "deviceType": "sensor",
                "eui": eui_upper
            }

    changed = False

    # Upsert desired osm positions
    for key, payload in desired.items():
        existing = positions.get(key)
        if not isinstance(existing, dict) or existing.get("type") == "osm":
            if existing != payload:
                positions[key] = payload
                changed = True

    if changed:
        _save_coverage_positions_state(state)
    return state

def _sync_coverage_positions_to_inventory(tenant_id=None, only_missing=True, state=None, allowed_keys=None):
    """
    Backfill inventory GPS from saved coverage map positions.
    - Only uses OSM positions (lat/lng).
    - `only_missing=True` updates only devices without GPS in inventory.
    """
    active_tenant = _normalize_tenant_id(tenant_id or _active_tenant_id(), fallback=_default_tenant_id())
    payload = state if isinstance(state, dict) else _load_coverage_positions_state()
    positions = payload.get("positions", {}) if isinstance(payload, dict) else {}
    if not isinstance(positions, dict):
        return {"sensor": 0, "bs": 0, "total": 0}

    if allowed_keys is None:
        tenant_sensor_keys = {
            f"sensor_{str(sensor.get('eui', '')).strip().upper()}"
            for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
        }
        tenant_bs_keys = {
            f"bs_{str(eui).strip().upper()}"
            for eui in _filter_base_stations_for_tenant(
                load_base_station_config().get("base_stations", {}),
                tenant_id=active_tenant,
            ).keys()
        }
        allowed_keys = tenant_sensor_keys | tenant_bs_keys
    else:
        allowed_keys = set(allowed_keys)

    # Snapshot current inventory GPS (tenant-scoped)
    sensor_has_gps = set()
    for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant):
        sensor_eui = _normalize_eui_upper((sensor or {}).get("eui", ""))
        if not sensor_eui:
            continue
        try:
            lat, lng = _normalize_gps_coordinates((sensor or {}).get("gps_lat"), (sensor or {}).get("gps_lng"))
            if lat is not None and lng is not None:
                sensor_has_gps.add(sensor_eui)
        except ValueError:
            continue

    bs_has_gps = set()
    for bs_eui, bs_data in _filter_base_stations_for_tenant(
        load_base_station_config().get("base_stations", {}),
        tenant_id=active_tenant,
    ).items():
        bs_eui_norm = _normalize_eui_upper(bs_eui)
        if not bs_eui_norm:
            continue
        try:
            lat, lng = _normalize_gps_coordinates((bs_data or {}).get("gps_lat"), (bs_data or {}).get("gps_lng"))
            if lat is not None and lng is not None:
                bs_has_gps.add(bs_eui_norm)
        except ValueError:
            continue

    updated_sensor = 0
    updated_bs = 0

    for key, pos in positions.items():
        canonical_key, normalized_pos = _normalize_coverage_position_record(key, pos)
        if not canonical_key or not isinstance(normalized_pos, dict):
            continue
        if canonical_key not in allowed_keys:
            continue
        if str(normalized_pos.get("type", "")).strip().lower() != "osm":
            continue

        try:
            lat, lng = _normalize_gps_coordinates(normalized_pos.get("lat"), normalized_pos.get("lng"))
        except ValueError:
            continue

        eui = _normalize_eui_upper(normalized_pos.get("eui"))
        if not eui:
            continue

        is_bs = str(normalized_pos.get("deviceType", "")).strip().lower() in {"bs", "base_station"}
        if is_bs:
            if only_missing and eui in bs_has_gps:
                continue
            try:
                _update_base_station_gps_by_eui(eui, lat, lng)
                bs_has_gps.add(eui)
                updated_bs += 1
            except Exception:
                continue
        else:
            if only_missing and eui in sensor_has_gps:
                continue
            try:
                _update_sensor_gps_by_eui(eui, lat, lng)
                sensor_has_gps.add(eui)
                updated_sensor += 1
            except Exception:
                continue

    return {
        "sensor": updated_sensor,
        "bs": updated_bs,
        "total": updated_sensor + updated_bs,
    }

def _update_sensor_gps_by_eui(eui: str, gps_lat: float, gps_lng: float) -> None:
    sensor_eui = _normalize_eui_upper(eui)
    if not sensor_eui:
        raise ValueError("Sensor EUI is required")

    try:
        with open(bssci_config.SENSOR_CONFIG_FILE, "r") as f:
            sensors = json.load(f) or []
    except FileNotFoundError:
        sensors = []
    except json.JSONDecodeError:
        sensors = []

    if not isinstance(sensors, list):
        raise ValueError("Sensor configuration is invalid")

    active_tenant = _active_tenant_id()
    found = False
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        if (
            _normalize_eui_upper(sensor.get("eui", "")) == sensor_eui
            and _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant)
        ):
            sensor["gps_lat"] = gps_lat
            sensor["gps_lng"] = gps_lng
            found = True
            break

    if not found:
        raise ValueError(f"Sensor {sensor_eui} not found")

    with open(bssci_config.SENSOR_CONFIG_FILE, "w") as f:
        json.dump(sensors, f, indent=4)

def _update_base_station_gps_by_eui(eui: str, gps_lat: float, gps_lng: float) -> None:
    bs_eui = _normalize_eui_upper(eui)
    if not bs_eui:
        raise ValueError("Base station EUI is required")

    config = load_base_station_config()
    base_stations = config.setdefault("base_stations", {})
    if not isinstance(base_stations, dict):
        raise ValueError("Base station configuration is invalid")

    target_key = None
    for key in base_stations.keys():
        if _normalize_eui_upper(key) == bs_eui:
            if not _tenant_matches(_tenant_id_from_base_station(base_stations.get(key, {})), _active_tenant_id()):
                continue
            target_key = key
            break

    if target_key is None:
        raise ValueError(f"Base station {bs_eui} not found")

    bs_data = dict(base_stations.get(target_key, {}) or {})
    bs_data["gps_lat"] = gps_lat
    bs_data["gps_lng"] = gps_lng
    base_stations[target_key] = bs_data
    save_base_station_config(config)

def _normalize_sensor_payload(data):
    payload = dict(data or {})
    payload["eui"] = str(payload.get("eui", "")).strip().upper()
    payload["nwKey"] = str(payload.get("nwKey", "")).strip().upper()
    payload["shortAddr"] = str(payload.get("shortAddr", "0000")).strip().upper() or "0000"
    payload["bidi"] = bool(payload.get("bidi", False))
    payload["name"] = str(payload.get("name", "") or "").strip()
    payload["tags"] = _normalize_sensor_tags(payload.get("tags", []))
    gps_lat, gps_lng = _normalize_gps_coordinates(payload.get("gps_lat"), payload.get("gps_lng"))
    payload["gps_lat"] = gps_lat
    payload["gps_lng"] = gps_lng
    payload["tenant_id"] = _normalize_tenant_id(payload.get("tenant_id"), fallback=_active_tenant_id())
    return payload

def _ensure_ca_exists():
    if os.path.exists('certs/ca_cert.pem') and os.path.exists('certs/ca_key.pem'):
        return True
    os.makedirs('certs', exist_ok=True)
    result = subprocess.run(['openssl', 'genrsa', '-out', 'certs/ca_key.pem', '2048'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return False
    result = subprocess.run(['openssl', 'req', '-new', '-x509', '-key', 'certs/ca_key.pem',
                            '-out', 'certs/ca_cert.pem', '-days', '365',
                            '-subj', '/C=US/ST=State/L=City/O=BSSCI/CN=BSSCI-CA'],
                           capture_output=True, text=True, timeout=30)
    return result.returncode == 0

def _generate_bs_certificate(eui):
    eui = eui.lower()
    if not _validate_eui(eui):
        return False, "Invalid EUI format"
    if not _ensure_ca_exists():
        return False, "Failed to ensure CA exists"
    bs_cert_dir = os.path.join('certs', f'bs_{eui}')
    os.makedirs(bs_cert_dir, exist_ok=True)
    key_path = os.path.join(bs_cert_dir, f'{eui}_key.pem')
    csr_path = os.path.join(bs_cert_dir, f'{eui}.csr')
    cert_path = os.path.join(bs_cert_dir, f'{eui}_cert.pem')
    result = subprocess.run(['openssl', 'genrsa', '-out', key_path, '2048'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return False, f"Key generation failed: {result.stderr}"
    result = subprocess.run(['openssl', 'req', '-new', '-key', key_path, '-out', csr_path,
                            '-subj', f'/C=US/ST=State/L=City/O=BSSCI/CN={eui}'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return False, f"CSR generation failed: {result.stderr}"
    result = subprocess.run(['openssl', 'x509', '-req', '-in', csr_path,
                            '-CA', 'certs/ca_cert.pem', '-CAkey', 'certs/ca_key.pem',
                            '-CAcreateserial', '-out', cert_path, '-days', '365'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return False, f"Certificate signing failed: {result.stderr}"
    if os.path.exists(csr_path):
        os.remove(csr_path)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=365)
    config = load_base_station_config()
    if eui in config.get("base_stations", {}):
        config["base_stations"][eui]["cert_generated"] = now.strftime('%Y-%m-%dT%H:%M:%S')
        config["base_stations"][eui]["cert_expires"] = expires.strftime('%Y-%m-%dT%H:%M:%S')
        save_base_station_config(config)
    return True, "Certificate generated successfully"

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bssci-service-secret-key-change-me')
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = bool(getattr(bssci_config, "SESSION_COOKIE_SECURE", False))
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
    minutes=max(5, int(getattr(bssci_config, "AUTH_SESSION_TIMEOUT_MINUTES", 30) or 30))
)

# Configure logger for this module
logger = logging.getLogger(__name__)

_auth_security_lock = threading.Lock()
_login_rate_limit_by_ip = {}
_api_rate_limit_by_actor = {}
_session_timeout_seconds = max(
    300.0, float(getattr(bssci_config, "AUTH_SESSION_TIMEOUT_MINUTES", 30) or 30) * 60.0
)
_login_rate_limit_attempts = max(1, int(getattr(bssci_config, "AUTH_LOGIN_RATE_LIMIT_ATTEMPTS", 8) or 8))
_login_rate_limit_window_seconds = max(
    30.0, float(getattr(bssci_config, "AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS", 300) or 300)
)
_login_rate_limit_lockout_seconds = max(
    _login_rate_limit_window_seconds,
    float(getattr(bssci_config, "AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS", 900) or 900),
)
_api_rate_limit_requests = max(50, int(getattr(bssci_config, "AUTH_API_RATE_LIMIT_REQUESTS", 300) or 300))
_api_rate_limit_window_seconds = max(
    5.0, float(getattr(bssci_config, "AUTH_API_RATE_LIMIT_WINDOW_SECONDS", 60) or 60)
)

ADMIN_SCOPE_DEFINITIONS = {
    "manage_users": {
        "label": "Manage users",
        "description": "Create, update and delete user accounts.",
        "default": True,
    },
    "manage_tenants": {
        "label": "Manage tenants",
        "description": "Create/update/delete tenants and tenant import/export.",
        "default": True,
    },
    "manage_configuration": {
        "label": "Manage configuration",
        "description": "Read/write service configuration and operational settings.",
        "default": True,
    },
    "manage_system": {
        "label": "Manage system operations",
        "description": "Restart service/container and reset operational counters.",
        "default": True,
    },
    "manage_certificates": {
        "label": "Manage certificates",
        "description": "Issue, upload, restore and download certificate assets.",
        "default": True,
    },
    "view_admin_audit": {
        "label": "View admin audit",
        "description": "Read admin audit trail entries.",
        "default": True,
    },
    "export_admin_audit": {
        "label": "Export admin audit",
        "description": "Export admin audit entries (JSON/CSV).",
        "default": True,
    },
    "clear_admin_audit": {
        "label": "Clear admin audit",
        "description": "Clear persisted admin audit history.",
        "default": True,
    },
    "clear_service_logs": {
        "label": "Clear service logs",
        "description": "Clear in-memory service log stream.",
        "default": True,
    },
}


def _default_admin_permissions():
    return {scope: bool(meta.get("default", True)) for scope, meta in ADMIN_SCOPE_DEFINITIONS.items()}


def _normalize_admin_permissions(raw_permissions):
    defaults = _default_admin_permissions()
    if raw_permissions is None:
        return defaults
    if isinstance(raw_permissions, dict):
        normalized = dict(defaults)
        for scope in ADMIN_SCOPE_DEFINITIONS.keys():
            if scope in raw_permissions:
                normalized[scope] = bool(raw_permissions.get(scope))
        return normalized
    if isinstance(raw_permissions, (list, tuple, set)):
        allowed = {str(item).strip() for item in raw_permissions}
        return {scope: scope in allowed for scope in ADMIN_SCOPE_DEFINITIONS.keys()}
    raw_text = str(raw_permissions or "").strip()
    if not raw_text:
        return defaults
    allowed = {chunk.strip() for chunk in raw_text.split(",") if chunk.strip()}
    return {scope: scope in allowed for scope in ADMIN_SCOPE_DEFINITIONS.keys()}


def _has_admin_scope(user, scope):
    if not isinstance(user, dict):
        return False
    if _normalize_user_role(user.get("role", "viewer")) != "admin":
        return False
    normalized_scope = str(scope or "").strip()
    if not normalized_scope:
        return False
    permissions = _normalize_admin_permissions(user.get("admin_permissions"))
    return bool(permissions.get(normalized_scope, False))


def _apply_grantable_admin_permissions(actor_user, requested_permissions, existing_permissions=None):
    """
    Constrain requested admin scopes to what the actor can grant.
    - For scopes actor has: apply requested value.
    - For scopes actor does not have:
      - on update (existing_permissions provided): preserve existing value
      - on create: force False
    """
    requested = _normalize_admin_permissions(requested_permissions)
    existing = _normalize_admin_permissions(existing_permissions) if existing_permissions is not None else None
    actor_scopes = _normalize_admin_permissions((actor_user or {}).get("admin_permissions"))

    # Backward compatibility: if actor has full scope set, keep requested payload untouched.
    if actor_scopes and all(bool(actor_scopes.get(scope, False)) for scope in ADMIN_SCOPE_DEFINITIONS.keys()):
        return requested

    constrained = {}
    for scope in ADMIN_SCOPE_DEFINITIONS.keys():
        if bool(actor_scopes.get(scope, False)):
            constrained[scope] = bool(requested.get(scope, False))
        elif existing is not None:
            constrained[scope] = bool(existing.get(scope, False))
        else:
            constrained[scope] = False
    return constrained


def _cleanup_rate_limit_buckets(now_ts):
    stale_after = max(
        _login_rate_limit_window_seconds + _login_rate_limit_lockout_seconds,
        _api_rate_limit_window_seconds * 2.0,
    )
    for bucket in (_login_rate_limit_by_ip, _api_rate_limit_by_actor):
        stale_keys = []
        for key, state in bucket.items():
            last_seen = float(state.get("last_seen", 0.0) or 0.0)
            if last_seen > 0.0 and (now_ts - last_seen) > stale_after:
                stale_keys.append(key)
        for key in stale_keys:
            bucket.pop(key, None)


def _check_login_rate_limit(ip_addr):
    now_ts = time.time()
    with _auth_security_lock:
        _cleanup_rate_limit_buckets(now_ts)
        state = _login_rate_limit_by_ip.setdefault(
            str(ip_addr or "unknown"),
            {"attempts": deque(), "blocked_until": 0.0, "last_seen": now_ts},
        )
        state["last_seen"] = now_ts
        blocked_until = float(state.get("blocked_until", 0.0) or 0.0)
        if blocked_until > now_ts:
            return False, int(max(1, round(blocked_until - now_ts)))
        attempts = state.setdefault("attempts", deque())
        while attempts and (now_ts - float(attempts[0])) > _login_rate_limit_window_seconds:
            attempts.popleft()
        if len(attempts) >= _login_rate_limit_attempts:
            state["blocked_until"] = now_ts + _login_rate_limit_lockout_seconds
            attempts.clear()
            return False, int(_login_rate_limit_lockout_seconds)
        return True, 0


def _register_login_failure(ip_addr):
    now_ts = time.time()
    with _auth_security_lock:
        state = _login_rate_limit_by_ip.setdefault(
            str(ip_addr or "unknown"),
            {"attempts": deque(), "blocked_until": 0.0, "last_seen": now_ts},
        )
        state["last_seen"] = now_ts
        attempts = state.setdefault("attempts", deque())
        while attempts and (now_ts - float(attempts[0])) > _login_rate_limit_window_seconds:
            attempts.popleft()
        attempts.append(now_ts)
        if len(attempts) >= _login_rate_limit_attempts:
            state["blocked_until"] = now_ts + _login_rate_limit_lockout_seconds
            attempts.clear()


def _register_login_success(ip_addr):
    with _auth_security_lock:
        _login_rate_limit_by_ip.pop(str(ip_addr or "unknown"), None)


def _check_api_rate_limit(actor_key):
    now_ts = time.time()
    with _auth_security_lock:
        _cleanup_rate_limit_buckets(now_ts)
        state = _api_rate_limit_by_actor.setdefault(
            str(actor_key or "anonymous"),
            {"hits": deque(), "last_seen": now_ts},
        )
        state["last_seen"] = now_ts
        hits = state.setdefault("hits", deque())
        while hits and (now_ts - float(hits[0])) > _api_rate_limit_window_seconds:
            hits.popleft()
        if len(hits) >= _api_rate_limit_requests:
            retry_after = int(max(1, round(_api_rate_limit_window_seconds - (now_ts - float(hits[0])))))
            return False, retry_after
        hits.append(now_ts)
    return True, 0

TENANT_REGISTRY_FILE = "tenants.json"

def load_tenant_registry():
    """Load tenant metadata from local registry file."""
    default_tenant = _default_tenant_id()
    payload = {"tenants": []}
    changed = False

    try:
        if os.path.exists(TENANT_REGISTRY_FILE):
            with open(TENANT_REGISTRY_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                payload = raw
            else:
                changed = True
        else:
            changed = True
    except Exception as exc:
        logger.warning(f"Failed to load tenant registry '{TENANT_REGISTRY_FILE}': {exc}")
        changed = True

    tenants_raw = payload.get("tenants", [])
    if not isinstance(tenants_raw, list):
        tenants_raw = []
        changed = True

    normalized_map = {}
    for item in tenants_raw:
        if not isinstance(item, dict):
            changed = True
            continue
        tenant_id = _sanitize_tenant_id(item.get("id") or item.get("tenant_id"))
        if not tenant_id:
            changed = True
            continue
        if tenant_id in normalized_map:
            changed = True
            continue
        name = str(item.get("name") or tenant_id).strip()
        name = name[:120] if name else tenant_id
        description = str(item.get("description") or "").strip()[:240]
        created_at = str(item.get("created_at") or datetime.now(timezone.utc).isoformat())
        normalized_map[tenant_id] = {
            "id": tenant_id,
            "name": name,
            "description": description,
            "created_at": created_at,
        }

    if default_tenant not in normalized_map:
        normalized_map[default_tenant] = {
            "id": default_tenant,
            "name": "Default",
            "description": "Default tenant namespace",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        changed = True

    normalized_payload = {
        "tenants": [normalized_map[key] for key in sorted(normalized_map.keys())]
    }
    if changed:
        save_tenant_registry(normalized_payload)
    return normalized_payload

def save_tenant_registry(registry_data):
    """Persist tenant registry metadata to disk."""
    try:
        with open(TENANT_REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(registry_data, f, indent=2, ensure_ascii=True)
        return True
    except Exception as exc:
        logger.error(f"Failed to save tenant registry '{TENANT_REGISTRY_FILE}': {exc}")
        return False

def _tenant_registry_map():
    registry = load_tenant_registry()
    tenant_map = {}
    for entry in registry.get("tenants", []):
        if not isinstance(entry, dict):
            continue
        tenant_id = _sanitize_tenant_id(entry.get("id"))
        if not tenant_id:
            continue
        tenant_map[tenant_id] = {
            "id": tenant_id,
            "name": str(entry.get("name") or tenant_id).strip() or tenant_id,
            "description": str(entry.get("description") or "").strip(),
            "created_at": str(entry.get("created_at") or ""),
        }
    return tenant_map

def _upsert_tenant_registry_entry(tenant_id, name=None, description=None):
    tenant_id = _sanitize_tenant_id(tenant_id)
    if not tenant_id:
        return False, "Invalid tenant id", None

    registry = load_tenant_registry()
    tenant_map = {
        str(item.get("id")).strip().lower(): item
        for item in registry.get("tenants", [])
        if isinstance(item, dict) and _sanitize_tenant_id(item.get("id"))
    }
    existing = tenant_map.get(tenant_id)
    now_iso = datetime.now(timezone.utc).isoformat()

    if existing is None:
        existing = {
            "id": tenant_id,
            "name": str(name or tenant_id).strip() or tenant_id,
            "description": str(description or "").strip(),
            "created_at": now_iso,
        }
    else:
        if name is not None:
            existing["name"] = str(name).strip() or tenant_id
        else:
            existing["name"] = str(existing.get("name") or tenant_id).strip() or tenant_id
        if description is not None:
            existing["description"] = str(description).strip()
        else:
            existing["description"] = str(existing.get("description") or "").strip()
        existing["created_at"] = str(existing.get("created_at") or now_iso)

    existing["name"] = existing["name"][:120]
    existing["description"] = existing["description"][:240]
    tenant_map[tenant_id] = existing
    next_payload = {"tenants": [tenant_map[key] for key in sorted(tenant_map.keys())]}

    if not save_tenant_registry(next_payload):
        return False, "Failed to persist tenant registry", None
    return True, None, existing

# User management functions
def load_users():
    """Load users from users.json file"""
    try:
        with open('users.json', 'r') as f:
            data = json.load(f)
            if not isinstance(data, dict):
                data = {}
            users = data.setdefault("users", {})
            data.setdefault("role_permissions", {})
            default_tenant = _default_tenant_id()
            changed = False
            for _, user in users.items():
                if not isinstance(user, dict):
                    continue
                normalized_role = _normalize_user_role(user.get("role", "viewer"))
                if user.get("role") != normalized_role:
                    user["role"] = normalized_role
                    changed = True
                if normalized_role == "admin":
                    normalized_admin_permissions = _normalize_admin_permissions(user.get("admin_permissions"))
                    if user.get("admin_permissions") != normalized_admin_permissions:
                        user["admin_permissions"] = normalized_admin_permissions
                        changed = True
                elif "admin_permissions" in user:
                    user.pop("admin_permissions", None)
                    changed = True
                normalized_tenant = _normalize_user_tenant_for_role(
                    normalized_role,
                    user.get("tenant_id"),
                    fallback=default_tenant,
                )
                if user.get("tenant_id") != normalized_tenant:
                    user["tenant_id"] = normalized_tenant
                    changed = True
            if changed:
                save_users(data)
            return data
    except Exception as e:
        logger.error(f"Failed to load users: {e}")
        return {"users": {}, "role_permissions": {}}

def save_users(users_data):
    """Save users to users.json file"""
    try:
        with open('users.json', 'w') as f:
            json.dump(users_data, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save users: {e}")
        return False

def get_current_user():
    """Get current logged in user info"""
    if 'username' not in session:
        return None
    users_data = load_users()
    username = session.get('username')
    if username in users_data.get('users', {}):
        user = users_data['users'][username].copy()
        user['username'] = username
        role = _normalize_user_role(user.get('role', 'viewer'))
        user['role'] = role
        user['tenant_id'] = _normalize_user_tenant_for_role(
            role,
            user.get('tenant_id'),
            fallback=_default_tenant_id(),
        )
        if role == "admin":
            user['admin_permissions'] = _normalize_admin_permissions(user.get("admin_permissions"))
        else:
            user['admin_permissions'] = {}
        user['permissions'] = users_data.get('role_permissions', {}).get(role, {})
        return user
    return None

def get_user_permissions():
    """Get permissions for current user"""
    user = get_current_user()
    if user:
        return user.get('permissions', {})
    return {}

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Login required'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    """Decorator to require specific role(s)"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))
            if user.get('role') not in roles:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Insufficient permissions'}), 403
                return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def admin_scope_required(*scopes, any_scope=False):
    """Decorator for fine-grained admin scopes."""
    normalized_scopes = [str(scope or "").strip() for scope in scopes if str(scope or "").strip()]

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))
            if _normalize_user_role(user.get('role', 'viewer')) != 'admin':
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Admin role required'}), 403
                return redirect(url_for('index'))

            if normalized_scopes:
                checks = [_has_admin_scope(user, scope) for scope in normalized_scopes]
                authorized = any(checks) if any_scope else all(checks)
                if not authorized:
                    if request.path.startswith('/api/'):
                        return jsonify({
                            'error': 'Insufficient admin scope',
                            'required_scopes': normalized_scopes,
                        }), 403
                    return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def permission_required(permission):
    """Decorator to require specific permission"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))

            admin_scope_by_permission = {
                'can_edit_config': 'manage_configuration',
                'can_manage_certificates': 'manage_certificates',
                'can_update_system': 'manage_system',
            }
            if _normalize_user_role(user.get('role', 'viewer')) == 'admin':
                required_scope = admin_scope_by_permission.get(str(permission or ''))
                if required_scope and not _has_admin_scope(user, required_scope):
                    if request.path.startswith('/api/'):
                        return jsonify({'error': 'Insufficient admin scope'}), 403
                    return redirect(url_for('index'))

            perms = get_user_permissions()
            if not perms.get(permission, False):
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Insufficient permissions'}), 403
                return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors and return JSON"""
    app.logger.error(f"Internal server error: {error}")
    if request.path.startswith('/api/'):
        return jsonify({
            'error': 'Internal server error',
            'running': False,
            'service_type': 'web_ui',
            'tls_server': {'active': False},
            'mqtt_broker': {'active': False},
            'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []}
        }), 500
    return error

@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors for API endpoints"""
    if request.path.startswith('/api/'):
        return jsonify({'error': 'API endpoint not found'}), 404
    return error

@app.before_request
def ensure_json_api():
    """Apply API behavior defaults + auth hardening checks."""
    endpoint = str(request.endpoint or "")
    path = str(request.path or "")
    is_api = path.startswith('/api/')

    # Keep static and login assets outside auth timeout updates.
    is_static_like = endpoint == "static" or path.startswith("/static/")

    # Session timeout enforcement.
    if not is_static_like and path not in ("/login",):
        if 'username' in session:
            now_ts = time.time()
            try:
                last_activity = float(session.get('_last_activity_ts') or 0.0)
            except (TypeError, ValueError):
                last_activity = 0.0
            if last_activity > 0 and (now_ts - last_activity) > _session_timeout_seconds:
                username = str(session.get('username') or 'unknown')
                session.clear()
                logger.info("Session expired for user '%s' (timeout=%ss)", username, int(_session_timeout_seconds))
                if is_api:
                    return (
                        jsonify({'error': 'Session expired. Please login again.'}),
                        401,
                        {'X-Session-Expired': '1'},
                    )
                return redirect(url_for('login', reason='timeout'))
            session.permanent = True
            session['_last_activity_ts'] = now_ts

    # Login rate-limit (POST only).
    if path == '/login' and request.method == 'POST' and bool(getattr(bssci_config, "AUTH_RATE_LIMIT_ENABLED", True)):
        ip_addr = _current_request_ip()
        allowed, retry_after = _check_login_rate_limit(ip_addr)
        if not allowed:
            message = f"Too many login attempts. Try again in {int(retry_after)}s."
            response = make_response(render_template('login.html', error=message), 429)
            response.headers['Retry-After'] = str(int(retry_after))
            return response

    # API rate-limit (per actor/IP).
    if is_api and bool(getattr(bssci_config, "AUTH_RATE_LIMIT_ENABLED", True)):
        actor = str(session.get('username') or 'anonymous')
        actor_key = f"{actor}@{_current_request_ip()}"
        allowed, retry_after = _check_api_rate_limit(actor_key)
        if not allowed:
            return (
                jsonify({
                    'error': 'Rate limit exceeded',
                    'retry_after_seconds': int(retry_after),
                }),
                429,
                {'Retry-After': str(int(retry_after))},
            )

        if not request.is_json and request.method in ['POST', 'PUT', 'PATCH']:
            # For non-JSON API writes, let route handlers decide explicit validation.
            pass

# Global variables for log storage and configuration
log_entries: List[Dict[str, Any]] = []
max_log_entries = 1000
web_log_handler = None
admin_audit_entries: List[Dict[str, Any]] = []
max_admin_audit_entries = max(200, int(getattr(bssci_config, "ADMIN_AUDIT_MAX_ENTRIES", 5000) or 5000))
admin_audit_log_file = os.path.join("logs", "admin_audit.jsonl")
_admin_audit_lock = threading.Lock()
_AUDIT_SENSITIVE_KEY_MARKERS = (
    "password",
    "token",
    "secret",
    "key",
    "authorization",
    "credential",
    "bearer",
    "cookie",
)


def _audit_scrub_value(value, depth=0):
    if depth > 4:
        return "[truncated]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        compact = value.strip()
        return compact if len(compact) <= 400 else f"{compact[:400]}...<truncated>"
    if isinstance(value, dict):
        result = {}
        for idx, (raw_key, raw_val) in enumerate(value.items()):
            if idx >= 80:
                result["__truncated__"] = f"{len(value) - 80} more keys"
                break
            key = str(raw_key)
            key_lower = key.lower()
            if any(marker in key_lower for marker in _AUDIT_SENSITIVE_KEY_MARKERS):
                result[key] = "***"
            else:
                result[key] = _audit_scrub_value(raw_val, depth + 1)
        return result
    if isinstance(value, (list, tuple, set)):
        seq = list(value)
        out = [_audit_scrub_value(item, depth + 1) for item in seq[:80]]
        if len(seq) > 80:
            out.append(f"...<{len(seq) - 80} more>")
        return out
    return str(value)


def _append_admin_audit_entry(entry):
    global admin_audit_entries
    with _admin_audit_lock:
        admin_audit_entries.append(entry)
        if len(admin_audit_entries) > max_admin_audit_entries:
            admin_audit_entries = admin_audit_entries[-max_admin_audit_entries:]
        try:
            os.makedirs(os.path.dirname(admin_audit_log_file), exist_ok=True)
            with open(admin_audit_log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=True) + "\n")
        except Exception as exc:
            logger.warning("Failed to persist admin audit entry: %s", exc)


def _load_admin_audit_entries():
    global admin_audit_entries
    if not os.path.exists(admin_audit_log_file):
        admin_audit_entries = []
        return
    loaded = []
    try:
        with open(admin_audit_log_file, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except Exception:
                    continue
                if isinstance(item, dict):
                    loaded.append(item)
    except Exception as exc:
        logger.warning("Failed to load admin audit log: %s", exc)
        loaded = []
    admin_audit_entries = loaded[-max_admin_audit_entries:]


def _current_request_ip():
    if not has_request_context():
        return ""
    forwarded = str(request.headers.get("X-Forwarded-For", "")).strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "")


def _record_admin_audit(action, entity, target_id="", status="success", details=None):
    actor = "system"
    role = "system"
    actor_tenant = _default_tenant_id()
    active_tenant = _default_tenant_id()
    method = ""
    path = ""
    ip_addr = ""
    user_agent = ""

    if has_request_context():
        actor = str(session.get("username") or "anonymous")
        role = str(session.get("role") or "")
        actor_tenant = _normalize_tenant_id(session.get("tenant_id"), fallback=_default_tenant_id())
        active_tenant = _active_tenant_id()
        method = str(request.method or "")
        path = str(request.path or "")
        ip_addr = _current_request_ip()
        user_agent = str(request.headers.get("User-Agent", "") or "")[:240]

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "action": str(action or "").strip() or "unknown",
        "entity": str(entity or "").strip() or "unknown",
        "target_id": str(target_id or "").strip(),
        "status": str(status or "success").strip().lower(),
        "actor": actor,
        "role": role,
        "actor_tenant": actor_tenant,
        "active_tenant": active_tenant,
        "method": method,
        "path": path,
        "ip": ip_addr,
        "user_agent": user_agent,
        "details": _audit_scrub_value(details or {}),
    }
    _append_admin_audit_entry(entry)

# Custom log handler to capture all logs with timezone support
class WebUILogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        # Use configured timezone
        self._update_timezone()
    
    def _update_timezone(self):
        """Update timezone from config"""
        try:
            import zoneinfo
            self.tz = zoneinfo.ZoneInfo(bssci_config.TIMEZONE)
            self.use_zoneinfo = True
        except Exception:
            # Fallback to UTC+1 (CET)
            self.tz = timezone(timedelta(hours=1))
            self.use_zoneinfo = False

    def emit(self, record):
        global log_entries

        # Filter out noisy web request logs to reduce clutter
        if record.name == 'werkzeug' and any(x in record.getMessage() for x in [
            'GET /api/', 'GET /logs', 'GET /sensors', 'GET /config', 'GET /administration', 'GET /', 'GET /static/'
        ]):
            return  # Skip web request logs

        # Convert UTC timestamp to local timezone
        utc_time = datetime.fromtimestamp(record.created, tz=timezone.utc)
        local_time = utc_time.astimezone(self.tz)
        current_time = local_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        message = record.getMessage()

        # Check if this exact message was logged in the last second (duplicate detection)
        if log_entries:
            last_entry = log_entries[-1]
            try:
                last_time = datetime.strptime(last_entry['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
                time_diff = abs((local_time.replace(tzinfo=None) - last_time).total_seconds())

                if (time_diff < 1.0 and  # Within 1 second
                    last_entry['message'] == message and
                    last_entry['logger'] == record.name):
                    return  # Skip duplicate message
            except:
                pass  # If timestamp parsing fails, continue with logging

        log_entry = {
            'timestamp': current_time,
            'level': record.levelname,
            'logger': record.name,
            'message': message,
            'source': 'memory'
        }
        log_entries.append(log_entry)

        # Keep only the last max_log_entries
        if len(log_entries) > max_log_entries:
            log_entries = log_entries[-max_log_entries:]

def ensure_web_log_handler():
    """Ensure in-memory web log handler is attached even after logger reconfiguration."""
    global web_log_handler

    root_logger = logging.getLogger()
    existing_handler = next((h for h in root_logger.handlers if isinstance(h, WebUILogHandler)), None)

    if existing_handler is None:
        web_log_handler = WebUILogHandler()
        root_logger.addHandler(web_log_handler)
    else:
        web_log_handler = existing_handler

    # Keep visibility for INFO/DEBUG logs in the web logs page.
    if root_logger.level > logging.DEBUG:
        root_logger.setLevel(logging.DEBUG)

    logging.getLogger('TLSServer').setLevel(logging.DEBUG)
    logging.getLogger('mqtt_interface').setLevel(logging.DEBUG)


# Attach handler at import time; call again from runtime paths after any logging reset.
ensure_web_log_handler()
_load_admin_audit_entries()

@app.context_processor
def inject_user():
    """Inject user info into all templates"""
    user = get_current_user()
    return {
        'current_user': user,
        'user_permissions': user.get('permissions', {}) if user else {},
        'visible_tabs': user.get('permissions', {}).get('visible_tabs', []) if user else [],
        'active_tenant_id': _active_tenant_id() if user else _default_tenant_id(),
        'oms_enabled': bool(getattr(bssci_config, 'OMS_ENABLED', True)),
    }

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'GET' and request.args.get('reason') == 'timeout':
        error = 'Session expired due to inactivity. Please sign in again.'
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        ip_addr = _current_request_ip()
        users_data = load_users()
        user = users_data.get('users', {}).get(username)
        if user and user.get('password') == password:
            session['username'] = username
            session['role'] = _normalize_user_role(user.get('role', 'viewer'))
            session['tenant_id'] = _normalize_tenant_id(
                _normalize_user_tenant_for_role(
                    session['role'],
                    user.get('tenant_id'),
                    fallback=_default_tenant_id(),
                ),
                fallback=_default_tenant_id(),
            )
            session.permanent = True
            session['_last_activity_ts'] = time.time()
            _register_login_success(ip_addr)
            logger.info(f"User '{username}' logged in")
            return redirect(url_for('index'))
        _register_login_failure(ip_addr)
        error = 'Invalid username or password'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    username = session.get('username', 'Unknown')
    session.clear()
    logger.info(f"User '{username}' logged out")
    return redirect(url_for('login'))

@app.route('/api/users', methods=['GET', 'POST', 'PUT', 'DELETE'])
@login_required
@admin_scope_required('manage_users')
def api_users():
    """Manage users (admin only)"""
    users_data = load_users()
    actor_user = get_current_user() or {}
    
    if request.method == 'GET':
        users_list = []
        for username, data in users_data.get('users', {}).items():
            role = _normalize_user_role(data.get('role', 'viewer'))
            users_list.append({
                'username': username,
                'name': data.get('name', ''),
                'role': role,
                'tenant_id': _normalize_user_tenant_for_role(
                    role,
                    data.get('tenant_id'),
                    fallback=_default_tenant_id(),
                ),
                'admin_permissions': _normalize_admin_permissions(data.get('admin_permissions')) if role == 'admin' else {},
            })
        known_tenant_ids = {_default_tenant_id()}
        try:
            registry = load_tenant_registry()
            for row in (registry.get('tenants', []) if isinstance(registry, dict) else []):
                tenant_id = _sanitize_tenant_id((row or {}).get('tenant_id') if isinstance(row, dict) else '')
                if tenant_id:
                    known_tenant_ids.add(tenant_id)
        except Exception:
            pass
        for _, data in users_data.get('users', {}).items():
            if not isinstance(data, dict):
                continue
            role = _normalize_user_role(data.get('role', 'viewer'))
            if role == 'admin':
                continue
            known_tenant_ids.add(
                _normalize_user_tenant_for_role(role, data.get('tenant_id'), fallback=_default_tenant_id())
            )
        return jsonify({
            'users': users_list,
            'roles': list(users_data.get('role_permissions', {}).keys()),
            'default_tenant': _default_tenant_id(),
            'tenant_choices': sorted(known_tenant_ids),
            'admin_scopes': [
                {
                    'id': scope_id,
                    'label': meta.get('label', scope_id),
                    'description': meta.get('description', ''),
                    'default_enabled': bool(meta.get('default', True)),
                }
                for scope_id, meta in ADMIN_SCOPE_DEFINITIONS.items()
            ],
            'current_admin_permissions': _normalize_admin_permissions(actor_user.get('admin_permissions')),
        })
    
    elif request.method == 'POST':
        data = request.get_json()
        username = data.get('username', '').strip()
        if not username or username in users_data.get('users', {}):
            return jsonify({'success': False, 'error': 'Username is invalid or already exists'}), 400
        role = _normalize_user_role(data.get('role', 'viewer'))
        tenant_id = _normalize_user_tenant_for_role(
            role,
            data.get('tenant_id'),
            fallback=_default_tenant_id(),
        )
        users_data['users'][username] = {
            'password': data.get('password', 'password123'),
            'role': role,
            'name': data.get('name', username),
            'tenant_id': tenant_id,
        }
        if role == 'admin':
            users_data['users'][username]['admin_permissions'] = _apply_grantable_admin_permissions(
                actor_user,
                data.get('admin_permissions'),
                existing_permissions=None,
            )
        if tenant_id:
            _upsert_tenant_registry_entry(tenant_id)
        save_users(users_data)
        _record_admin_audit(
            action='user.create',
            entity='user',
            target_id=username,
            status='success',
            details={
                'role': role,
                'tenant_id': tenant_id,
                'name': data.get('name', username),
                'admin_permissions': users_data['users'][username].get('admin_permissions', {}) if role == 'admin' else {},
            },
        )
        return jsonify({'success': True})
    
    elif request.method == 'PUT':
        data = request.get_json()
        username = data.get('username')
        if username not in users_data.get('users', {}):
            return jsonify({'success': False, 'error': 'User not found'}), 404
        user_row = users_data['users'][username]
        if 'password' in data and data['password']:
            user_row['password'] = data['password']
        if 'role' in data:
            user_row['role'] = _normalize_user_role(data['role'])
        if 'name' in data:
            user_row['name'] = data['name']
        if 'tenant_id' in data or 'role' in data:
            tenant_source = data.get('tenant_id') if 'tenant_id' in data else user_row.get('tenant_id')
            tenant_id = _normalize_user_tenant_for_role(
                user_row.get('role', 'viewer'),
                tenant_source,
                fallback=_default_tenant_id(),
            )
            user_row['tenant_id'] = tenant_id
            if tenant_id:
                _upsert_tenant_registry_entry(tenant_id)
        if _normalize_user_role(user_row.get('role', 'viewer')) == 'admin':
            if 'admin_permissions' in data:
                user_row['admin_permissions'] = _apply_grantable_admin_permissions(
                    actor_user,
                    data.get('admin_permissions'),
                    existing_permissions=user_row.get('admin_permissions'),
                )
            else:
                user_row['admin_permissions'] = _normalize_admin_permissions(user_row.get('admin_permissions'))
            if username == session.get('username') and not bool(user_row['admin_permissions'].get('manage_users', False)):
                return jsonify({
                    'success': False,
                    'error': 'Cannot remove manage_users scope from your own admin account',
                }), 400
        else:
            user_row.pop('admin_permissions', None)
        save_users(users_data)
        if username == session.get('username'):
            session['role'] = _normalize_user_role(user_row.get('role', 'viewer'))
            session['tenant_id'] = _normalize_tenant_id(user_row.get('tenant_id'), fallback=_default_tenant_id())
        _record_admin_audit(
            action='user.update',
            entity='user',
            target_id=username,
            status='success',
            details={
                'role': user_row.get('role', 'viewer'),
                'tenant_id': user_row.get('tenant_id', _default_tenant_id()),
                'name': user_row.get('name', ''),
                'password_changed': bool(data.get('password')),
                'admin_permissions': user_row.get('admin_permissions', {}) if _normalize_user_role(user_row.get('role', 'viewer')) == 'admin' else {},
            },
        )
        return jsonify({'success': True})
    
    elif request.method == 'DELETE':
        data = request.get_json()
        username = data.get('username')
        if username == session.get('username'):
            return jsonify({'success': False, 'error': 'Cannot delete your own user account'}), 400
        if username in users_data.get('users', {}):
            del users_data['users'][username]
            save_users(users_data)
            _record_admin_audit(
                action='user.delete',
                entity='user',
                target_id=username,
                status='success',
                details={},
            )
        return jsonify({'success': True})

@app.route('/api/tenants', methods=['GET', 'POST', 'PUT', 'DELETE'])
@login_required
@admin_scope_required('manage_tenants')
def api_tenants():
    """List and manage tenant metadata."""
    tenant_ids = set()
    default_tenant = _default_tenant_id()
    tenant_ids.add(default_tenant)
    all_sensors = _load_all_sensors()
    registry_map = _tenant_registry_map()
    tenant_ids.update(registry_map.keys())

    users_data = load_users()
    users_map = users_data.get("users", {}) if isinstance(users_data, dict) else {}
    bs_config = load_base_station_config().get("base_stations", {}) or {}

    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        tenant_id = _sanitize_tenant_id(data.get("tenant_id") or data.get("id"))
        if not tenant_id:
            return jsonify({"success": False, "error": "Tenant ID is required"}), 400

        known_ids = set(registry_map.keys())
        for user in users_map.values():
            if isinstance(user, dict):
                role = _normalize_user_role(user.get("role", "viewer"))
                if role != "admin":
                    known_ids.add(_normalize_user_tenant_for_role(role, user.get("tenant_id"), fallback=default_tenant))
        for sensor in all_sensors:
            if isinstance(sensor, dict):
                known_ids.add(_tenant_id_from_sensor(sensor))
        for bs_data in bs_config.values():
            if isinstance(bs_data, dict):
                known_ids.add(_tenant_id_from_base_station(bs_data))

        if tenant_id in known_ids:
            return jsonify({"success": False, "error": f"Tenant '{tenant_id}' already exists"}), 400

        tenant_name = str(data.get("name") or tenant_id).strip()[:120]
        tenant_description = str(data.get("description") or "").strip()[:240]
        ok, err, tenant_entry = _upsert_tenant_registry_entry(
            tenant_id=tenant_id,
            name=tenant_name,
            description=tenant_description,
        )
        if not ok:
            return jsonify({"success": False, "error": err or "Failed to create tenant"}), 500

        conn, conn_err = _timescale_connect()
        timescale_result = {"synced": False, "error": conn_err}
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                        """,
                        (tenant_id, tenant_name),
                    )
                timescale_result = {"synced": True, "error": None}
            except Exception as exc:
                timescale_result = {"synced": False, "error": str(exc)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        _record_admin_audit(
            action='tenant.create',
            entity='tenant',
            target_id=tenant_id,
            status='success',
            details={
                'name': tenant_name,
                'description': tenant_description,
                'timescale_synced': bool(timescale_result.get('synced')),
                'timescale_error': timescale_result.get('error'),
            },
        )
        return jsonify({
            "success": True,
            "tenant": tenant_entry,
            "timescale": timescale_result,
        })

    if request.method == "PUT":
        data = request.get_json(silent=True) or {}
        tenant_id = _sanitize_tenant_id(data.get("tenant_id") or data.get("id"))
        if not tenant_id:
            return jsonify({"success": False, "error": "Tenant ID is required"}), 400

        tenant_name = str(data.get("name") or tenant_id).strip()[:120]
        tenant_description = str(data.get("description") or "").strip()[:240]
        ok, err, tenant_entry = _upsert_tenant_registry_entry(
            tenant_id=tenant_id,
            name=tenant_name,
            description=tenant_description,
        )
        if not ok:
            return jsonify({"success": False, "error": err or "Failed to update tenant"}), 500

        conn, conn_err = _timescale_connect()
        timescale_result = {"synced": False, "error": conn_err}
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                        """,
                        (tenant_id, tenant_name),
                    )
                timescale_result = {"synced": True, "error": None}
            except Exception as exc:
                timescale_result = {"synced": False, "error": str(exc)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        _record_admin_audit(
            action='tenant.update',
            entity='tenant',
            target_id=tenant_id,
            status='success',
            details={
                'name': tenant_name,
                'description': tenant_description,
                'timescale_synced': bool(timescale_result.get('synced')),
                'timescale_error': timescale_result.get('error'),
            },
        )
        return jsonify({
            "success": True,
            "tenant": tenant_entry,
            "timescale": timescale_result,
        })

    if request.method == "DELETE":
        data = request.get_json(silent=True) or {}
        tenant_id = _sanitize_tenant_id(
            data.get("tenant_id")
            or data.get("id")
            or request.args.get("tenant_id")
        )
        force_delete = _parse_bool_arg(data.get("force") if isinstance(data, dict) else None, default=False) or _parse_bool_arg(request.args.get("force"), default=False)
        if not tenant_id:
            return jsonify({"success": False, "error": "Tenant ID is required"}), 400
        if tenant_id == default_tenant:
            return jsonify({"success": False, "error": "Default tenant cannot be deleted"}), 400

        user_count = sum(
            1
            for _, user in users_map.items()
            if _user_belongs_to_tenant(user, tenant_id, fallback=default_tenant)
        )
        sensor_count = sum(1 for s in all_sensors if isinstance(s, dict) and _tenant_matches(_tenant_id_from_sensor(s), tenant_id))
        base_station_count = sum(
            1
            for _, bs_data in (bs_config or {}).items()
            if isinstance(bs_data, dict) and _tenant_matches(_tenant_id_from_base_station(bs_data), tenant_id)
        )

        ts_counts = {"inventory_events": 0, "telemetry_points": 0}
        conn, conn_err = _timescale_connect()
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM inventory_events WHERE tenant_id = %s", (tenant_id,))
                    ts_counts["inventory_events"] = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute("SELECT COUNT(*) FROM telemetry_uplink WHERE tenant_id = %s", (tenant_id,))
                    ts_counts["telemetry_points"] = int((cur.fetchone() or [0])[0] or 0)
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        has_usage = any([
            user_count > 0,
            sensor_count > 0,
            base_station_count > 0,
            ts_counts["inventory_events"] > 0,
            ts_counts["telemetry_points"] > 0,
        ])
        if has_usage and not force_delete:
            return jsonify({
                "success": False,
                "error": "Tenant contains assigned data. Use force=true to purge historical data.",
                "usage": {
                    "users": user_count,
                    "sensors": sensor_count,
                    "base_stations": base_station_count,
                    "inventory_events": ts_counts["inventory_events"],
                    "telemetry_points": ts_counts["telemetry_points"],
                },
            }), 400

        registry = load_tenant_registry()
        registry_entries = [
            item for item in registry.get("tenants", [])
            if isinstance(item, dict) and _sanitize_tenant_id(item.get("id")) != tenant_id
        ]
        registry["tenants"] = registry_entries
        save_tenant_registry(registry)

        conn, conn_err = _timescale_connect()
        timescale_result = {"purged": False, "error": conn_err}
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    if force_delete:
                        cur.execute("DELETE FROM telemetry_uplink WHERE tenant_id = %s", (tenant_id,))
                        cur.execute("DELETE FROM inventory_snapshot_latest WHERE tenant_id = %s", (tenant_id,))
                        cur.execute("DELETE FROM inventory_snapshot_points WHERE tenant_id = %s", (tenant_id,))
                        cur.execute("DELETE FROM inventory_events WHERE tenant_id = %s", (tenant_id,))
                    cur.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))
                timescale_result = {"purged": bool(force_delete), "error": None}
            except Exception as exc:
                timescale_result = {"purged": False, "error": str(exc)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        _record_admin_audit(
            action='tenant.delete',
            entity='tenant',
            target_id=tenant_id,
            status='success',
            details={
                'force': bool(force_delete),
                'users': user_count,
                'sensors': sensor_count,
                'base_stations': base_station_count,
                'inventory_events': ts_counts.get('inventory_events', 0),
                'telemetry_points': ts_counts.get('telemetry_points', 0),
                'timescale_purged': bool(timescale_result.get('purged')),
                'timescale_error': timescale_result.get('error'),
            },
        )
        return jsonify({
            "success": True,
            "tenant_id": tenant_id,
            "timescale": timescale_result,
        })

    for user in users_data.get("users", {}).values():
        if isinstance(user, dict):
            role = _normalize_user_role(user.get("role", "viewer"))
            if role == "admin":
                continue
            tenant_ids.add(_normalize_user_tenant_for_role(role, user.get("tenant_id"), fallback=default_tenant))

    for sensor in all_sensors:
        if isinstance(sensor, dict):
            tenant_ids.add(_tenant_id_from_sensor(sensor))

    for bs_data in bs_config.values():
        if isinstance(bs_data, dict):
            tenant_ids.add(_tenant_id_from_base_station(bs_data))

    timescale = {"enabled": bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)), "tenants": []}
    timescale_meta = {}
    conn, err = _timescale_connect()
    if conn is not None:
        try:
            _ensure_timescale_schema(conn)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        t.id,
                        t.name,
                        t.created_at,
                        COALESCE(ev.cnt, 0)::BIGINT AS event_count,
                        COALESCE(te.cnt, 0)::BIGINT AS telemetry_count
                    FROM tenants t
                    LEFT JOIN (
                        SELECT tenant_id, COUNT(*) AS cnt
                        FROM inventory_events
                        GROUP BY tenant_id
                    ) ev ON ev.tenant_id = t.id
                    LEFT JOIN (
                        SELECT tenant_id, COUNT(*) AS cnt
                        FROM telemetry_uplink
                        GROUP BY tenant_id
                    ) te ON te.tenant_id = t.id
                    ORDER BY t.id ASC
                """)
                for row in (cur.fetchall() or []):
                    tenant_ids.add(_normalize_tenant_id(row[0], fallback=default_tenant))
                    normalized_id = _normalize_tenant_id(row[0], fallback=default_tenant)
                    meta_entry = {
                        "id": normalized_id,
                        "name": str(row[1] or row[0] or "").strip() or normalized_id,
                        "created_at": str(row[2]) if row[2] else None,
                        "inventory_events": int(row[3] or 0),
                        "telemetry_points": int(row[4] or 0),
                    }
                    timescale_meta[normalized_id] = meta_entry
                    timescale["tenants"].append(meta_entry)
        except Exception as exc:
            timescale["error"] = str(exc)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    else:
        timescale["error"] = err

    tenant_summaries = []
    sorted_tenants = sorted(tenant_ids)
    for tenant_id in sorted_tenants:
        sensor_count = sum(1 for s in all_sensors if isinstance(s, dict) and _tenant_matches(_tenant_id_from_sensor(s), tenant_id))
        base_station_count = sum(
            1
            for _, bs_data in (bs_config or {}).items()
            if isinstance(bs_data, dict) and _tenant_matches(_tenant_id_from_base_station(bs_data), tenant_id)
        )
        user_count = sum(
            1
            for _, user in users_data.get("users", {}).items()
            if _user_belongs_to_tenant(user, tenant_id, fallback=default_tenant)
        )
        registry_entry = registry_map.get(tenant_id, {})
        ts_entry = timescale_meta.get(tenant_id, {})
        tenant_name = str(
            registry_entry.get("name")
            or ts_entry.get("name")
            or tenant_id
        ).strip() or tenant_id
        tenant_description = str(registry_entry.get("description") or "").strip()
        created_at = (
            registry_entry.get("created_at")
            or ts_entry.get("created_at")
            or None
        )
        tenant_summaries.append({
            "tenant_id": tenant_id,
            "name": tenant_name,
            "description": tenant_description,
            "created_at": created_at,
            "sensor_count": sensor_count,
            "base_station_count": base_station_count,
            "user_count": user_count,
            "can_delete": (
                tenant_id != default_tenant
                and user_count == 0
                and sensor_count == 0
                and base_station_count == 0
                and int(ts_entry.get("inventory_events") or 0) == 0
                and int(ts_entry.get("telemetry_points") or 0) == 0
            ),
        })

    return jsonify({
        "success": True,
        "default_tenant": default_tenant,
        "tenants": tenant_summaries,
        "timescale": timescale,
    })

@app.route('/api/tenants/export', methods=['GET'])
@login_required
@admin_scope_required('manage_tenants')
def export_tenant_data():
    tenant_id = _normalize_tenant_id(request.args.get("tenant_id"), fallback=_active_tenant_id())
    include_timescale = _parse_bool_arg(request.args.get("include_timescale"), default=True)
    telemetry_limit = max(1, min(int(request.args.get("telemetry_limit", 50000)), 250000))
    events_limit = max(1, min(int(request.args.get("events_limit", 50000)), 250000))

    sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=tenant_id)
    config = load_base_station_config()
    bs_map = _filter_base_stations_for_tenant(config.get("base_stations", {}), tenant_id=tenant_id)

    state = _load_coverage_positions_state()
    positions = state.get("positions", {}) if isinstance(state, dict) else {}
    positions = positions if isinstance(positions, dict) else {}
    sensor_keys = {f"sensor_{str(s.get('eui', '')).strip().upper()}" for s in sensors}
    bs_keys = {f"bs_{str(eui).strip().upper()}" for eui in bs_map.keys()}
    include_keys = sensor_keys | bs_keys
    export_positions = {key: value for key, value in positions.items() if key in include_keys}

    users_data = load_users()
    tenant_users = []
    for username, user in users_data.get("users", {}).items():
        if not isinstance(user, dict):
            continue
        if _user_belongs_to_tenant(user, tenant_id, fallback=_default_tenant_id()):
            tenant_users.append({
                "username": username,
                "name": user.get("name", ""),
                "role": user.get("role", "viewer"),
                "tenant_id": tenant_id,
            })

    tenant_registry_entry = _tenant_registry_map().get(tenant_id, {})
    payload = {
        "format_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "tenant_id": tenant_id,
        "tenant_name": tenant_registry_entry.get("name", tenant_id),
        "tenant_description": tenant_registry_entry.get("description", ""),
        "data": {
            "sensors": sensors,
            "base_stations": bs_map,
            "coverage_positions": {"positions": export_positions},
            "users": tenant_users,
        },
    }

    if include_timescale and bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)):
        ok, err, ts_dump = _timescale_fetch_tenant_dump(
            tenant_id=tenant_id,
            telemetry_limit=telemetry_limit,
            events_limit=events_limit,
        )
        payload["timescale"] = {
            "included": ok,
            "error": err,
            "limits": {"telemetry_limit": telemetry_limit, "events_limit": events_limit},
            "data": ts_dump if ok else {},
        }
    else:
        payload["timescale"] = {"included": False, "error": "disabled or not requested", "data": {}}

    _record_admin_audit(
        action="tenant.export",
        entity="tenant",
        target_id=tenant_id,
        status="success",
        details={
            "include_timescale": include_timescale,
            "telemetry_limit": telemetry_limit,
            "events_limit": events_limit,
            "sensor_count": len(sensors),
            "base_station_count": len(bs_map),
            "position_count": len(export_positions),
            "user_count": len(tenant_users),
            "timescale_included": bool(payload.get("timescale", {}).get("included")),
            "timescale_error": payload.get("timescale", {}).get("error"),
        },
    )

    content = json.dumps(payload, indent=2, ensure_ascii=True)
    filename = f"tenant_export_{tenant_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return Response(
        content,
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )

@app.route('/api/tenants/import', methods=['POST'])
@login_required
@admin_scope_required('manage_tenants')
def import_tenant_data():
    target_tenant = _normalize_tenant_id(
        request.args.get("tenant_id") or (request.form.get("tenant_id") if request.form else None),
        fallback=_active_tenant_id()
    )
    include_timescale = _parse_bool_arg(request.args.get("include_timescale"), default=True)
    merge_mode = _parse_bool_arg(request.args.get("merge"), default=True)

    try:
        if "file" in request.files and request.files["file"].filename:
            raw = request.files["file"].read().decode("utf-8")
            payload = json.loads(raw)
        else:
            payload = request.get_json(silent=True) or {}
    except Exception as exc:
        _record_admin_audit(
            action="tenant.import",
            entity="tenant",
            target_id=target_tenant,
            status="error",
            details={
                "error": f"Invalid import payload: {exc}",
                "include_timescale": include_timescale,
                "merge_mode": merge_mode,
            },
        )
        return jsonify({"success": False, "error": f"Invalid import payload: {exc}"}), 400

    imported = payload.get("data", payload)
    sensors_in = imported.get("sensors", []) if isinstance(imported, dict) else []
    bs_in = imported.get("base_stations", {}) if isinstance(imported, dict) else {}
    coverage_in = imported.get("coverage_positions", {}) if isinstance(imported, dict) else {}
    ts_in = payload.get("timescale", {}).get("data", {}) if isinstance(payload, dict) else {}
    incoming_tenant_name = ""
    incoming_tenant_description = ""
    if isinstance(payload, dict):
        incoming_tenant_name = str(payload.get("tenant_name") or payload.get("name") or "").strip()
        incoming_tenant_description = str(payload.get("tenant_description") or "").strip()
    _upsert_tenant_registry_entry(
        tenant_id=target_tenant,
        name=incoming_tenant_name or target_tenant,
        description=incoming_tenant_description,
    )

    sensors_all = _load_all_sensors()
    if not merge_mode:
        sensors_all = [s for s in sensors_all if not _tenant_matches(_tenant_id_from_sensor(s), target_tenant)]

    sensor_index = {}
    for idx, sensor in enumerate(sensors_all):
        if not isinstance(sensor, dict):
            continue
        sensor_index[(_tenant_id_from_sensor(sensor), str(sensor.get("eui", "")).strip().upper())] = idx

    sensor_created = 0
    sensor_updated = 0
    for sensor in (sensors_in or []):
        if not isinstance(sensor, dict):
            continue
        normalized = _normalize_sensor_payload(sensor)
        if not normalized.get("eui"):
            continue
        normalized["tenant_id"] = target_tenant
        key = (target_tenant, normalized["eui"])
        if key in sensor_index:
            sensors_all[sensor_index[key]].update(normalized)
            sensor_updated += 1
        else:
            sensors_all.append(normalized)
            sensor_index[key] = len(sensors_all) - 1
            sensor_created += 1
    _save_all_sensors(sensors_all)

    config = load_base_station_config()
    bs_map = config.setdefault("base_stations", {})
    if not merge_mode:
        keys_to_delete = [
            key for key, value in bs_map.items()
            if isinstance(value, dict) and _tenant_matches(_tenant_id_from_base_station(value), target_tenant)
        ]
        for key in keys_to_delete:
            bs_map.pop(key, None)

    bs_created = 0
    bs_updated = 0
    if isinstance(bs_in, dict):
        source_bs_items = bs_in.items()
    elif isinstance(bs_in, list):
        source_bs_items = [
            (str(item.get("eui", "")).strip().lower(), item)
            for item in bs_in
            if isinstance(item, dict)
        ]
    else:
        source_bs_items = []
    for key, bs_data in source_bs_items:
        if not isinstance(bs_data, dict):
            continue
        eui = str((bs_data.get("eui") or key or "")).strip().lower()
        if not eui or not _validate_eui(eui):
            continue
        payload_bs = {
            "name": str(bs_data.get("name", "") or "").strip(),
            "tags": bs_data.get("tags", []) if isinstance(bs_data.get("tags", []), list) else [],
            "ip": str(bs_data.get("ip", "") or "").strip(),
            "tenant_id": target_tenant,
        }
        gps_lat, gps_lng = _normalize_gps_coordinates(bs_data.get("gps_lat"), bs_data.get("gps_lng"))
        payload_bs["gps_lat"] = gps_lat
        payload_bs["gps_lng"] = gps_lng
        if eui in bs_map:
            bs_map[eui].update(payload_bs)
            bs_updated += 1
        else:
            bs_map[eui] = payload_bs
            bs_created += 1
    save_base_station_config(config)

    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})
    if not isinstance(positions, dict):
        positions = {}
        state["positions"] = positions
    incoming_positions = coverage_in.get("positions", {}) if isinstance(coverage_in, dict) else {}
    for key, value in (incoming_positions or {}).items():
        if not isinstance(value, dict):
            continue
        device_type = str(value.get("deviceType", "")).strip().lower()
        if device_type == "sensor":
            eui = str(value.get("eui", "")).strip().upper()
            if not any(
                isinstance(s, dict)
                and str(s.get("eui", "")).strip().upper() == eui
                and _tenant_matches(_tenant_id_from_sensor(s), target_tenant)
                for s in sensors_all
            ):
                continue
        elif device_type == "bs":
            eui = str(value.get("eui", "")).strip().lower()
            bs_row = bs_map.get(eui, {})
            if not isinstance(bs_row, dict) or not _tenant_matches(_tenant_id_from_base_station(bs_row), target_tenant):
                continue
        positions[key] = value
    _save_coverage_positions_state(state)

    ts_result = {"imported": False, "error": "not requested", "counts": {}}
    if include_timescale and bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)) and isinstance(ts_in, dict):
        conn, err = _timescale_connect()
        if conn is None:
            ts_result = {"imported": False, "error": err, "counts": {}}
        else:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO NOTHING
                    """, (target_tenant, target_tenant))

                    if not merge_mode:
                        cur.execute("DELETE FROM telemetry_uplink WHERE tenant_id = %s", (target_tenant,))
                        cur.execute("DELETE FROM inventory_snapshot_points WHERE tenant_id = %s", (target_tenant,))
                        cur.execute("DELETE FROM inventory_snapshot_latest WHERE tenant_id = %s", (target_tenant,))
                        cur.execute("DELETE FROM inventory_events WHERE tenant_id = %s", (target_tenant,))

                    events_rows = ts_in.get("inventory_events", []) or []
                    snapshot_points_rows = ts_in.get("inventory_snapshot_points", []) or []
                    snapshot_latest_rows = ts_in.get("inventory_snapshot_latest", []) or []
                    telemetry_rows = ts_in.get("telemetry_uplink", []) or []

                    if events_rows:
                        cur.executemany("""
                            INSERT INTO inventory_events
                                (ts, tenant_id, entity_type, action, eui, actor, event, has_payload, payload_size, source, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, [
                            (
                                row.get("ts"),
                                target_tenant,
                                row.get("entity_type"),
                                row.get("action"),
                                row.get("eui"),
                                row.get("actor") or "import",
                                row.get("event") or "imported",
                                bool(row.get("has_payload", False)),
                                int(row.get("payload_size", 0) or 0),
                                row.get("source") or "tenant_import",
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in events_rows if isinstance(row, dict)
                        ])

                    if snapshot_points_rows:
                        cur.executemany("""
                            INSERT INTO inventory_snapshot_points
                                (ts, tenant_id, entity_type, eui, status, trigger, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, [
                            (
                                row.get("ts"),
                                target_tenant,
                                row.get("entity_type"),
                                row.get("eui"),
                                row.get("status"),
                                row.get("trigger") or "import",
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in snapshot_points_rows if isinstance(row, dict)
                        ])

                    for row in snapshot_latest_rows:
                        if not isinstance(row, dict):
                            continue
                        cur.execute("""
                            INSERT INTO inventory_snapshot_latest
                                (tenant_id, entity_type, eui, status, trigger, payload, updated_at)
                            VALUES
                                (%s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s::timestamptz, NOW()))
                            ON CONFLICT (tenant_id, entity_type, eui)
                            DO UPDATE SET
                                status = EXCLUDED.status,
                                trigger = EXCLUDED.trigger,
                                payload = EXCLUDED.payload,
                                updated_at = EXCLUDED.updated_at
                        """, (
                            target_tenant,
                            row.get("entity_type"),
                            row.get("eui"),
                            row.get("status"),
                            row.get("trigger") or "import",
                            json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            row.get("updated_at"),
                        ))

                    if telemetry_rows:
                        cur.executemany("""
                            INSERT INTO telemetry_uplink
                                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, [
                            (
                                row.get("ts"),
                                target_tenant,
                                str(row.get("sensor_eui") or "").strip().lower(),
                                (str(row.get("base_station_eui") or "").strip().lower() or None),
                                float(row.get("snr")) if row.get("snr") is not None else None,
                                float(row.get("rssi")) if row.get("rssi") is not None else None,
                                float(row.get("packet_loss_pct")) if row.get("packet_loss_pct") is not None else None,
                                int(row.get("packet_cnt")) if row.get("packet_cnt") is not None else None,
                                str(row.get("msg_type") or "ul")[:16],
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in telemetry_rows if isinstance(row, dict)
                        ])

                ts_result = {
                    "imported": True,
                    "error": None,
                    "counts": {
                        "inventory_events": len(events_rows),
                        "inventory_snapshot_points": len(snapshot_points_rows),
                        "inventory_snapshot_latest": len(snapshot_latest_rows),
                        "telemetry_uplink": len(telemetry_rows),
                    },
                }
            except Exception as exc:
                ts_result = {"imported": False, "error": str(exc), "counts": {}}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    _try_record_inventory_event(
        "tenant",
        "imported",
        target_tenant,
        {
            "sensor_created": sensor_created,
            "sensor_updated": sensor_updated,
            "base_station_created": bs_created,
            "base_station_updated": bs_updated,
            "merge_mode": merge_mode,
            "timescale_imported": bool(ts_result.get("imported")),
        }
    )

    import_status = "success"
    if include_timescale and not bool(ts_result.get("imported")):
        import_status = "warning"

    _record_admin_audit(
        action="tenant.import",
        entity="tenant",
        target_id=target_tenant,
        status=import_status,
        details={
            "merge_mode": merge_mode,
            "include_timescale": include_timescale,
            "sensors_created": sensor_created,
            "sensors_updated": sensor_updated,
            "base_stations_created": bs_created,
            "base_stations_updated": bs_updated,
            "timescale": ts_result,
        },
    )

    return jsonify({
        "success": True,
        "tenant_id": target_tenant,
        "merge_mode": merge_mode,
        "sensors": {"created": sensor_created, "updated": sensor_updated},
        "base_stations": {"created": bs_created, "updated": bs_updated},
        "timescale": ts_result,
    })

def _safe_positive_int(value, default=1, min_value=1, max_value=10_000_000):
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    if parsed < min_value:
        return min_value
    if parsed > max_value:
        return max_value
    return parsed

def _safe_refresh_seconds(value, default=10):
    return _safe_positive_int(value, default=default, min_value=5, max_value=300)

def _grafana_base_url():
    base = str(getattr(bssci_config, "GRAFANA_URL", "http://localhost:3000") or "").strip()
    return (base or "http://localhost:3000").rstrip("/")

def _grafana_internal_base_url():
    internal = str(getattr(bssci_config, "GRAFANA_INTERNAL_URL", "") or "").strip()
    if internal:
        return internal.rstrip("/")
    return _grafana_base_url()

def _grafana_dashboard_uid():
    uid = str(getattr(bssci_config, "GRAFANA_DASHBOARD_UID", "service-center-overview") or "").strip()
    return uid or "service-center-overview"

def _grafana_dashboard_slug():
    raw_slug = str(getattr(bssci_config, "GRAFANA_DASHBOARD_SLUG", "service-center-overview") or "").strip().lower()
    raw_slug = re.sub(r"[^a-z0-9-]+", "-", raw_slug)
    raw_slug = raw_slug.strip("-")
    return raw_slug or "service-center-overview"

def _grafana_org_id():
    return _safe_positive_int(getattr(bssci_config, "GRAFANA_ORG_ID", 1), default=1, min_value=1, max_value=100_000)

def _grafana_embed_enabled():
    return bool(getattr(bssci_config, "GRAFANA_EMBED_ENABLED", True))

def _grafana_proxy_enabled():
    return bool(getattr(bssci_config, "GRAFANA_PROXY_ENABLED", True))

def _grafana_proxy_timeout_seconds():
    return _safe_positive_int(
        getattr(bssci_config, "GRAFANA_PROXY_TIMEOUT_SECONDS", 20),
        default=20,
        min_value=2,
        max_value=120,
    )

def _grafana_proxy_auth_header():
    bearer_token = str(getattr(bssci_config, "GRAFANA_PROXY_BEARER_TOKEN", "") or "").strip()
    if bearer_token:
        return f"Bearer {bearer_token}"

    username = str(getattr(bssci_config, "GRAFANA_PROXY_BASIC_USER", "") or "").strip()
    password = str(getattr(bssci_config, "GRAFANA_PROXY_BASIC_PASSWORD", "") or "")
    if username and password:
        raw = f"{username}:{password}".encode("utf-8")
        encoded = base64.b64encode(raw).decode("ascii")
        return f"Basic {encoded}"

    return ""

def _grafana_is_configured():
    base_url = _grafana_internal_base_url() if _grafana_proxy_enabled() else _grafana_base_url()
    return bool(str(base_url or "").strip()) and bool(str(_grafana_dashboard_uid() or "").strip())

def _grafana_requested_tenant_id():
    active_tenant = _active_tenant_id()
    requested_tenant = request.args.get("tenant_id")
    if requested_tenant and str(session.get("role", "")).strip().lower() == "admin":
        return _normalize_tenant_id(requested_tenant, fallback=active_tenant)
    return active_tenant

def _default_health_panel_map():
    return {
        "throughput": 1,
        "signal": 2,
        "active_sensors": 3,
        "active_base_stations": 4,
        "top_sensors": 5,
        "recent_messages": 6,
    }

def _grafana_health_panel_map():
    raw_map = str(getattr(bssci_config, "GRAFANA_HEALTH_PANEL_MAP", "") or "").strip()
    if not raw_map:
        return _default_health_panel_map()

    parsed_map = {}
    for token in raw_map.split(","):
        if ":" not in token:
            continue
        key_raw, value_raw = token.split(":", 1)
        key = str(key_raw or "").strip().lower()
        if not key:
            continue
        panel_id = _safe_positive_int(value_raw, default=0, min_value=0, max_value=1000)
        if panel_id > 0:
            parsed_map[key] = panel_id

    if not parsed_map:
        return _default_health_panel_map()

    defaults = _default_health_panel_map()
    for key, panel_id in defaults.items():
        parsed_map.setdefault(key, panel_id)
    return parsed_map

def _build_grafana_dashboard_params(tenant_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = {
        "orgId": _grafana_org_id(),
        "var-tenant": tenant_id,
    }
    if from_ms is not None and to_ms is not None:
        params["from"] = int(from_ms)
        params["to"] = int(to_ms)
    elif minutes is not None:
        minutes = _safe_positive_int(minutes, default=360, min_value=5, max_value=7 * 24 * 60)
        params["from"] = f"now-{minutes}m"
        params["to"] = "now"
    if refresh_seconds is not None:
        params["refresh"] = f"{_safe_refresh_seconds(refresh_seconds)}s"
    return params

def _build_grafana_dashboard_url(tenant_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_dashboard_params(
        tenant_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    return (
        f"{_grafana_base_url()}/d/{urllib.parse.quote(_grafana_dashboard_uid())}/"
        f"{urllib.parse.quote(_grafana_dashboard_slug())}?{urllib.parse.urlencode(params)}"
    )

def _build_grafana_panel_params(tenant_id, panel_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = {
        "orgId": _grafana_org_id(),
        "panelId": int(panel_id),
        "var-tenant": tenant_id,
        "theme": "light",
    }
    if from_ms is not None and to_ms is not None:
        params["from"] = int(from_ms)
        params["to"] = int(to_ms)
    elif minutes is not None:
        minutes = _safe_positive_int(minutes, default=360, min_value=5, max_value=7 * 24 * 60)
        params["from"] = f"now-{minutes}m"
        params["to"] = "now"
    if refresh_seconds is not None:
        params["refresh"] = f"{_safe_refresh_seconds(refresh_seconds)}s"
    return params

def _build_grafana_panel_url(tenant_id, panel_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_panel_params(
        tenant_id,
        panel_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    return (
        f"{_grafana_base_url()}/d-solo/{urllib.parse.quote(_grafana_dashboard_uid())}/"
        f"{urllib.parse.quote(_grafana_dashboard_slug())}?{urllib.parse.urlencode(params)}"
    )

def _build_grafana_proxy_url(proxy_path, params=None):
    clean_path = str(proxy_path or "").strip().lstrip("/")
    query = urllib.parse.urlencode(params or {}, doseq=True)
    base = f"/grafana-proxy/{clean_path}" if clean_path else "/grafana-proxy/"
    if query:
        return f"{base}?{query}"
    return base

def _build_grafana_dashboard_proxy_url(tenant_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_dashboard_params(
        tenant_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    params["tenant_id"] = tenant_id
    params.pop("var-tenant", None)
    return _build_grafana_proxy_url(
        f"d/{urllib.parse.quote(_grafana_dashboard_uid())}/{urllib.parse.quote(_grafana_dashboard_slug())}",
        params=params,
    )

def _build_grafana_panel_proxy_url(tenant_id, panel_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_panel_params(
        tenant_id,
        panel_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    params["tenant_id"] = tenant_id
    params.pop("var-tenant", None)
    return _build_grafana_proxy_url(
        f"d-solo/{urllib.parse.quote(_grafana_dashboard_uid())}/{urllib.parse.quote(_grafana_dashboard_slug())}",
        params=params,
    )

def _should_enforce_grafana_tenant(proxy_path):
    normalized = str(proxy_path or "").strip().lstrip("/").lower()
    return (
        normalized.startswith("d/")
        or normalized.startswith("d-solo/")
        or normalized.startswith("render/d/")
        or normalized.startswith("render/d-solo/")
    )

def _inject_tenant_scoped_var(payload, tenant_id):
    if not isinstance(payload, dict):
        return payload

    tenant_scope = {"text": tenant_id, "value": tenant_id, "selected": True}
    scoped_vars = payload.get("scopedVars")
    if isinstance(scoped_vars, dict):
        scoped_vars["tenant"] = tenant_scope

    queries = payload.get("queries")
    if isinstance(queries, list):
        for query in queries:
            if not isinstance(query, dict):
                continue
            query_scoped = query.get("scopedVars")
            if isinstance(query_scoped, dict):
                query_scoped["tenant"] = tenant_scope

    return payload

def _build_grafana_proxy_upstream_url(proxy_path):
    internal_base = _grafana_internal_base_url()
    parsed_base = urllib.parse.urlsplit(internal_base)
    base_path = parsed_base.path.rstrip("/")
    normalized_proxy_path = str(proxy_path or "").lstrip("/")
    upstream_path = f"{base_path}/{normalized_proxy_path}" if normalized_proxy_path else (base_path or "/")

    query_pairs = []
    for key in request.args:
        if key.lower() in {"var-tenant", "tenant_id"}:
            continue
        for value in request.args.getlist(key):
            query_pairs.append((key, value))

    if _should_enforce_grafana_tenant(proxy_path):
        query_pairs.append(("var-tenant", _grafana_requested_tenant_id()))

    upstream_query = urllib.parse.urlencode(query_pairs, doseq=True)
    return urllib.parse.urlunsplit(
        (
            parsed_base.scheme or "http",
            parsed_base.netloc,
            upstream_path,
            upstream_query,
            "",
        )
    )

def _rewrite_grafana_location_header(location):
    raw_location = str(location or "").strip()
    if not raw_location:
        return raw_location

    proxy_prefix = "/grafana-proxy"
    if raw_location.startswith(proxy_prefix):
        return raw_location

    parsed_base = urllib.parse.urlsplit(_grafana_internal_base_url())
    parsed_location = urllib.parse.urlsplit(raw_location)

    if parsed_location.scheme and parsed_location.netloc:
        if parsed_location.netloc == parsed_base.netloc:
            return urllib.parse.urlunsplit(
                (
                    "",
                    "",
                    f"{proxy_prefix}{parsed_location.path}",
                    parsed_location.query,
                    parsed_location.fragment,
                )
            )
        return raw_location

    if raw_location.startswith("/"):
        return f"{proxy_prefix}{raw_location}"
    return raw_location

def _rewrite_grafana_text_body(body_text):
    proxy_prefix = "/grafana-proxy"
    text = str(body_text or "")
    replacements = (
        ('href="/', f'href="{proxy_prefix}/'),
        ('src="/', f'src="{proxy_prefix}/'),
        ('action="/', f'action="{proxy_prefix}/'),
        ("url(/", f"url({proxy_prefix}/"),
        ('"/public/', f'"{proxy_prefix}/public/'),
        ("'/public/", f"'{proxy_prefix}/public/"),
        ('"/api/', f'"{proxy_prefix}/api/'),
        ("'/api/", f"'{proxy_prefix}/api/"),
        ('"/avatar/', f'"{proxy_prefix}/avatar/'),
        ("'/avatar/", f"'{proxy_prefix}/avatar/"),
        ('"/login', f'"{proxy_prefix}/login'),
        ("'/login", f"'{proxy_prefix}/login"),
        ('"/logout', f'"{proxy_prefix}/logout'),
        ("'/logout", f"'{proxy_prefix}/logout"),
    )
    for source, target in replacements:
        text = text.replace(source, target)

    text = re.sub(r'("appSubUrl"\s*:\s*")([^"]*)(")', r'\1/grafana-proxy\3', text)
    while "/grafana-proxy/grafana-proxy/" in text:
        text = text.replace("/grafana-proxy/grafana-proxy/", "/grafana-proxy/")
    return text

@app.route('/grafana-proxy', defaults={'proxy_path': ''}, methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
@app.route('/grafana-proxy/<path:proxy_path>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
@login_required
def grafana_proxy(proxy_path):
    if not _grafana_proxy_enabled():
        return jsonify({"success": False, "error": "Grafana proxy is disabled."}), 403
    if not _grafana_is_configured():
        return jsonify({"success": False, "error": "Grafana is not configured."}), 503

    upstream_url = _build_grafana_proxy_upstream_url(proxy_path)
    request_body = request.get_data(cache=False, as_text=False) or b""
    normalized_path = str(proxy_path or "").strip().lstrip("/").lower()
    tenant_id = _grafana_requested_tenant_id()

    if normalized_path.startswith("api/ds/query") and request_body:
        try:
            parsed_payload = json.loads(request_body.decode("utf-8"))
            parsed_payload = _inject_tenant_scoped_var(parsed_payload, tenant_id)
            request_body = json.dumps(parsed_payload, separators=(",", ":")).encode("utf-8")
        except Exception:
            pass

    forwarded_headers = {}
    for header_name, header_value in request.headers.items():
        lower_name = header_name.lower()
        if lower_name in {"host", "content-length", "accept-encoding", "cookie"}:
            continue
        if lower_name in {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}:
            continue
        forwarded_headers[header_name] = header_value
    forwarded_headers["Accept-Encoding"] = "identity"
    forwarded_headers["X-Forwarded-Host"] = request.host
    forwarded_headers["X-Forwarded-Proto"] = request.scheme
    forwarded_headers["X-Forwarded-Prefix"] = "/grafana-proxy"
    upstream_auth = _grafana_proxy_auth_header()
    if upstream_auth:
        forwarded_headers["Authorization"] = upstream_auth

    upstream_request = urllib.request.Request(
        upstream_url,
        data=request_body if request.method in {"POST", "PUT", "PATCH", "DELETE"} else None,
        headers=forwarded_headers,
        method=request.method,
    )

    def _build_proxy_response(status_code, upstream_headers, body_bytes):
        response_headers = {}
        for header_name, header_value in list(upstream_headers.items()):
            lower_name = header_name.lower()
            if lower_name in {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length"}:
                continue
            if lower_name == "location":
                response_headers[header_name] = _rewrite_grafana_location_header(header_value)
                continue
            response_headers[header_name] = header_value

        content_type = str(response_headers.get("Content-Type", "") or "").lower()
        if any(token in content_type for token in ("text/html", "text/css", "application/javascript", "text/javascript")):
            try:
                rewritten = _rewrite_grafana_text_body(body_bytes.decode("utf-8"))
                body_bytes = rewritten.encode("utf-8")
            except Exception:
                pass

        response = Response(body_bytes, status=status_code)
        for header_name, header_value in response_headers.items():
            response.headers[header_name] = header_value
        response.headers.pop("X-Frame-Options", None)
        return response

    try:
        with urllib.request.urlopen(
            upstream_request,
            timeout=_grafana_proxy_timeout_seconds(),
            context=ssl._create_unverified_context(),
        ) as upstream_response:
            upstream_body = upstream_response.read()
            return _build_proxy_response(upstream_response.status, upstream_response.headers, upstream_body)
    except urllib.error.HTTPError as error:
        error_body = error.read() if hasattr(error, "read") else b""
        return _build_proxy_response(error.code, error.headers, error_body)
    except Exception as error:
        return jsonify({"success": False, "error": f"Grafana proxy failed: {error}"}), 502

@app.route('/api/grafana/dashboard-url', methods=['GET'])
@login_required
def grafana_dashboard_url():
    tenant_id = _grafana_requested_tenant_id()
    use_proxy = _grafana_proxy_enabled()
    return jsonify({
        "success": True,
        "tenant_id": tenant_id,
        "proxy_enabled": use_proxy,
        "url": (
            _build_grafana_dashboard_proxy_url(tenant_id)
            if use_proxy
            else _build_grafana_dashboard_url(tenant_id)
        ),
    })

@app.route('/api/health/grafana', methods=['GET'])
@login_required
def health_grafana_urls():
    minutes = _safe_positive_int(request.args.get("minutes"), default=360, min_value=5, max_value=7 * 24 * 60)
    raw_refresh_seconds = str(request.args.get("refresh_seconds", "") or "").strip().lower()
    if raw_refresh_seconds in {"", "0", "false", "off", "no"}:
        refresh_seconds = None
    else:
        refresh_seconds = _safe_refresh_seconds(raw_refresh_seconds, default=10)

    tenant_id = _grafana_requested_tenant_id()
    panel_map = _grafana_health_panel_map()
    use_proxy = _grafana_proxy_enabled()
    panels = {
        key: {
            "panel_id": int(panel_id),
            "url": (
                _build_grafana_panel_proxy_url(
                    tenant_id,
                    panel_id,
                    minutes=minutes,
                    refresh_seconds=refresh_seconds,
                )
                if use_proxy
                else _build_grafana_panel_url(
                    tenant_id,
                    panel_id,
                    minutes=minutes,
                    refresh_seconds=refresh_seconds,
                )
            ),
        }
        for key, panel_id in panel_map.items()
    }

    return jsonify({
        "success": True,
        "configured": _grafana_is_configured(),
        "embed_enabled": _grafana_embed_enabled(),
        "proxy_enabled": use_proxy,
        "anonymous_enabled": bool(getattr(bssci_config, "GRAFANA_ANONYMOUS_ENABLED", True)),
        "anonymous_org_role": str(getattr(bssci_config, "GRAFANA_ANONYMOUS_ORG_ROLE", "Viewer") or "Viewer"),
        "tenant_id": tenant_id,
        "minutes": minutes,
        "refresh_seconds": refresh_seconds,
        "from": f"now-{minutes}m",
        "to": "now",
        "dashboard_url": (
            _build_grafana_dashboard_proxy_url(
                tenant_id,
                minutes=minutes,
                refresh_seconds=refresh_seconds,
            )
            if use_proxy
            else _build_grafana_dashboard_url(
                tenant_id,
                minutes=minutes,
                refresh_seconds=refresh_seconds,
            )
        ),
        "panels": panels,
    })

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/sensors')
@login_required
def sensors():
    sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=_active_tenant_id())
    return render_template('sensors.html', sensors=sensors)

@app.route('/api/sensors', methods=['GET'])
@login_required
def get_sensors():
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        active_tenant = _active_tenant_id()
        
        # Load sensors from config file first
        sensor_status = {}
        try:
            sensor_file = getattr(bssci_config, 'SENSOR_CONFIG_FILE', 'endpoints.json')
            print(f"Loading sensors from file: {sensor_file}")
            sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
            print(f"Loaded {len(sensors)} sensors from file for tenant '{active_tenant}'")
                
            # Initialize sensor status from config file
            for sensor in sensors:
                eui = str(sensor.get('eui', '')).upper()
                if not eui:
                    continue
                sensor_status[eui] = {
                    'eui': eui,
                    'nwKey': sensor.get('nwKey', ''),
                    'shortAddr': sensor.get('shortAddr', '0000'),
                    'bidi': bool(sensor.get('bidi', False)),
                    'name': sensor.get('name', ''),
                    'tags': _normalize_sensor_tags(sensor.get('tags', [])),
                    'gps_lat': sensor.get('gps_lat'),
                    'gps_lng': sensor.get('gps_lng'),
                    'tenant_id': sensor.get('tenant_id', active_tenant),
                    'registered': False,
                    'registration_info': {},
                    'base_stations': [],
                    'runtime_base_stations': [],
                    'attached_base_stations': _normalize_base_station_route_list(
                        sensor.get('attached_base_stations', [])
                    ),
                    'missing_registrations': [],
                    'total_registrations': 0,
                    'total_available_bases': 0,
                    'preferredDownlinkPath': sensor.get('preferredDownlinkPath', None),
                    'activity_status': 'no_data',
                    'hours_since_last_seen': 0
                }
                
            # Get connected base stations list for missing registration tracking
            connected_bases = []
            if tls_server and hasattr(tls_server, 'connected_base_stations'):
                connected_bases = _normalize_base_station_route_list(
                    list(tls_server.connected_base_stations.values())
                )
                # Update total available bases for all sensors
                for sensor_eui in sensor_status:
                    sensor_status[sensor_eui]['total_available_bases'] = len(connected_bases)
            
            # Now safely get real registration data from TLS server
            if tls_server and hasattr(tls_server, 'registered_sensors'):
                try:
                    # Thread-safe access to registered sensors data
                    registered_dict = getattr(tls_server, 'registered_sensors', {})
                    print(f"Accessing registration data for {len(registered_dict)} registered sensors")
                    
                    for sensor_eui, reg_data in list(registered_dict.items()):
                        if sensor_eui in sensor_status:
                            try:
                                # Get base stations list safely
                                base_stations_list = _normalize_base_station_route_list(
                                    reg_data.get('base_stations', [])
                                )
                                registrations_list = reg_data.get('registrations', [])
                                
                                # Calculate missing registrations
                                missing_bases = [bs for bs in connected_bases if bs not in base_stations_list]
                                
                                sensor_status[sensor_eui].update({
                                    'registered': reg_data.get('status') == 'registered',
                                    'base_stations': base_stations_list,
                                    'runtime_base_stations': base_stations_list,
                                    'missing_registrations': missing_bases,
                                    'total_registrations': len(base_stations_list),
                                    'registration_info': {
                                        'status': reg_data.get('status', 'unknown'),
                                        'last_update': reg_data.get('registration_time', 'Unknown'),
                                        'registrations': registrations_list
                                    }
                                })
                                
                                print(f"Sensor {sensor_eui}: {len(base_stations_list)} base stations - {base_stations_list}")
                                
                            except Exception as e:
                                print(f"Error processing registration data for sensor {sensor_eui}: {e}")
                                
                except Exception as e:
                    print(f"Error accessing TLS server registration data: {e}")
            
            # For sensors without registration data, mark all connected bases as missing
            for sensor_eui in sensor_status:
                if not sensor_status[sensor_eui]['base_stations']:
                    sensor_status[sensor_eui]['missing_registrations'] = connected_bases.copy()
                    
            print(f"Processed sensor status for {len(sensor_status)} sensors with registration data")
            return jsonify(sensor_status)
        except FileNotFoundError:
            sensor_file = getattr(bssci_config, 'SENSOR_CONFIG_FILE', 'endpoints.json')
            print(f"Sensor config file not found: {sensor_file}")
            return jsonify({})
        except json.JSONDecodeError as e:
            sensor_file = getattr(bssci_config, 'SENSOR_CONFIG_FILE', 'endpoints.json')
            print(f"Invalid JSON in sensor config file {sensor_file}: {e}")
            return jsonify({})
            
    except Exception as e:
        print(f"Error in get_sensors: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/sensors', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def add_sensor():
    try:
        data = _normalize_sensor_payload(request.json or {})
    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400
    
    try:
        if not data.get('eui'):
            return jsonify({'success': False, 'message': 'EUI is required'})
        
        # Step 1: Save directly to endpoints.json
        sensors = _load_all_sensors()
        active_tenant = _normalize_tenant_id(data.get("tenant_id"), fallback=_active_tenant_id())
        data["tenant_id"] = active_tenant

        # Check if sensor already exists
        sensor_updated = False
        for sensor in sensors:
            if str(sensor.get('eui', '')).upper() != data['eui'].upper():
                continue
            existing_tenant = _tenant_id_from_sensor(sensor)
            if not _tenant_matches(existing_tenant, active_tenant):
                return jsonify({
                    'success': False,
                    'message': f"Sensor EUI already exists in tenant '{existing_tenant}'. Reassign first if needed."
                }), 409
            # Update existing sensor
            sensor.update(data)
            sensor_updated = True
            break
        
        if not sensor_updated:
            # Add new sensor
            sensors.append(data)

        # Save to file
        _save_all_sensors(sensors)

        _try_record_inventory_event(
            "sensor",
            "updated" if sensor_updated else "created",
            data.get("eui", ""),
            {
                "short_addr": data.get("shortAddr", ""),
                "bidi": bool(data.get("bidi", False)),
                "nwkey_present": bool(data.get("nwKey")),
                "name": data.get("name", ""),
                "tags_count": len(data.get("tags", [])),
                "gps_lat": data.get("gps_lat"),
                "gps_lng": data.get("gps_lng"),
                "tenant_id": active_tenant,
            }
        )

        _upsert_device_gps_position("sensor", data.get("eui", ""), data.get("gps_lat"), data.get("gps_lng"))
        _record_admin_audit(
            action='sensor.update' if sensor_updated else 'sensor.create',
            entity='sensor',
            target_id=data.get("eui", ""),
            status='success',
            details={
                "name": data.get("name", ""),
                "short_addr": data.get("shortAddr", ""),
                "bidi": bool(data.get("bidi", False)),
                "tags_count": len(data.get("tags", [])),
                "gps_lat": data.get("gps_lat"),
                "gps_lng": data.get("gps_lng"),
                "tenant_id": active_tenant,
            },
        )
        
        # Step 2: Notify TLS server to reload config and send attach requests
        global tls_server_instance
        tls_server = tls_server_instance
        
        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                # Reload the sensor configuration in TLS server
                tls_server.reload_sensor_config()
                
                # Force attach to connected base stations if any
                if hasattr(tls_server, 'connected_base_stations') and tls_server.connected_base_stations:
                    print(f"Triggering attach for new sensor {data['eui']} to {len(tls_server.connected_base_stations)} base stations")
                    
                    # Use simple synchronous method to send attach requests
                    if hasattr(tls_server, 'attach_sensor_sync'):
                        attached_count = tls_server.attach_sensor_sync(data['eui'])
                        if attached_count > 0:
                            connected_targets = [
                                _normalize_eui_upper(bs_eui)
                                for bs_eui in (getattr(tls_server, "connected_base_stations", {}) or {}).values()
                                if _normalize_eui_upper(bs_eui)
                            ]
                            _update_sensor_attached_base_stations(
                                data['eui'],
                                connected_targets,
                                tenant_id=active_tenant,
                            )
                            print(f"Successfully sent attach requests for {data['eui']} to {attached_count} base stations")
                            return jsonify({'success': True, 'message': f'Sensor saved and attach requests sent to {attached_count} base stations'})
                        else:
                            print(f"Failed to send attach requests for {data['eui']}")
                            return jsonify({'success': True, 'message': 'Sensor saved but failed to send attach requests'})
                    else:
                        return jsonify({'success': True, 'message': 'Sensor saved but attach function not available'})
                else:
                    return jsonify({'success': True, 'message': 'Sensor saved (no base stations connected for attach)'})
                            
                return jsonify({'success': True, 'message': 'Sensor saved and processed'})
            except Exception as e:
                print(f"Error notifying TLS server: {e}")
                return jsonify({'success': True, 'message': 'Sensor saved but failed to notify TLS server'})
        else:
            return jsonify({'success': True, 'message': 'Sensor saved (TLS server not available for attach)'})
                
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@app.route('/api/sensors/<eui>', methods=['DELETE'])
@login_required
@permission_required('can_edit_sensors')
def delete_sensor(eui):
    sensors = _load_all_sensors()
    active_tenant = _active_tenant_id()
    deleted_sensor = None
    kept = []
    for sensor in sensors:
        same_eui = str(sensor.get('eui', '')).upper() == eui.upper()
        same_tenant = _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant)
        if same_eui and same_tenant and deleted_sensor is None:
            deleted_sensor = sensor
            continue
        kept.append(sensor)

    try:
        _save_all_sensors(kept)
        _try_record_inventory_event(
            "sensor",
            "deleted",
            eui,
            {
                "short_addr": (deleted_sensor or {}).get("shortAddr", ""),
                "bidi": bool((deleted_sensor or {}).get("bidi", False)),
                "nwkey_present": bool((deleted_sensor or {}).get("nwKey")),
                "name": (deleted_sensor or {}).get("name", ""),
                "tags_count": len(_normalize_sensor_tags((deleted_sensor or {}).get("tags", []))),
                "gps_lat": (deleted_sensor or {}).get("gps_lat"),
                "gps_lng": (deleted_sensor or {}).get("gps_lng"),
                "tenant_id": active_tenant,
            }
        )
        _remove_device_position("sensor", eui)
        _record_admin_audit(
            action='sensor.delete',
            entity='sensor',
            target_id=eui,
            status='success',
            details={
                "name": (deleted_sensor or {}).get("name", ""),
                "short_addr": (deleted_sensor or {}).get("shortAddr", ""),
                "tenant_id": active_tenant,
            },
        )
        return jsonify({'success': True, 'message': 'Sensor deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/<eui>/attach', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def attach_sensor(eui):
    """Set sensor attach mapping (attach + detach in one endpoint)."""
    try:
        eui_upper = str(eui or "").strip().upper()
        active_tenant = _active_tenant_id()
        if not any(
            str(s.get("eui", "")).strip().upper() == eui_upper
            and _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
            for s in _load_all_sensors()
        ):
            return jsonify({'success': False, 'message': 'Sensor not found in active tenant'}), 404

        payload = request.get_json(silent=True) or {}
        requested_bases = payload.get('base_stations')
        if requested_bases is None:
            return jsonify({'success': False, 'message': 'Select one or more base stations for attach'}), 400

        if isinstance(requested_bases, str):
            requested_bases = [part.strip() for part in requested_bases.split(',') if part and part.strip()]
        if not isinstance(requested_bases, list):
            return jsonify({'success': False, 'message': 'base_stations must be an array of EUI strings'}), 400

        selected_base_stations = _normalize_base_station_route_list(requested_bases)
        if requested_bases and not selected_base_stations:
            return jsonify({'success': False, 'message': 'No valid base stations selected'}), 400

        global tls_server_instance
        tls_server = tls_server_instance

        # Empty selection means explicit detach / clear mapping.
        if not selected_base_stations:
            _update_sensor_attached_base_stations(eui_upper, [], tenant_id=active_tenant)

            runtime_detached = None
            runtime_error = None
            if tls_server and hasattr(tls_server, 'detach_sensor_sync'):
                try:
                    runtime_detached = bool(tls_server.detach_sensor_sync(eui))
                except Exception as exc:
                    runtime_error = str(exc)

            if tls_server and hasattr(tls_server, 'reload_sensor_config'):
                try:
                    tls_server.reload_sensor_config()
                except Exception:
                    pass

            message = f'Sensor {eui} set to detached (no base station mapping).'
            if runtime_detached is True:
                message = f'{message} Detached from online base stations.'
            elif runtime_detached is False:
                message = f'{message} Runtime detach will complete when base stations are reachable.'
            if runtime_error:
                message = f'{message} Runtime warning: {runtime_error}'

            _record_admin_audit(
                action='sensor.detach',
                entity='sensor',
                target_id=eui_upper,
                status='success',
                details={
                    'tenant_id': active_tenant,
                    'mapping_cleared': True,
                    'runtime_detached': bool(runtime_detached),
                    'runtime_error': runtime_error,
                },
            )

            return jsonify({
                'success': True,
                'message': message,
                'attached_count': 0,
                'pending_count': 0,
                'requested_base_stations': [],
                'online_base_stations': [],
                'pending_base_stations': [],
                'mapping_cleared': True,
            })

        # Validate that selected targets belong to inventory scope (tenant + currently connected).
        bs_config = _filter_base_stations_for_tenant(
            load_base_station_config().get("base_stations", {}),
            tenant_id=active_tenant,
        )
        configured_targets = {
            _normalize_eui_upper(bs_eui)
            for bs_eui in (bs_config or {}).keys()
            if _normalize_eui_upper(bs_eui)
        }

        connected_targets = set()
        if tls_server and hasattr(tls_server, 'connected_base_stations'):
            connected_targets = {
                _normalize_eui_upper(bs_eui)
                for bs_eui in (tls_server.connected_base_stations or {}).values()
                if _normalize_eui_upper(bs_eui)
            }

        allowed_targets = configured_targets | connected_targets
        unknown_targets = [bs for bs in selected_base_stations if bs not in allowed_targets]
        if unknown_targets:
            return jsonify({
                'success': False,
                'message': f"Unknown base stations for active tenant: {', '.join(unknown_targets)}"
            }), 400

        # Always persist desired attach mapping, even if target BS is offline.
        _update_sensor_attached_base_stations(eui_upper, selected_base_stations, tenant_id=active_tenant)
        _record_admin_audit(
            action='sensor.attach',
            entity='sensor',
            target_id=eui_upper,
            status='success',
            details={
                'tenant_id': active_tenant,
                'requested_base_stations': selected_base_stations,
                'requested_count': len(selected_base_stations),
                'connected_count_now': len(connected_targets),
            },
        )

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()
            except Exception:
                pass

        if tls_server and hasattr(tls_server, 'attach_sensor_sync'):
            attached_count = int(tls_server.attach_sensor_sync(eui, selected_base_stations) or 0)
            total_targets = len(selected_base_stations)
            pending_count = max(0, total_targets - attached_count)
            pending_targets = [bs for bs in selected_base_stations if bs not in connected_targets]
            online_targets = [bs for bs in selected_base_stations if bs in connected_targets]

            if attached_count > 0 and pending_count > 0:
                return jsonify({
                    'success': True,
                    'message': f'Sensor {eui} saved. Attached to {attached_count} online base stations; {pending_count} pending until reconnect.',
                    'attached_count': attached_count,
                    'pending_count': pending_count,
                    'requested_base_stations': selected_base_stations,
                    'online_base_stations': online_targets,
                    'pending_base_stations': pending_targets
                })
            if attached_count > 0:
                return jsonify({
                    'success': True,
                    'message': f'Sensor {eui} attached to {attached_count} base stations.',
                    'attached_count': attached_count,
                    'pending_count': 0,
                    'requested_base_stations': selected_base_stations,
                    'online_base_stations': online_targets,
                    'pending_base_stations': []
                })

            return jsonify({
                'success': True,
                'message': f'Sensor {eui} mapping saved. No selected base station is online now; attach will run after reconnect.',
                'attached_count': 0,
                'pending_count': len(selected_base_stations),
                'requested_base_stations': selected_base_stations,
                'online_base_stations': [],
                'pending_base_stations': selected_base_stations
            })

        return jsonify({
            'success': True,
            'message': f'Sensor {eui} mapping saved. Attach will run when TLS/base stations are available.',
            'attached_count': 0,
            'pending_count': len(selected_base_stations),
            'requested_base_stations': selected_base_stations,
            'online_base_stations': [],
            'pending_base_stations': selected_base_stations
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/<eui>/detach', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def detach_sensor(eui):
    """Detach a specific sensor from all base stations (desired-state first, runtime best-effort)."""
    try:
        active_tenant = _active_tenant_id()
        eui_upper = str(eui or "").strip().upper()
        if not any(
            str(s.get("eui", "")).strip().upper() == eui_upper
            and _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
            for s in _load_all_sensors()
        ):
            return jsonify({'success': False, 'message': 'Sensor not found in active tenant'}), 404

        # Always clear desired attach mapping first (works even if no online BS).
        _update_sensor_attached_base_stations(eui_upper, [], tenant_id=active_tenant)

        global tls_server_instance
        tls_server = tls_server_instance
        runtime_detached = None
        runtime_error = None

        if tls_server and hasattr(tls_server, 'detach_sensor_sync'):
            try:
                runtime_detached = bool(tls_server.detach_sensor_sync(eui))
            except Exception as exc:
                runtime_error = str(exc)

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()
            except Exception:
                pass

        if runtime_detached is True:
            message = f'Sensor {eui} detached from online base stations and mapping cleared.'
        elif runtime_detached is False:
            message = f'Sensor {eui} mapping cleared. Runtime detach will complete when base stations are reachable.'
        else:
            message = f'Sensor {eui} mapping cleared.'

        if runtime_error:
            message = f'{message} Runtime warning: {runtime_error}'

        _record_admin_audit(
            action='sensor.detach',
            entity='sensor',
            target_id=eui_upper,
            status='success',
            details={
                'tenant_id': active_tenant,
                'mapping_cleared': True,
                'runtime_detached': bool(runtime_detached),
                'runtime_error': runtime_error,
            },
        )

        return jsonify({
            'success': True,
            'message': message,
            'runtime_detached': bool(runtime_detached),
            'mapping_cleared': True,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

def _convert_topology_timestamps(receiving_bases, sensor_last_seen):
    """Use the sensor's real last_seen timestamp for all base stations"""
    if not receiving_bases:
        return {}
    result = {}
    for bs_eui, data in receiving_bases.items():
        result[bs_eui] = data.copy()
        if sensor_last_seen and sensor_last_seen > 0:
            result[bs_eui]['last_seen'] = sensor_last_seen
    return result

@app.route('/api/sensors/<eui>/details', methods=['GET'])
@login_required
def get_sensor_details(eui):
    """Get detailed statistics for a specific sensor"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        eui_upper = eui.upper()
        active_tenant = _active_tenant_id()
        
        # Get base sensor config
        sensor_config = None
        try:
            sensors = _load_all_sensors()
            for s in sensors:
                if (
                    str(s.get('eui', '')).upper() == eui_upper
                    and _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
                ):
                    sensor_config = dict(s)
                    sensor_config["tenant_id"] = _tenant_id_from_sensor(s)
                    break
        except:
            pass
        
        if not sensor_config:
            return jsonify({'success': False, 'message': 'Sensor not found'})
        
        # Get packet statistics
        stats = {}
        if tls_server and hasattr(tls_server, 'sensor_packet_stats'):
            stats = tls_server.sensor_packet_stats.get(eui_upper, {})
        
        # Get topology info
        topology = {}
        if tls_server and hasattr(tls_server, 'sensor_topology'):
            topology = tls_server.sensor_topology.get(eui_upper, {})
        
        # Get registration info
        registration = {}
        if tls_server and hasattr(tls_server, 'registered_sensors'):
            registration = tls_server.registered_sensors.get(eui_upper, {})
        
        # Get preferred downlink path
        downlink_path = {}
        if tls_server and hasattr(tls_server, 'preferred_downlink_paths'):
            downlink_path = tls_server.preferred_downlink_paths.get(eui_upper, {})
        
        # Calculate derived metrics
        avg_snr = stats.get('snr_sum', 0) / stats.get('snr_count', 1) if stats.get('snr_count', 0) > 0 else 0
        avg_rssi = stats.get('rssi_sum', 0) / stats.get('rssi_count', 1) if stats.get('rssi_count', 0) > 0 else 0
        packets_received = stats.get('packets_received', 0)
        packets_lost = stats.get('packets_lost', 0)
        packet_loss_rate = (packets_lost / (packets_received + packets_lost) * 100) if (packets_received + packets_lost) > 0 else 0
        
        # Calculate send interval (based on last 10 messages if history available)
        send_interval = 0
        if 'snr_history' in stats and len(stats['snr_history']) >= 2:
            history = stats['snr_history'][-10:]
            intervals = []
            for i in range(1, len(history)):
                intervals.append(history[i]['ts'] - history[i-1]['ts'])
            if intervals:
                send_interval = sum(intervals) / len(intervals)
        
        # Calculate device health scores
        signal_score = 5.0
        if avg_snr < -5:
            signal_score = 1.0
        elif avg_snr < 0:
            signal_score = 2.0
        elif avg_snr < 5:
            signal_score = 3.0
        elif avg_snr < 10:
            signal_score = 4.0
        
        energy_score = 3.0  # Default fair
        if stats.get('spreading_factor', 7) <= 7:
            energy_score = 5.0
        elif stats.get('spreading_factor', 7) <= 9:
            energy_score = 4.0
        elif stats.get('spreading_factor', 7) <= 10:
            energy_score = 3.0
        elif stats.get('spreading_factor', 7) <= 11:
            energy_score = 2.0
        else:
            energy_score = 1.0
        
        # Get gateway count from topology
        gateway_count = len(topology.get('receiving_bases', {})) if topology else 0
        
        # Calculate duty cycle
        total_airtime_ms = stats.get('total_airtime_ms', 0)
        first_seen = stats.get('first_seen', 0)
        last_seen = stats.get('last_seen', 0)
        observation_time_s = (last_seen - first_seen) if first_seen and last_seen else 1
        duty_cycle = (total_airtime_ms / 1000 / observation_time_s * 100) if observation_time_s > 0 else 0
        
        response = {
            'success': True,
            'eui': eui_upper,
            'config': sensor_config,
            'first_seen': stats.get('first_seen'),
            'last_seen': stats.get('last_seen'),
            'packets_received': packets_received,
            'packets_lost': packets_lost,
            'packet_loss_rate': round(packet_loss_rate, 2),
            'avg_snr': round(avg_snr, 2),
            'avg_rssi': round(avg_rssi, 2),
            'min_snr': stats.get('min_snr', 0),
            'max_snr': stats.get('max_snr', 0),
            'min_rssi': stats.get('min_rssi', 0),
            'max_rssi': stats.get('max_rssi', 0),
            'current_rssi': stats.get('rssi_sum', 0) / stats.get('rssi_count', 1) if stats.get('rssi_count', 0) > 0 else 0,
            'send_interval': round(send_interval, 1),
            'gateway_count': gateway_count,
            'primary_gateway': topology.get('primary_bs', ''),
            'receiving_gateways': list(topology.get('receiving_bases', {}).keys()) if topology else [],
            'receiving_bases_details': _convert_topology_timestamps(topology.get('receiving_bases', {}), last_seen) if topology else {},
            'signal_score': round(signal_score, 1),
            'energy_score': round(energy_score, 1),
            'spreading_factor': stats.get('spreading_factor', 7),
            'data_rate': stats.get('data_rate', 'SF7BW125'),
            'frequency_mhz': round(stats.get('frequency_mhz', 0), 3),
            'frame_counter': stats.get('frame_counter', 0),
            'last_airtime_ms': round(stats.get('last_airtime_ms', 0), 2),
            'avg_airtime_ms': round(total_airtime_ms / packets_received, 2) if packets_received > 0 else 0,
            'total_airtime_ms': round(total_airtime_ms, 2),
            'duty_cycle': round(duty_cycle, 4),
            'snr_history': stats.get('snr_history', [])[-50:],
            'rssi_history': stats.get('rssi_history', [])[-50:],
            'registration': registration,
            'downlink_path': downlink_path
        }
        
        return jsonify(response)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/attach-all', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def attach_all_sensors():
    """Attach all configured sensors to base stations"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        
        if not tls_server:
            return jsonify({'success': False, 'message': 'TLS server not available'})
        
        if not hasattr(tls_server, 'connected_base_stations') or not tls_server.connected_base_stations:
            return jsonify({'success': False, 'message': 'No base stations connected'})
        
        # Get all sensors from config file
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=_active_tenant_id())
        
        if not sensors:
            return jsonify({'success': False, 'message': 'No sensors configured to attach'})
        
        # Force reload sensor config to ensure all sensors are loaded
        tls_server.reload_sensor_config()
        
        # Send attach requests for all sensors to all connected base stations
        if hasattr(tls_server, 'attach_all_sensors_sync'):
            attached_count = tls_server.attach_all_sensors_sync()
            bs_count = len(tls_server.connected_base_stations)
            if attached_count > 0:
                connected_targets = [
                    _normalize_eui_upper(bs_eui)
                    for bs_eui in (tls_server.connected_base_stations or {}).values()
                    if _normalize_eui_upper(bs_eui)
                ]
                active_tenant = _active_tenant_id()
                all_sensors = _load_all_sensors()
                changed = False
                for sensor in all_sensors:
                    if not isinstance(sensor, dict):
                        continue
                    if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                        continue
                    sensor_eui = str(sensor.get("eui", "")).strip().upper()
                    if not sensor_eui:
                        continue
                    current_targets = _normalize_base_station_route_list(sensor.get("attached_base_stations", []))
                    if current_targets != connected_targets:
                        sensor["attached_base_stations"] = list(connected_targets)
                        changed = True
                if changed:
                    _save_all_sensors(all_sensors)
            message = f'Sent attach requests for {attached_count} sensors to {bs_count} base stations'
            _record_admin_audit(
                action='sensor.attach_all',
                entity='sensor',
                target_id='tenant',
                status='success',
                details={
                    'tenant_id': _active_tenant_id(),
                    'attached_count': attached_count,
                    'base_station_count': bs_count,
                },
            )
            return jsonify({'success': True, 'message': message})
        else:
            return jsonify({'success': False, 'message': 'Attach all function not available in TLS server'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/detach-all', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def detach_all_sensors():
    """Detach all sensors from base stations (desired-state first, runtime best-effort)."""
    try:
        global tls_server_instance
        tls_server = tls_server_instance

        # Always clear desired mappings for current tenant.
        active_tenant = _active_tenant_id()
        sensors = _load_all_sensors()
        changed = False
        for sensor in sensors:
            if not isinstance(sensor, dict):
                continue
            if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                continue
            if "attached_base_stations" in sensor:
                sensor.pop("attached_base_stations", None)
                changed = True
        if changed:
            _save_all_sensors(sensors)

        detached_count = 0
        runtime_error = None
        if tls_server and hasattr(tls_server, 'detach_all_sensors_sync'):
            try:
                detached_count = int(tls_server.detach_all_sensors_sync() or 0)
            except Exception as exc:
                runtime_error = str(exc)

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()
            except Exception:
                pass

        message = f'Cleared attach mapping for all sensors in tenant "{active_tenant}".'
        if detached_count > 0:
            message = f'{message} Detached {detached_count} runtime sensor bindings.'
        elif tls_server and hasattr(tls_server, 'detach_all_sensors_sync'):
            message = f'{message} No runtime bindings were detached (likely no online base stations).'
        if runtime_error:
            message = f'{message} Runtime warning: {runtime_error}'

        _record_admin_audit(
            action='sensor.detach_all',
            entity='sensor',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'detached_count': detached_count,
                'mapping_cleared': True,
                'runtime_error': runtime_error,
            },
        )

        return jsonify({
            'success': True,
            'message': message,
            'detached_count': detached_count,
            'mapping_cleared': True,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/clear', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def clear_all_sensors():
    """Clear all sensor configurations and detach all sensors"""
    try:
        detached_count = 0
        active_tenant = _active_tenant_id()

        # First detach all sensors from base stations
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, 'detach_all_sensors_sync'):
            detached_count = tls_server.detach_all_sensors_sync()

        # Get count before clear for telemetry record
        existing_sensors = _load_all_sensors()
        sensors_to_remove = _filter_sensors_for_tenant(existing_sensors, tenant_id=active_tenant)
        kept_sensors = [
            s for s in existing_sensors
            if not _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
        ]
        _save_all_sensors(kept_sensors)

        # Remove sensor placements from coverage positions (keep base stations)
        state = _load_coverage_positions_state()
        positions = state.get("positions", {})
        if isinstance(positions, dict):
            target_euis = {
                str((sensor or {}).get("eui", "")).strip().upper()
                for sensor in sensors_to_remove
            }
            keys_to_remove = [
                key for key in positions.keys()
                if str(key).startswith("sensor_") and str(key).split("_", 1)[-1].strip().upper() in target_euis
            ]
            for key in keys_to_remove:
                positions.pop(key, None)
            if keys_to_remove:
                _save_coverage_positions_state(state)

        # Also clear from TLS server if available
        if tls_server and hasattr(tls_server, 'clear_all_sensors'):
            tls_server.clear_all_sensors()

        _try_record_inventory_event(
            "sensor",
            "cleared",
            "all",
            {
                "count": len(sensors_to_remove),
                "detached_count": detached_count,
                "tenant_id": active_tenant,
            }
        )

        message = f'Tenant sensors cleared successfully ({len(sensors_to_remove)} removed). Detached {detached_count} sensors from base stations.'
        _record_admin_audit(
            action='sensor.clear_all',
            entity='sensor',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'removed_count': len(sensors_to_remove),
                'detached_count': detached_count,
            },
        )
        return jsonify({'success': True, 'message': message})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/reload', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def reload_sensors():
    """Force reload sensor configuration in TLS server"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server:
            tls_server.reload_sensor_config()
            _record_admin_audit(
                action='sensor.reload_config',
                entity='sensor',
                target_id='tenant',
                status='success',
                details={'tenant_id': _active_tenant_id()},
            )
            return jsonify({'success': True, 'message': 'Sensor configuration reloaded successfully'})
        else:
            return jsonify({'success': False, 'message': 'TLS server not available'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/export', methods=['GET'])
@login_required
def export_sensors():
    """Export all sensors as CSV file"""
    try:
        # Load sensors from config file
        active_tenant = _active_tenant_id()
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
        
        if not sensors:
            return jsonify({'success': False, 'message': 'No sensors to export'}), 404
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['eui', 'nwKey', 'shortAddr', 'bidi', 'name', 'tags', 'gps_lat', 'gps_lng', 'tenant_id'])
        
        # Write sensor data
        for sensor in sensors:
            writer.writerow([
                sensor.get('eui', ''),
                sensor.get('nwKey', ''),
                sensor.get('shortAddr', ''),
                'true' if sensor.get('bidi', False) else 'false',
                sensor.get('name', ''),
                '|'.join(_normalize_sensor_tags(sensor.get('tags', []))),
                sensor.get('gps_lat', ''),
                sensor.get('gps_lng', ''),
                sensor.get('tenant_id', active_tenant),
            ])
        
        # Create response with CSV file
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=sensors_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/sensors/import', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def import_sensors():
    """Import sensors from CSV/TXT file"""
    try:
        active_tenant = _active_tenant_id()
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'}), 400
        
        # Read file content
        content = file.read().decode('utf-8')
        lines = content.strip().split('\n')
        
        if len(lines) < 1:
            return jsonify({'success': False, 'message': 'File is empty'}), 400
        
        # Detect delimiter (comma, semicolon, or tab)
        first_line = lines[0]
        if ';' in first_line:
            delimiter = ';'
        elif '\t' in first_line:
            delimiter = '\t'
        else:
            delimiter = ','
        
        # Parse CSV
        reader = csv.reader(io.StringIO(content), delimiter=delimiter)
        rows = list(reader)
        
        if len(rows) < 1:
            return jsonify({'success': False, 'message': 'No data in file'}), 400
        
        # Check if first row is header
        header = rows[0]
        has_header = any(h.lower() in ['eui', 'nwkey', 'shortaddr', 'bidi', 'network_key', 'short_addr', 'name', 'tags', 'label', 'gps_lat', 'gps_lng', 'latitude', 'longitude', 'lat', 'lng', 'tenant', 'tenant_id'] for h in header)
        
        if has_header:
            # Map header columns
            header_lower = [h.lower().strip() for h in header]
            eui_idx = next((i for i, h in enumerate(header_lower) if h in ['eui', 'mac', 'address']), 0)
            nwkey_idx = next((i for i, h in enumerate(header_lower) if h in ['nwkey', 'network_key', 'key', 'networkkey']), 1)
            shortaddr_idx = next((i for i, h in enumerate(header_lower) if h in ['shortaddr', 'short_addr', 'shortaddress', 'addr']), 2)
            bidi_idx = next((i for i, h in enumerate(header_lower) if h in ['bidi', 'bidirectional', 'bidir']), 3)
            name_idx = next((i for i, h in enumerate(header_lower) if h in ['name', 'label', 'title']), None)
            tags_idx = next((i for i, h in enumerate(header_lower) if h in ['tags', 'tag', 'labels']), None)
            lat_idx = next((i for i, h in enumerate(header_lower) if h in ['gps_lat', 'latitude', 'lat']), None)
            lng_idx = next((i for i, h in enumerate(header_lower) if h in ['gps_lng', 'longitude', 'lng', 'lon']), None)
            tenant_idx = next((i for i, h in enumerate(header_lower) if h in ['tenant', 'tenant_id']), None)
            data_rows = rows[1:]
        else:
            # Assume order: eui, nwKey, shortAddr, bidi, name, tags, gps_lat, gps_lng
            eui_idx, nwkey_idx, shortaddr_idx, bidi_idx = 0, 1, 2, 3
            name_idx, tags_idx = 4, 5
            lat_idx, lng_idx = 6, 7
            tenant_idx = None
            data_rows = rows
        
        # Load existing sensors
        existing_sensors = _load_all_sensors()
        existing_by_eui = {
            str(s.get('eui', '')).strip().upper(): s
            for s in existing_sensors
            if isinstance(s, dict) and str(s.get('eui', '')).strip()
        }
        
        imported_count = 0
        updated_count = 0
        errors = []
        
        for row_idx, row in enumerate(data_rows):
            try:
                if len(row) < 3:  # At least eui, nwKey, shortAddr required
                    errors.append(f"Row {row_idx + 1}: Not enough columns")
                    continue
                
                eui = row[eui_idx].strip().upper() if eui_idx < len(row) else ''
                nwkey = row[nwkey_idx].strip() if nwkey_idx < len(row) else ''
                shortaddr = row[shortaddr_idx].strip() if shortaddr_idx < len(row) else '0000'
                bidi_val = row[bidi_idx].strip().lower() if bidi_idx < len(row) else 'false'
                bidi = bidi_val in ['true', '1', 'yes', 'on']
                name = row[name_idx].strip() if (name_idx is not None and name_idx < len(row)) else ''
                tags_raw = row[tags_idx].strip() if (tags_idx is not None and tags_idx < len(row)) else ''
                tags = _normalize_sensor_tags(tags_raw)
                lat_raw = row[lat_idx].strip() if (lat_idx is not None and lat_idx < len(row)) else ''
                lng_raw = row[lng_idx].strip() if (lng_idx is not None and lng_idx < len(row)) else ''
                tenant_raw = row[tenant_idx].strip() if (tenant_idx is not None and tenant_idx < len(row)) else active_tenant
                if str(session.get("role", "")).strip().lower() != "admin":
                    tenant_raw = active_tenant
                tenant_id = _normalize_tenant_id(tenant_raw, fallback=active_tenant)
                gps_lat, gps_lng = _normalize_gps_coordinates(lat_raw, lng_raw)
                
                # Validate EUI
                if not eui or len(eui) < 8:
                    errors.append(f"Row {row_idx + 1}: Invalid EUI '{eui}'")
                    continue
                
                # Validate nwKey
                if not nwkey or len(nwkey) < 16:
                    errors.append(f"Row {row_idx + 1}: Invalid network key")
                    continue
                
                sensor_data = {
                    'eui': eui,
                    'nwKey': nwkey,
                    'shortAddr': shortaddr if shortaddr else '0000',
                    'bidi': bidi,
                    'name': name,
                    'tags': tags,
                    'gps_lat': gps_lat,
                    'gps_lng': gps_lng,
                    'tenant_id': tenant_id,
                }
                
                existing_sensor = existing_by_eui.get(eui)
                if existing_sensor is not None:
                    existing_tenant = _tenant_id_from_sensor(existing_sensor)
                    if not _tenant_matches(existing_tenant, tenant_id):
                        errors.append(
                            f"Row {row_idx + 1}: EUI '{eui}' already belongs to tenant '{existing_tenant}'"
                        )
                        continue
                    # Update existing sensor
                    for s in existing_sensors:
                        if (
                            str(s.get('eui', '')).upper() == eui
                            and _tenant_matches(_tenant_id_from_sensor(s), tenant_id)
                        ):
                            s.update(sensor_data)
                            break
                    updated_count += 1
                else:
                    # Add new sensor
                    existing_sensors.append(sensor_data)
                    existing_by_eui[eui] = sensor_data
                    imported_count += 1
                    
            except Exception as e:
                errors.append(f"Row {row_idx + 1}: {str(e)}")
        
        # Save to file
        _save_all_sensors(existing_sensors)

        # Keep coverage map positions in sync with imported sensor coordinates
        for sensor in existing_sensors:
            eui_value = str(sensor.get("eui", "")).strip().upper()
            if not eui_value:
                continue
            try:
                gps_lat, gps_lng = _normalize_gps_coordinates(sensor.get("gps_lat"), sensor.get("gps_lng"))
            except ValueError:
                gps_lat, gps_lng = None, None
            _upsert_device_gps_position("sensor", eui_value, gps_lat, gps_lng)

        _try_record_inventory_event(
            "sensor",
            "imported",
            "batch",
            {
                "imported_count": imported_count,
                "updated_count": updated_count,
                "error_count": len(errors),
                "total_after": len(existing_sensors),
                "named_count": sum(1 for s in existing_sensors if str(s.get("name", "")).strip()),
                "tagged_count": sum(1 for s in existing_sensors if len(_normalize_sensor_tags(s.get("tags", []))) > 0),
                "filename": file.filename or "unknown",
                "tenant_id": active_tenant,
            }
        )
        
        # Reload TLS server config
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'reload_sensor_config'):
            tls_server_instance.reload_sensor_config()
        
        message = f'Import complete: {imported_count} new sensors, {updated_count} updated'
        if errors:
            message += f', {len(errors)} errors'
        
        return jsonify({
            'success': True,
            'message': message,
            'imported': imported_count,
            'updated': updated_count,
            'errors': errors[:10] if errors else []  # Limit error messages
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/config')
@login_required
@permission_required('can_edit_config')
def config():
    try:
        # Force reload the config module to get latest values
        import importlib
        import sys
        if 'bssci_config' in sys.modules:
            importlib.reload(sys.modules['bssci_config'])
        
        import bssci_config
        
        config_data = {
            'LISTEN_HOST': getattr(bssci_config, 'LISTEN_HOST', '0.0.0.0'),
            'LISTEN_PORT': getattr(bssci_config, 'LISTEN_PORT', 16018),
            'MQTT_BROKER': getattr(bssci_config, 'MQTT_BROKER', 'localhost'),
            'MQTT_PORT': getattr(bssci_config, 'MQTT_PORT', 1883),
            'MQTT_USERNAME': getattr(bssci_config, 'MQTT_USERNAME', ''),
            'MQTT_PASSWORD': getattr(bssci_config, 'MQTT_PASSWORD', ''),
            'BASE_TOPIC': getattr(bssci_config, 'BASE_TOPIC', 'bssci/'),
            'STATUS_INTERVAL': getattr(bssci_config, 'STATUS_INTERVAL', 30),
            'DEDUPLICATION_DELAY': getattr(bssci_config, 'DEDUPLICATION_DELAY', 2.0),
            'AUTO_DETACH_ENABLED': getattr(bssci_config, 'AUTO_DETACH_ENABLED', True),
            'AUTO_DETACH_TIMEOUT': getattr(bssci_config, 'AUTO_DETACH_TIMEOUT', 259200),
            'AUTO_DETACH_WARNING_TIMEOUT': getattr(bssci_config, 'AUTO_DETACH_WARNING_TIMEOUT', 129600),
            'AUTO_DETACH_CHECK_INTERVAL': getattr(bssci_config, 'AUTO_DETACH_CHECK_INTERVAL', 3600),
            'TIMEZONE': getattr(bssci_config, 'TIMEZONE', 'Europe/Berlin'),
            'TELEMETRY_SOURCE': getattr(bssci_config, 'TELEMETRY_SOURCE', 'auto'),
            'INFLUXDB_URL': getattr(bssci_config, 'INFLUXDB_URL', ''),
            'INFLUXDB_ORG': getattr(bssci_config, 'INFLUXDB_ORG', ''),
            'INFLUXDB_BUCKET': getattr(bssci_config, 'INFLUXDB_BUCKET', ''),
            'INFLUXDB_TOKEN': getattr(bssci_config, 'INFLUXDB_TOKEN', ''),
            'INFLUXDB_VERIFY_SSL': getattr(bssci_config, 'INFLUXDB_VERIFY_SSL', True),
            'INFLUX_UPTIME_MEASUREMENT': getattr(bssci_config, 'INFLUX_UPTIME_MEASUREMENT', 'bssci_bs_uptime'),
            'INFLUX_UPTIME_FIELD': getattr(bssci_config, 'INFLUX_UPTIME_FIELD', 'status'),
            'INFLUX_UPTIME_EUI_TAG': getattr(bssci_config, 'INFLUX_UPTIME_EUI_TAG', 'eui'),
            'INFLUX_UPTIME_QUERY': getattr(bssci_config, 'INFLUX_UPTIME_QUERY', ''),
            'INFLUX_INVENTORY_WRITE_ENABLED': getattr(bssci_config, 'INFLUX_INVENTORY_WRITE_ENABLED', True),
            'INFLUX_INVENTORY_MEASUREMENT': getattr(bssci_config, 'INFLUX_INVENTORY_MEASUREMENT', 'bssci_inventory_events'),
            'INFLUX_SNAPSHOT_ENABLED': getattr(bssci_config, 'INFLUX_SNAPSHOT_ENABLED', True),
            'INFLUX_SNAPSHOT_INTERVAL_SECONDS': getattr(bssci_config, 'INFLUX_SNAPSHOT_INTERVAL_SECONDS', 60),
            'INFLUX_SNAPSHOT_MEASUREMENT': getattr(bssci_config, 'INFLUX_SNAPSHOT_MEASUREMENT', 'bssci_inventory_snapshot'),
            'TIMESCALE_ENABLED': getattr(bssci_config, 'TIMESCALE_ENABLED', False),
            'TIMESCALE_HOST': getattr(bssci_config, 'TIMESCALE_HOST', 'timescaledb'),
            'TIMESCALE_PORT': getattr(bssci_config, 'TIMESCALE_PORT', 5432),
            'TIMESCALE_DB': getattr(bssci_config, 'TIMESCALE_DB', 'bssci'),
            'TIMESCALE_USER': getattr(bssci_config, 'TIMESCALE_USER', 'bssci_user'),
            'TIMESCALE_PASSWORD': getattr(bssci_config, 'TIMESCALE_PASSWORD', ''),
            'TIMESCALE_SSLMODE': getattr(bssci_config, 'TIMESCALE_SSLMODE', 'disable'),
            'TIMESCALE_DEFAULT_TENANT': getattr(bssci_config, 'TIMESCALE_DEFAULT_TENANT', 'default'),
            'TIMESCALE_INVENTORY_WRITE_ENABLED': getattr(bssci_config, 'TIMESCALE_INVENTORY_WRITE_ENABLED', True),
            'TIMESCALE_TELEMETRY_WRITE_ENABLED': getattr(bssci_config, 'TIMESCALE_TELEMETRY_WRITE_ENABLED', True),
            'TIMESCALE_SNAPSHOT_ENABLED': getattr(bssci_config, 'TIMESCALE_SNAPSHOT_ENABLED', True),
            'TIMESCALE_SNAPSHOT_INTERVAL_SECONDS': getattr(bssci_config, 'TIMESCALE_SNAPSHOT_INTERVAL_SECONDS', 60),
            'TIMESCALE_RETENTION_ENABLED': getattr(bssci_config, 'TIMESCALE_RETENTION_ENABLED', True),
            'TIMESCALE_TELEMETRY_RETENTION_DAYS': getattr(bssci_config, 'TIMESCALE_TELEMETRY_RETENTION_DAYS', 90),
            'TIMESCALE_INVENTORY_RETENTION_DAYS': getattr(bssci_config, 'TIMESCALE_INVENTORY_RETENTION_DAYS', 365),
            'TIMESCALE_COMPRESSION_ENABLED': getattr(bssci_config, 'TIMESCALE_COMPRESSION_ENABLED', True),
            'TIMESCALE_COMPRESSION_AFTER_DAYS': getattr(bssci_config, 'TIMESCALE_COMPRESSION_AFTER_DAYS', 7),
            'GRAFANA_URL': getattr(bssci_config, 'GRAFANA_URL', 'http://localhost:3000'),
            'GRAFANA_INTERNAL_URL': getattr(bssci_config, 'GRAFANA_INTERNAL_URL', ''),
            'GRAFANA_DASHBOARD_UID': getattr(bssci_config, 'GRAFANA_DASHBOARD_UID', 'service-center-overview'),
            'GRAFANA_DASHBOARD_SLUG': getattr(bssci_config, 'GRAFANA_DASHBOARD_SLUG', 'service-center-overview'),
            'GRAFANA_ORG_ID': getattr(bssci_config, 'GRAFANA_ORG_ID', 1),
            'GRAFANA_EMBED_ENABLED': getattr(bssci_config, 'GRAFANA_EMBED_ENABLED', True),
            'GRAFANA_ANONYMOUS_ENABLED': getattr(bssci_config, 'GRAFANA_ANONYMOUS_ENABLED', True),
            'GRAFANA_ANONYMOUS_ORG_ROLE': getattr(bssci_config, 'GRAFANA_ANONYMOUS_ORG_ROLE', 'Viewer'),
            'GRAFANA_PROXY_ENABLED': getattr(bssci_config, 'GRAFANA_PROXY_ENABLED', True),
            'GRAFANA_PROXY_TIMEOUT_SECONDS': getattr(bssci_config, 'GRAFANA_PROXY_TIMEOUT_SECONDS', 20),
            'GRAFANA_HEALTH_PANEL_MAP': getattr(
                bssci_config,
                'GRAFANA_HEALTH_PANEL_MAP',
                'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6'
            ),
            'MONITOR_QUEUE_WARN_PCT': getattr(bssci_config, 'MONITOR_QUEUE_WARN_PCT', 65.0),
            'MONITOR_QUEUE_CRIT_PCT': getattr(bssci_config, 'MONITOR_QUEUE_CRIT_PCT', 85.0),
            'MONITOR_RETRY_FAIL_WARN_PCT': getattr(bssci_config, 'MONITOR_RETRY_FAIL_WARN_PCT', 5.0),
            'MONITOR_RETRY_FAIL_CRIT_PCT': getattr(bssci_config, 'MONITOR_RETRY_FAIL_CRIT_PCT', 20.0),
            'MONITOR_DB_WRITE_LATENCY_WARN_MS': getattr(bssci_config, 'MONITOR_DB_WRITE_LATENCY_WARN_MS', 500.0),
            'MONITOR_DB_WRITE_LATENCY_CRIT_MS': getattr(bssci_config, 'MONITOR_DB_WRITE_LATENCY_CRIT_MS', 1500.0),
            'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR': getattr(bssci_config, 'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR', 3.0),
            'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR': getattr(bssci_config, 'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR', 8.0),
            'OMS_ENABLED': getattr(bssci_config, 'OMS_ENABLED', True),
        }
        return render_template('config.html', config=config_data)
    except Exception as e:
        print(f"Error loading config page: {e}")
        # Return default config if there's an error
        default_config = {
            'LISTEN_HOST': '0.0.0.0',
            'LISTEN_PORT': 16018,
            'MQTT_BROKER': 'localhost',
            'MQTT_PORT': 1883,
            'MQTT_USERNAME': '',
            'MQTT_PASSWORD': '',
            'BASE_TOPIC': 'bssci/',
            'STATUS_INTERVAL': 30,
            'DEDUPLICATION_DELAY': 2.0,
            'AUTO_DETACH_ENABLED': True,
            'AUTO_DETACH_TIMEOUT': 259200,
            'AUTO_DETACH_WARNING_TIMEOUT': 129600,
            'AUTO_DETACH_CHECK_INTERVAL': 3600,
            'TIMEZONE': 'Europe/Berlin',
            'TELEMETRY_SOURCE': 'auto',
            'INFLUXDB_URL': '',
            'INFLUXDB_ORG': '',
            'INFLUXDB_BUCKET': '',
            'INFLUXDB_TOKEN': '',
            'INFLUXDB_VERIFY_SSL': True,
            'INFLUX_UPTIME_MEASUREMENT': 'bssci_bs_uptime',
            'INFLUX_UPTIME_FIELD': 'status',
            'INFLUX_UPTIME_EUI_TAG': 'eui',
            'INFLUX_UPTIME_QUERY': '',
            'INFLUX_INVENTORY_WRITE_ENABLED': True,
            'INFLUX_INVENTORY_MEASUREMENT': 'bssci_inventory_events',
            'INFLUX_SNAPSHOT_ENABLED': True,
            'INFLUX_SNAPSHOT_INTERVAL_SECONDS': 60,
            'INFLUX_SNAPSHOT_MEASUREMENT': 'bssci_inventory_snapshot',
            'TIMESCALE_ENABLED': False,
            'TIMESCALE_HOST': 'timescaledb',
            'TIMESCALE_PORT': 5432,
            'TIMESCALE_DB': 'bssci',
            'TIMESCALE_USER': 'bssci_user',
            'TIMESCALE_PASSWORD': '',
            'TIMESCALE_SSLMODE': 'disable',
            'TIMESCALE_DEFAULT_TENANT': 'default',
            'TIMESCALE_INVENTORY_WRITE_ENABLED': True,
            'TIMESCALE_TELEMETRY_WRITE_ENABLED': True,
            'TIMESCALE_SNAPSHOT_ENABLED': True,
            'TIMESCALE_SNAPSHOT_INTERVAL_SECONDS': 60,
            'TIMESCALE_RETENTION_ENABLED': True,
            'TIMESCALE_TELEMETRY_RETENTION_DAYS': 90,
            'TIMESCALE_INVENTORY_RETENTION_DAYS': 365,
            'TIMESCALE_COMPRESSION_ENABLED': True,
            'TIMESCALE_COMPRESSION_AFTER_DAYS': 7,
            'GRAFANA_URL': 'http://localhost:3000',
            'GRAFANA_INTERNAL_URL': '',
            'GRAFANA_DASHBOARD_UID': 'service-center-overview',
            'GRAFANA_DASHBOARD_SLUG': 'service-center-overview',
            'GRAFANA_ORG_ID': 1,
            'GRAFANA_EMBED_ENABLED': True,
            'GRAFANA_ANONYMOUS_ENABLED': True,
            'GRAFANA_ANONYMOUS_ORG_ROLE': 'Viewer',
            'GRAFANA_PROXY_ENABLED': True,
            'GRAFANA_PROXY_TIMEOUT_SECONDS': 20,
            'GRAFANA_HEALTH_PANEL_MAP': 'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6',
            'MONITOR_QUEUE_WARN_PCT': 65.0,
            'MONITOR_QUEUE_CRIT_PCT': 85.0,
            'MONITOR_RETRY_FAIL_WARN_PCT': 5.0,
            'MONITOR_RETRY_FAIL_CRIT_PCT': 20.0,
            'MONITOR_DB_WRITE_LATENCY_WARN_MS': 500.0,
            'MONITOR_DB_WRITE_LATENCY_CRIT_MS': 1500.0,
            'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR': 3.0,
            'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR': 8.0,
            'OMS_ENABLED': True,
        }
        return render_template('config.html', config=default_config)

@app.route('/administration')
@login_required
@admin_scope_required('manage_users', 'manage_tenants', any_scope=True)
def administration():
    return render_template('administration.html')

@app.route('/api/config', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def update_config():
    try:
        data = request.json
        
        # Type safety: Validate request data
        if data is None:
            return jsonify({'success': False, 'message': 'No JSON data provided'}), 400
        
        def _to_bool(value, default=False):
            if isinstance(value, bool):
                return value
            if value is None:
                return default
            return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}

        # Values are already in seconds from HTML form (no conversion needed)
        auto_detach_timeout = int(data.get('AUTO_DETACH_TIMEOUT', 259200))
        auto_detach_warning_timeout = int(data.get('AUTO_DETACH_WARNING_TIMEOUT', 129600))
        auto_detach_check_interval = int(data.get('AUTO_DETACH_CHECK_INTERVAL', 3600))
        influx_snapshot_interval = max(15, int(data.get('INFLUX_SNAPSHOT_INTERVAL_SECONDS', 60)))
        timescale_snapshot_interval = max(15, int(data.get('TIMESCALE_SNAPSHOT_INTERVAL_SECONDS', 60)))
        timescale_telemetry_retention_days = max(1, int(data.get('TIMESCALE_TELEMETRY_RETENTION_DAYS', 90)))
        timescale_inventory_retention_days = max(1, int(data.get('TIMESCALE_INVENTORY_RETENTION_DAYS', 365)))
        timescale_compression_after_days = max(1, int(data.get('TIMESCALE_COMPRESSION_AFTER_DAYS', 7)))
        influx_uptime_query = str(data.get('INFLUX_UPTIME_QUERY', '')).replace('\r', ' ').replace('\n', ' ').strip()
        telemetry_source = str(data.get('TELEMETRY_SOURCE', 'auto')).strip().lower()
        if telemetry_source not in {'auto', 'runtime', 'influx'}:
            telemetry_source = 'auto'
        grafana_org_id = max(1, int(data.get('GRAFANA_ORG_ID', 1)))
        timescale_port = int(data.get('TIMESCALE_PORT', 5432))
        timescale_sslmode = str(data.get('TIMESCALE_SSLMODE', 'disable')).strip().lower()
        if timescale_sslmode not in {'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'}:
            timescale_sslmode = 'disable'

        # Preserve selected sensitive/legacy values if present
        existing_env = {}
        try:
            if os.path.exists('.env'):
                with open('.env', 'r') as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        if not line or line.startswith('#') or '=' not in line:
                            continue
                        key, value = line.split('=', 1)
                        existing_env[key.strip()] = value.strip()
        except Exception:
            existing_env = {}

        secret_key = existing_env.get('SECRET_KEY', os.getenv('SECRET_KEY', 'your-secret-key-here'))
        tls_client_cert_mode = existing_env.get('TLS_CLIENT_CERT_MODE', os.getenv('TLS_CLIENT_CERT_MODE', 'required'))
        cert_file = existing_env.get('CERT_FILE', 'certs/service_center_cert.pem')
        key_file = existing_env.get('KEY_FILE', 'certs/service_center_key.pem')
        ca_file = existing_env.get('CA_FILE', 'certs/ca_cert.pem')
        grafana_url = str(data.get('GRAFANA_URL', existing_env.get('GRAFANA_URL', 'http://localhost:3000'))).strip() or 'http://localhost:3000'
        grafana_internal_url = str(
            data.get('GRAFANA_INTERNAL_URL', existing_env.get('GRAFANA_INTERNAL_URL', 'http://grafana:3000'))
        ).strip() or 'http://grafana:3000'
        grafana_dashboard_uid = str(
            data.get('GRAFANA_DASHBOARD_UID', existing_env.get('GRAFANA_DASHBOARD_UID', 'service-center-overview'))
        ).strip() or 'service-center-overview'
        grafana_dashboard_slug = str(
            data.get('GRAFANA_DASHBOARD_SLUG', existing_env.get('GRAFANA_DASHBOARD_SLUG', 'service-center-overview'))
        ).strip() or 'service-center-overview'
        grafana_embed_enabled = _to_bool(
            data.get('GRAFANA_EMBED_ENABLED', existing_env.get('GRAFANA_EMBED_ENABLED', 'true')),
            True,
        )
        grafana_anonymous_enabled = _to_bool(
            data.get('GRAFANA_ANONYMOUS_ENABLED', existing_env.get('GRAFANA_ANONYMOUS_ENABLED', 'true')),
            True,
        )
        grafana_anonymous_role = str(
            data.get('GRAFANA_ANONYMOUS_ORG_ROLE', existing_env.get('GRAFANA_ANONYMOUS_ORG_ROLE', 'Viewer'))
        ).strip() or 'Viewer'
        grafana_proxy_enabled = _to_bool(
            data.get('GRAFANA_PROXY_ENABLED', existing_env.get('GRAFANA_PROXY_ENABLED', 'true')),
            True,
        )
        grafana_proxy_timeout = max(
            2,
            int(data.get('GRAFANA_PROXY_TIMEOUT_SECONDS', existing_env.get('GRAFANA_PROXY_TIMEOUT_SECONDS', 20))),
        )
        grafana_admin_user = str(
            existing_env.get('GRAFANA_ADMIN_USER', os.getenv('GRAFANA_ADMIN_USER', 'admin'))
        ).strip() or 'admin'
        grafana_admin_password = str(
            existing_env.get('GRAFANA_ADMIN_PASSWORD', os.getenv('GRAFANA_ADMIN_PASSWORD', 'admin'))
        )
        grafana_proxy_bearer_token = str(
            existing_env.get('GRAFANA_PROXY_BEARER_TOKEN', os.getenv('GRAFANA_PROXY_BEARER_TOKEN', ''))
        ).strip()
        grafana_proxy_basic_user = str(
            existing_env.get(
                'GRAFANA_PROXY_BASIC_USER',
                os.getenv('GRAFANA_PROXY_BASIC_USER', grafana_admin_user),
            )
        ).strip()
        grafana_proxy_basic_password = str(
            existing_env.get(
                'GRAFANA_PROXY_BASIC_PASSWORD',
                os.getenv('GRAFANA_PROXY_BASIC_PASSWORD', grafana_admin_password),
            )
        )
        grafana_panel_map = str(
            data.get(
                'GRAFANA_HEALTH_PANEL_MAP',
                existing_env.get(
                    'GRAFANA_HEALTH_PANEL_MAP',
                    'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6',
                ),
            )
        ).strip()
        if not grafana_panel_map:
            grafana_panel_map = 'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6'

        def _parse_float(value, default):
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        def _parse_threshold_pair(
            warn_key,
            crit_key,
            default_warn,
            default_crit,
            minimum=0.0,
        ):
            warn = max(float(minimum), _parse_float(data.get(warn_key, default_warn), default_warn))
            crit = max(warn, _parse_float(data.get(crit_key, default_crit), default_crit))
            return round(warn, 2), round(crit, 2)

        monitor_queue_warn_pct, monitor_queue_crit_pct = _parse_threshold_pair(
            'MONITOR_QUEUE_WARN_PCT',
            'MONITOR_QUEUE_CRIT_PCT',
            65.0,
            85.0,
        )
        monitor_retry_warn_pct, monitor_retry_crit_pct = _parse_threshold_pair(
            'MONITOR_RETRY_FAIL_WARN_PCT',
            'MONITOR_RETRY_FAIL_CRIT_PCT',
            5.0,
            20.0,
        )
        monitor_db_latency_warn_ms, monitor_db_latency_crit_ms = _parse_threshold_pair(
            'MONITOR_DB_WRITE_LATENCY_WARN_MS',
            'MONITOR_DB_WRITE_LATENCY_CRIT_MS',
            500.0,
            1500.0,
        )
        monitor_reconnect_warn_per_hour, monitor_reconnect_crit_per_hour = _parse_threshold_pair(
            'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR',
            'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR',
            3.0,
            8.0,
        )

        # Update the .env file - this is the primary configuration source
        env_content = f"""# TLS Server Configuration
LISTEN_HOST={data.get('LISTEN_HOST', '0.0.0.0')}
LISTEN_PORT={data.get('LISTEN_PORT', 16018)}

# SSL/TLS Certificate Configuration
CERT_FILE={cert_file}
KEY_FILE={key_file}
CA_FILE={ca_file}
TLS_CLIENT_CERT_MODE={tls_client_cert_mode}

# MQTT Configuration
MQTT_BROKER={data.get('MQTT_BROKER', 'localhost')}
MQTT_PORT={data.get('MQTT_PORT', 1883)}
MQTT_USERNAME={data.get('MQTT_USERNAME', '')}
MQTT_PASSWORD={data.get('MQTT_PASSWORD', '')}
BASE_TOPIC={data.get('BASE_TOPIC', 'bssci/')}

# Monitoring / Alert thresholds
MONITOR_QUEUE_WARN_PCT={monitor_queue_warn_pct}
MONITOR_QUEUE_CRIT_PCT={monitor_queue_crit_pct}
MONITOR_RETRY_FAIL_WARN_PCT={monitor_retry_warn_pct}
MONITOR_RETRY_FAIL_CRIT_PCT={monitor_retry_crit_pct}
MONITOR_DB_WRITE_LATENCY_WARN_MS={monitor_db_latency_warn_ms}
MONITOR_DB_WRITE_LATENCY_CRIT_MS={monitor_db_latency_crit_ms}
MONITOR_MQTT_RECONNECT_WARN_PER_HOUR={monitor_reconnect_warn_per_hour}
MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR={monitor_reconnect_crit_per_hour}

# Application Configuration
SENSOR_CONFIG_FILE=endpoints.json
STATUS_INTERVAL={data.get('STATUS_INTERVAL', 30)}
DEDUPLICATION_DELAY={data.get('DEDUPLICATION_DELAY', 2.0)}

# Web Interface Configuration
WEB_HOST=0.0.0.0
WEB_PORT=5000
WEB_DEBUG=false

# Auto-detach Configuration
AUTO_DETACH_ENABLED={str(data.get('AUTO_DETACH_ENABLED', True)).lower()}
AUTO_DETACH_TIMEOUT={auto_detach_timeout}
AUTO_DETACH_HOURS={auto_detach_timeout // 3600}
AUTO_DETACH_WARNING_TIMEOUT={auto_detach_warning_timeout}
AUTO_DETACH_WARNING_HOURS={auto_detach_warning_timeout // 3600}
AUTO_DETACH_CHECK_INTERVAL={auto_detach_check_interval}

# Timezone Configuration
TIMEZONE={data.get('TIMEZONE', 'Europe/Berlin')}

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/bssci_service.log

# Optional modules
OMS_ENABLED={str(_to_bool(data.get('OMS_ENABLED', True), True)).lower()}

# Telemetry Source
# auto | runtime | influx
TELEMETRY_SOURCE={telemetry_source}

# InfluxDB (optional - needed if TELEMETRY_SOURCE=influx/auto)
INFLUXDB_URL={data.get('INFLUXDB_URL', '')}
INFLUXDB_ORG={data.get('INFLUXDB_ORG', '')}
INFLUXDB_BUCKET={data.get('INFLUXDB_BUCKET', '')}
INFLUXDB_TOKEN={data.get('INFLUXDB_TOKEN', '')}
INFLUXDB_VERIFY_SSL={str(_to_bool(data.get('INFLUXDB_VERIFY_SSL', True), True)).lower()}
INFLUX_UPTIME_MEASUREMENT={data.get('INFLUX_UPTIME_MEASUREMENT', 'bssci_bs_uptime')}
INFLUX_UPTIME_FIELD={data.get('INFLUX_UPTIME_FIELD', 'status')}
INFLUX_UPTIME_EUI_TAG={data.get('INFLUX_UPTIME_EUI_TAG', 'eui')}
INFLUX_UPTIME_QUERY={influx_uptime_query}
INFLUX_INVENTORY_WRITE_ENABLED={str(_to_bool(data.get('INFLUX_INVENTORY_WRITE_ENABLED', True), True)).lower()}
INFLUX_INVENTORY_MEASUREMENT={data.get('INFLUX_INVENTORY_MEASUREMENT', 'bssci_inventory_events')}
INFLUX_SNAPSHOT_ENABLED={str(_to_bool(data.get('INFLUX_SNAPSHOT_ENABLED', True), True)).lower()}
INFLUX_SNAPSHOT_INTERVAL_SECONDS={influx_snapshot_interval}
INFLUX_SNAPSHOT_MEASUREMENT={data.get('INFLUX_SNAPSHOT_MEASUREMENT', 'bssci_inventory_snapshot')}

# TimescaleDB/PostgreSQL (optional - recommended for multi-tenant operational store)
TIMESCALE_ENABLED={str(_to_bool(data.get('TIMESCALE_ENABLED', False), False)).lower()}
TIMESCALE_HOST={data.get('TIMESCALE_HOST', 'timescaledb')}
TIMESCALE_PORT={timescale_port}
TIMESCALE_DB={data.get('TIMESCALE_DB', 'bssci')}
TIMESCALE_USER={data.get('TIMESCALE_USER', 'bssci_user')}
TIMESCALE_PASSWORD={data.get('TIMESCALE_PASSWORD', '')}
TIMESCALE_SSLMODE={timescale_sslmode}
TIMESCALE_DEFAULT_TENANT={data.get('TIMESCALE_DEFAULT_TENANT', 'default')}
TIMESCALE_INVENTORY_WRITE_ENABLED={str(_to_bool(data.get('TIMESCALE_INVENTORY_WRITE_ENABLED', True), True)).lower()}
TIMESCALE_TELEMETRY_WRITE_ENABLED={str(_to_bool(data.get('TIMESCALE_TELEMETRY_WRITE_ENABLED', True), True)).lower()}
TIMESCALE_SNAPSHOT_ENABLED={str(_to_bool(data.get('TIMESCALE_SNAPSHOT_ENABLED', True), True)).lower()}
TIMESCALE_SNAPSHOT_INTERVAL_SECONDS={timescale_snapshot_interval}
TIMESCALE_RETENTION_ENABLED={str(_to_bool(data.get('TIMESCALE_RETENTION_ENABLED', True), True)).lower()}
TIMESCALE_TELEMETRY_RETENTION_DAYS={timescale_telemetry_retention_days}
TIMESCALE_INVENTORY_RETENTION_DAYS={timescale_inventory_retention_days}
TIMESCALE_COMPRESSION_ENABLED={str(_to_bool(data.get('TIMESCALE_COMPRESSION_ENABLED', True), True)).lower()}
TIMESCALE_COMPRESSION_AFTER_DAYS={timescale_compression_after_days}

# Grafana
GRAFANA_ADMIN_USER={grafana_admin_user}
GRAFANA_ADMIN_PASSWORD={grafana_admin_password}
GRAFANA_URL={grafana_url}
GRAFANA_INTERNAL_URL={grafana_internal_url}
GRAFANA_DASHBOARD_UID={grafana_dashboard_uid}
GRAFANA_DASHBOARD_SLUG={grafana_dashboard_slug}
GRAFANA_ORG_ID={grafana_org_id}
GRAFANA_EMBED_ENABLED={str(grafana_embed_enabled).lower()}
GRAFANA_ANONYMOUS_ENABLED={str(grafana_anonymous_enabled).lower()}
GRAFANA_ANONYMOUS_ORG_ROLE={grafana_anonymous_role}
GRAFANA_PROXY_ENABLED={str(grafana_proxy_enabled).lower()}
GRAFANA_PROXY_TIMEOUT_SECONDS={grafana_proxy_timeout}
GRAFANA_PROXY_BEARER_TOKEN={grafana_proxy_bearer_token}
GRAFANA_PROXY_BASIC_USER={grafana_proxy_basic_user}
GRAFANA_PROXY_BASIC_PASSWORD={grafana_proxy_basic_password}
GRAFANA_HEALTH_PANEL_MAP={grafana_panel_map}

# Security
SECRET_KEY={secret_key}"""
        
        # Write to .env file with error handling for Docker environments
        try:
            with open('.env', 'w') as f:
                f.write(env_content)
        except PermissionError as pe:
            # Try alternative approach for Docker/Synology environments
            try:
                import tempfile
                import shutil
                # Write to temp file first, then move
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
                    tmp.write(env_content)
                    tmp_name = tmp.name
                shutil.move(tmp_name, '.env')
            except Exception as fallback_error:
                raise Exception(f"Cannot write .env file. Docker volume not mounted as writable? Original error: {pe}, Fallback error: {fallback_error}")
        
        # Reload environment variables
        from dotenv import load_dotenv
        load_dotenv(override=True)
            
        # Force reload of the bssci_config module to pick up new .env values
        import importlib
        import sys
        if 'bssci_config' in sys.modules:
            importlib.reload(sys.modules['bssci_config'])
        
        safe_changed_keys = []
        for raw_key in (data.keys() if isinstance(data, dict) else []):
            key = str(raw_key or "").strip()
            key_lower = key.lower()
            if not key:
                continue
            if any(marker in key_lower for marker in _AUDIT_SENSITIVE_KEY_MARKERS):
                continue
            safe_changed_keys.append(key)
        safe_changed_keys = sorted(set(safe_changed_keys))
        _record_admin_audit(
            action='config.update',
            entity='config',
            target_id='.env',
            status='success',
            details={
                'changed_keys': safe_changed_keys,
                'changed_count': len(safe_changed_keys),
                'timescale_enabled': _to_bool(data.get('TIMESCALE_ENABLED', False), False),
                'telemetry_source': telemetry_source,
                'oms_enabled': _to_bool(data.get('OMS_ENABLED', True), True),
            },
        )
        
        return jsonify({'success': True, 'message': 'Configuration updated in .env file and reloaded successfully.'})
    except Exception as e:
        print(f"Error updating config: {e}")
        return jsonify({'success': False, 'message': f'Configuration update failed: {str(e)}'})

@app.route('/certificates')
@login_required
@permission_required('can_manage_certificates')
def certificates():
    return render_template('certificates.html')

@app.route('/logs')
@login_required
def logs():
    return render_template('logs.html')

@app.route('/mqtt')
@login_required
def mqtt():
    return render_template('mqtt.html')

@app.route('/documentation')
@login_required
def documentation():
    return render_template('documentation.html')

@app.route('/traffic')
@login_required
def traffic():
    return redirect(url_for('health'))

@app.route('/oms')
@login_required
def oms():
    if not getattr(bssci_config, 'OMS_ENABLED', True):
        return redirect(url_for('index'))
    return render_template('oms.html')

@app.route('/health')
@login_required
def health():
    return render_template('health.html')

@app.route('/api/health', methods=['GET'])
@login_required
def get_health_stats():
    """Get comprehensive health statistics for the system"""
    try:
        global tls_server_instance
        
        result = {
            "system": {
                "uptime": 0,
                "total_sensors": 0,
                "active_sensors": 0,
                "total_base_stations": 0,
                "connected_base_stations": 0,
                "total_packets_received": 0,
                "total_packets_lost": 0,
                "overall_packet_loss_rate": 0,
                "avg_snr": 0,
                "avg_rssi": 0
            },
            "base_stations": [],
            "sensors": [],
            "snr_rssi_history": []
        }
        timescale_summary = _timescale_fetch_telemetry_summary(window_minutes=24 * 60, bucket_seconds=300, top_limit=10)
        result["timescale"] = timescale_summary
        
        if tls_server_instance:
            # System stats
            start_time = tls_server_instance.traffic_metrics.get('start_time', 0)
            if start_time:
                from datetime import datetime, timezone
                result["system"]["uptime"] = int(datetime.now(timezone.utc).timestamp() - start_time)
            
            result["system"]["total_sensors"] = len(tls_server_instance.sensor_config)
            result["system"]["active_sensors"] = len(tls_server_instance.active_sensors_hourly)
            result["system"]["total_base_stations"] = len(tls_server_instance.connected_base_stations) + len(tls_server_instance.connecting_base_stations)
            result["system"]["connected_base_stations"] = len(tls_server_instance.connected_base_stations)
            
            # Aggregate packet stats
            total_received = 0
            total_lost = 0
            for eui, stats in tls_server_instance.sensor_packet_stats.items():
                total_received += stats.get('packets_received', 0)
                total_lost += stats.get('packets_lost', 0)
            
            result["system"]["total_packets_received"] = total_received
            result["system"]["total_packets_lost"] = total_lost
            if total_received + total_lost > 0:
                result["system"]["overall_packet_loss_rate"] = round(total_lost / (total_received + total_lost) * 100, 2)
            
            # Base station health
            bs_config = load_base_station_config().get("base_stations", {})
            for writer, bs_eui in tls_server_instance.connected_base_stations.items():
                eui_lower = bs_eui.lower()
                health = tls_server_instance.base_station_health.get(eui_lower, {})
                bs_info = bs_config.get(eui_lower, {})
                result["base_stations"].append({
                    "eui": eui_lower,
                    "name": bs_info.get("name", ""),
                    "status": "connected",
                    "cpu": health.get("cpu", 0),
                    "memory": health.get("memory", 0),
                    "duty_cycle": health.get("duty_cycle", 0),
                    "uptime": health.get("uptime", 0)
                })
            
            # Sensor packet stats
            for eui, stats in tls_server_instance.sensor_packet_stats.items():
                received = stats.get('packets_received', 0)
                lost = stats.get('packets_lost', 0)
                snr_avg = stats.get('snr_sum', 0) / max(stats.get('snr_count', 1), 1)
                rssi_avg = stats.get('rssi_sum', 0) / max(stats.get('rssi_count', 1), 1)
                loss_rate = 0
                if received + lost > 0:
                    loss_rate = round(lost / (received + lost) * 100, 2)
                
                result["sensors"].append({
                    "eui": eui.lower(),
                    "packets_received": received,
                    "packets_lost": lost,
                    "packet_loss_rate": loss_rate,
                    "avg_snr": round(snr_avg, 2),
                    "avg_rssi": round(rssi_avg, 2)
                })
            
            # Sort sensors by packet loss rate (worst first)
            result["sensors"].sort(key=lambda x: x["packet_loss_rate"], reverse=True)
            
            # Calculate overall average SNR/RSSI
            total_snr = 0
            total_rssi = 0
            sensor_count = 0
            for stats in tls_server_instance.sensor_packet_stats.values():
                if stats.get('snr_count', 0) > 0:
                    total_snr += stats['snr_sum'] / stats['snr_count']
                    total_rssi += stats['rssi_sum'] / stats['rssi_count']
                    sensor_count += 1
            
            if sensor_count > 0:
                result["system"]["avg_snr"] = round(total_snr / sensor_count, 2)
                result["system"]["avg_rssi"] = round(total_rssi / sensor_count, 2)
            
            # Include SNR/RSSI history
            result["snr_rssi_history"] = tls_server_instance.snr_rssi_history
            
            # Calculate signal score distribution based on SNR
            # Excellent: >= 10 dB, Good: 5-10 dB, Fair: 0-5 dB, Poor: -5-0 dB, Critical: < -5 dB
            distribution = {"excellent": 0, "good": 0, "fair": 0, "poor": 0, "critical": 0}
            for stats in tls_server_instance.sensor_packet_stats.values():
                if stats.get('snr_count', 0) > 0:
                    avg_snr = stats['snr_sum'] / stats['snr_count']
                    if avg_snr >= 10:
                        distribution["excellent"] += 1
                    elif avg_snr >= 5:
                        distribution["good"] += 1
                    elif avg_snr >= 0:
                        distribution["fair"] += 1
                    elif avg_snr >= -5:
                        distribution["poor"] += 1
                    else:
                        distribution["critical"] += 1
            result["signal_distribution"] = distribution

        # Fallback to Timescale-derived sensor insights if runtime packet stats are unavailable
        if not result["sensors"] and timescale_summary.get("success"):
            for sensor in timescale_summary.get("top_sensors", []):
                result["sensors"].append({
                    "eui": str(sensor.get("sensor_eui", "")).lower(),
                    "packets_received": int(sensor.get("uplinks", 0) or 0),
                    "packets_lost": 0,
                    "packet_loss_rate": float(sensor.get("avg_packet_loss_pct", 0.0) or 0.0),
                    "avg_snr": round(float(sensor.get("avg_snr", 0.0) or 0.0), 2),
                    "avg_rssi": round(float(sensor.get("avg_rssi", 0.0) or 0.0), 2),
                })
            result["sensors"].sort(key=lambda x: x["packet_loss_rate"], reverse=True)

        if not result["snr_rssi_history"] and timescale_summary.get("success"):
            result["snr_rssi_history"] = [
                {
                    "timestamp": point.get("timestamp"),
                    "avg_snr": point.get("avg_snr"),
                    "avg_rssi": point.get("avg_rssi"),
                }
                for point in (timescale_summary.get("series") or [])
            ]
        
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/base-stations')
@login_required
def base_stations():
    telemetry_source = (getattr(bssci_config, 'TELEMETRY_SOURCE', 'auto') or 'auto').strip().lower()
    if telemetry_source not in {'auto', 'runtime', 'influx'}:
        telemetry_source = 'auto'
    influx_configured = bool(
        getattr(bssci_config, 'INFLUXDB_URL', '')
        and getattr(bssci_config, 'INFLUXDB_ORG', '')
        and getattr(bssci_config, 'INFLUXDB_BUCKET', '')
        and getattr(bssci_config, 'INFLUXDB_TOKEN', '')
    )
    return render_template(
        'base_stations.html',
        telemetry_source=telemetry_source,
        influx_configured=influx_configured
    )

@app.route('/network')
@login_required
def network():
    return render_template('network.html')

@app.route('/coverage')
@login_required
def coverage():
    return redirect(url_for('network'))

def _normalize_eui_upper(value: Any) -> str:
    return str(value or "").strip().upper()

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _build_sensor_name_index(sensor_config: List[Dict[str, Any]]) -> Dict[str, str]:
    index = {}
    for sensor in sensor_config or []:
        if not isinstance(sensor, dict):
            continue
        sensor_eui = _normalize_eui_upper(sensor.get("eui", ""))
        if not sensor_eui:
            continue
        sensor_name = str(sensor.get("name", "") or "").strip()
        index[sensor_eui] = sensor_name if sensor_name else f"{sensor_eui[:8]}..."
    return index

def _load_configured_sensors_index() -> Dict[str, Dict[str, Any]]:
    """
    Load configured sensors from endpoints.json (or configured sensor file).
    Returns mapping:
      EUI_UPPER -> {"name": str, "tags": list[str], "bidi": bool, "attached_base_stations": list[str]}
    """
    result: Dict[str, Dict[str, Any]] = {}
    try:
        sensor_file = getattr(bssci_config, "SENSOR_CONFIG_FILE", "endpoints.json")
        with open(sensor_file, "r") as f:
            sensors = json.load(f) or []
    except Exception:
        sensors = []

    active_tenant = _active_tenant_id()
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
            continue
        sensor_eui = _normalize_eui_upper(sensor.get("eui", ""))
        if not sensor_eui:
            continue
        sensor_name = str(sensor.get("name", "") or "").strip()
        result[sensor_eui] = {
            "name": sensor_name if sensor_name else f"{sensor_eui[:8]}...",
            "tags": _normalize_sensor_tags(sensor.get("tags", [])),
            "bidi": bool(sensor.get("bidi", False)),
            "attached_base_stations": _normalize_base_station_route_list(
                sensor.get("attached_base_stations", [])
            ),
        }
    return result

def _collect_network_snapshot() -> Dict[str, Any]:
    """
    Build one normalized snapshot used by both:
      - /api/coverage/topology
      - /api/network
    """
    global tls_server_instance

    active_tenant = _active_tenant_id()
    bs_config_raw = load_base_station_config().get("base_stations", {}) or {}
    bs_config = {}
    for eui_key, bs_data in bs_config_raw.items():
        bs_eui = _normalize_eui_upper(eui_key)
        if not bs_eui:
            continue
        if not _tenant_matches(_tenant_id_from_base_station(bs_data), active_tenant):
            continue
        bs_config[bs_eui] = bs_data if isinstance(bs_data, dict) else {}

    connected_bs = set()
    bs_health = {}
    sensor_topology = {}
    sensor_name_index = {}
    configured_sensors = _load_configured_sensors_index()
    configured_sensor_routes: Dict[str, List[str]] = {
        sensor_eui: _normalize_base_station_route_list(meta.get("attached_base_stations", []))
        for sensor_eui, meta in configured_sensors.items()
    }
    runtime_registered_sensors = set()
    registered_sensor_routes: Dict[str, List[str]] = {}

    if tls_server_instance:
        connected_map = getattr(tls_server_instance, "connected_base_stations", {}) or {}
        connected_bs = {
            _normalize_eui_upper(bs_eui)
            for bs_eui in connected_map.values()
            if _normalize_eui_upper(bs_eui)
        }
        bs_health = getattr(tls_server_instance, "base_station_health", {}) or {}
        sensor_topology = getattr(tls_server_instance, "sensor_topology", {}) or {}
        sensor_name_index = _build_sensor_name_index(getattr(tls_server_instance, "sensor_config", []) or [])
        registered_map = getattr(tls_server_instance, "registered_sensors", {}) or {}
        runtime_registered_sensors = {
            _normalize_eui_upper(sensor_eui)
            for sensor_eui in registered_map.keys()
            if _normalize_eui_upper(sensor_eui)
        }
        for sensor_eui_raw, reg_raw in registered_map.items():
            sensor_eui = _normalize_eui_upper(sensor_eui_raw)
            if not sensor_eui:
                continue

            # Keep tenant-safe inventory scope when building relation graph.
            if sensor_eui not in configured_sensors and sensor_eui not in sensor_topology:
                continue

            reg_payload = reg_raw if isinstance(reg_raw, dict) else {}
            bs_candidates = reg_payload.get("base_stations", [])
            if isinstance(bs_candidates, str):
                bs_candidates = [bs_candidates]
            elif not isinstance(bs_candidates, (list, tuple, set)):
                bs_candidates = []

            routes: List[str] = []
            for bs_candidate in bs_candidates:
                bs_eui = _normalize_eui_upper(bs_candidate)
                if not bs_eui:
                    continue
                if bs_eui not in routes:
                    routes.append(bs_eui)
            if routes:
                registered_sensor_routes[sensor_eui] = routes

    # Merge runtime-loaded sensor names over file-loaded names.
    for sensor_eui, sensor_name in sensor_name_index.items():
        configured_sensors.setdefault(sensor_eui, {
            "name": sensor_name,
            "tags": [],
            "bidi": False
        })
        configured_sensors[sensor_eui]["name"] = sensor_name

    all_bs = set(bs_config.keys()) | connected_bs
    coverage_sensors = {}
    sensor_nodes = []
    edges = []
    edge_ids = set()
    edge_pairs = set()

    def _classify_live_link_status(snr_value: float) -> tuple[str, bool]:
        snr = _safe_float(snr_value, -100.0)
        if snr >= 10:
            return "good", False
        if snr >= 0:
            return "fair", False
        if snr >= -5:
            return "poor", True
        return "critical", True

    for sensor_eui_raw, topo_raw in sensor_topology.items():
        sensor_eui = _normalize_eui_upper(sensor_eui_raw)
        if not sensor_eui:
            continue

        topo = topo_raw if isinstance(topo_raw, dict) else {}
        receiving_raw = topo.get("receiving_bases", {})
        receiving = receiving_raw if isinstance(receiving_raw, dict) else {}
        primary_bs = _normalize_eui_upper(topo.get("primary_bs", ""))

        coverage_receiving = {}
        for bs_eui_raw, bs_stats_raw in receiving.items():
            bs_eui = _normalize_eui_upper(bs_eui_raw)
            if not bs_eui:
                continue

            all_bs.add(bs_eui)
            bs_stats = bs_stats_raw if isinstance(bs_stats_raw, dict) else {}
            snr = round(_safe_float(bs_stats.get("snr", 0.0), 0.0), 2)
            rssi = round(_safe_float(bs_stats.get("rssi", -100.0), -100.0), 2)
            count = int(_safe_float(bs_stats.get("count", 0), 0))
            last_seen = _safe_float(bs_stats.get("last_seen", 0), 0.0)

            coverage_receiving[bs_eui] = {
                "snr": snr,
                "rssi": rssi,
                "count": count
            }

            edge_id = f"edge_{sensor_eui}_{bs_eui}"
            if edge_id not in edge_ids:
                link_status, problematic = _classify_live_link_status(snr)
                edge_ids.add(edge_id)
                edge_pairs.add((sensor_eui, bs_eui))
                edges.append({
                    "id": edge_id,
                    "source": f"sensor_{sensor_eui}",
                    "target": f"bs_{bs_eui}",
                    "primary": bs_eui == primary_bs,
                    "snr": snr,
                    "rssi": rssi,
                    "last_seen": last_seen,
                    "count": count,
                    "route_kind": "live",
                    "link_status": link_status,
                    "problematic": problematic
                })

        if coverage_receiving:
            coverage_sensors[sensor_eui] = {
                "base_stations": coverage_receiving
            }

        assigned_bases = []
        for candidate in (registered_sensor_routes.get(sensor_eui, []), configured_sensor_routes.get(sensor_eui, [])):
            for bs_eui in candidate:
                if bs_eui not in assigned_bases:
                    assigned_bases.append(bs_eui)
        if not primary_bs and assigned_bases:
            primary_bs = assigned_bases[0]
        receiver_count = max(len(coverage_receiving), len(assigned_bases))

        sensor_nodes.append({
            "id": f"sensor_{sensor_eui}",
            "type": "sensor",
            "eui": sensor_eui,
            "label": configured_sensors.get(sensor_eui, {}).get("name", sensor_name_index.get(sensor_eui, f"{sensor_eui[:8]}...")),
            "primary_bs": primary_bs,
            "receiver_count": receiver_count,
            "live_receiver_count": len(coverage_receiving),
            "assigned_bases": assigned_bases,
            "configured": sensor_eui in configured_sensors,
            "registered": sensor_eui in runtime_registered_sensors
        })

    # Include configured sensors even when there is currently no live topology.
    existing_sensor_euis = {str(node.get("eui", "")).upper() for node in sensor_nodes}
    for sensor_eui, sensor_meta in configured_sensors.items():
        if sensor_eui in existing_sensor_euis:
            continue
        assigned_bases = []
        for candidate in (registered_sensor_routes.get(sensor_eui, []), configured_sensor_routes.get(sensor_eui, [])):
            for bs_eui in candidate:
                if bs_eui not in assigned_bases:
                    assigned_bases.append(bs_eui)
        primary_bs = assigned_bases[0] if assigned_bases else ""
        sensor_nodes.append({
            "id": f"sensor_{sensor_eui}",
            "type": "sensor",
            "eui": sensor_eui,
            "label": str(sensor_meta.get("name", f"{sensor_eui[:8]}...")),
            "primary_bs": primary_bs,
            "receiver_count": len(assigned_bases),
            "live_receiver_count": 0,
            "assigned_bases": assigned_bases,
            "configured": True,
            "registered": sensor_eui in runtime_registered_sensors
        })

    # Add assignment edges from registration data when live topology edge does not exist.
    for sensor_node in sensor_nodes:
        sensor_eui = _normalize_eui_upper(sensor_node.get("eui", ""))
        if not sensor_eui:
            continue
        primary_bs = _normalize_eui_upper(sensor_node.get("primary_bs", ""))
        assigned_bases = sensor_node.get("assigned_bases", [])
        if not isinstance(assigned_bases, list):
            continue
        registered_bs_set = set(registered_sensor_routes.get(sensor_eui, []))
        configured_bs_set = set(configured_sensor_routes.get(sensor_eui, []))

        for bs_eui in assigned_bases:
            bs_eui_upper = _normalize_eui_upper(bs_eui)
            if not bs_eui_upper:
                continue
            all_bs.add(bs_eui_upper)
            edge_key = (sensor_eui, bs_eui_upper)
            if edge_key in edge_pairs:
                continue

            edge_id = f"edge_registration_{sensor_eui}_{bs_eui_upper}"
            if edge_id in edge_ids:
                continue

            edge_ids.add(edge_id)
            edge_pairs.add(edge_key)
            if bs_eui_upper in registered_bs_set:
                route_kind = "registration"
            elif bs_eui_upper in configured_bs_set:
                route_kind = "configured"
            else:
                route_kind = "configured"
            is_bs_connected = bs_eui_upper in connected_bs
            link_status = "assigned" if is_bs_connected else "assigned_offline"
            edges.append({
                "id": edge_id,
                "source": f"sensor_{sensor_eui}",
                "target": f"bs_{bs_eui_upper}",
                "primary": bs_eui_upper == primary_bs,
                "snr": None,
                "rssi": None,
                "last_seen": 0,
                "count": 0,
                "route_kind": route_kind,
                "link_status": link_status,
                "problematic": not is_bs_connected
            })

    base_station_nodes = []
    for bs_eui in sorted(all_bs):
        bs_cfg = bs_config.get(bs_eui, {})
        health = bs_health.get(bs_eui.lower(), {}) or bs_health.get(bs_eui, {}) or {}
        bs_name = str(bs_cfg.get("name", "") or "").strip()

        base_station_nodes.append({
            "id": f"bs_{bs_eui}",
            "type": "base_station",
            "eui": bs_eui,
            "label": bs_name if bs_name else f"{bs_eui[:8]}...",
            "connected": bs_eui in connected_bs,
            "cpu": round(_safe_float(health.get("cpu", 0.0), 0.0), 1),
            "memory": round(_safe_float(health.get("memory", 0.0), 0.0), 1),
            "duty_cycle": round(_safe_float(health.get("duty_cycle", 0.0), 0.0), 1)
        })

    sensor_nodes.sort(key=lambda item: (item.get("label", ""), item.get("eui", "")))
    edges.sort(key=lambda item: item.get("id", ""))

    return {
        "coverage": {
            "sensors": coverage_sensors,
            "base_stations": sorted(all_bs)
        },
        "topology": {
            "nodes": base_station_nodes + sensor_nodes,
            "edges": edges
        },
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "base_station_count": len(base_station_nodes),
            "connected_base_station_count": len(connected_bs),
            "sensor_count": len(sensor_nodes),
            "edge_count": len(edges)
        }
    }

@app.route('/api/coverage/topology')
@login_required
def api_coverage_topology():
    """Get sensor topology with SNR/RSSI per base station for coverage heatmap"""
    try:
        snapshot = _collect_network_snapshot()
        return jsonify({
            **snapshot["coverage"],
            "meta": snapshot["meta"]
        })
    except Exception as e:
        logger.exception("Failed to build coverage topology snapshot")
        return jsonify({'sensors': {}, 'base_stations': [], 'error': str(e)})

@app.route('/api/coverage/positions', methods=['GET', 'POST'])
@login_required
def api_coverage_positions():
    """Get or save coverage map device positions"""
    positions_file = _coverage_positions_file()
    
    if request.method == 'POST':
        perms = get_user_permissions()
        if not perms.get('can_edit_sensors', False):
            return jsonify({'success': False, 'error': 'Insufficient permissions'}), 403
        try:
            incoming = request.get_json() or {}
            if not isinstance(incoming, dict):
                return jsonify({'success': False, 'error': 'Invalid payload'}), 400

            active_tenant = _active_tenant_id()
            tenant_sensor_keys = {
                f"sensor_{str(sensor.get('eui', '')).strip().upper()}"
                for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
            }
            tenant_bs_keys = {
                f"bs_{str(eui).strip().upper()}"
                for eui in _filter_base_stations_for_tenant(
                    load_base_station_config().get("base_stations", {}),
                    tenant_id=active_tenant,
                ).keys()
            }
            editable_keys = tenant_sensor_keys | tenant_bs_keys

            state = _load_coverage_positions_state()
            if not isinstance(state, dict):
                state = {"positions": {}}
            current_positions = state.setdefault("positions", {})
            if not isinstance(current_positions, dict):
                current_positions = {}
                state["positions"] = current_positions

            incoming_positions = incoming.get("positions", incoming)
            incoming_positions = incoming_positions if isinstance(incoming_positions, dict) else {}

            for key in list(current_positions.keys()):
                if key in editable_keys:
                    current_positions.pop(key, None)

            normalized_incoming_positions = {}
            for key, value in incoming_positions.items():
                canonical_key, normalized_payload = _normalize_coverage_position_record(key, value)
                if not canonical_key or canonical_key not in editable_keys:
                    continue
                existing = normalized_incoming_positions.get(canonical_key)
                normalized_incoming_positions[canonical_key] = _merge_coverage_position_payload(existing, normalized_payload)

            current_positions.update(normalized_incoming_positions)

            for key, value in incoming.items():
                if key == "positions":
                    continue
                state[key] = value

            # Keep inventory GPS consistent with saved OSM positions from the map.
            sync_summary = _sync_coverage_positions_to_inventory(
                tenant_id=active_tenant,
                only_missing=False,
                state=state,
                allowed_keys=editable_keys,
            )

            with open(positions_file, 'w') as f:
                json.dump(state, f, indent=2)
            return jsonify({'success': True, 'synced_to_inventory': sync_summary})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        try:
            active_tenant = _active_tenant_id()
            tenant_sensor_keys = {
                f"sensor_{str(sensor.get('eui', '')).strip().upper()}"
                for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
            }
            tenant_bs_keys = {
                f"bs_{str(eui).strip().upper()}"
                for eui in _filter_base_stations_for_tenant(
                    load_base_station_config().get("base_stations", {}),
                    tenant_id=active_tenant,
                ).keys()
            }
            allowed_keys = tenant_sensor_keys | tenant_bs_keys

            # Self-heal older inconsistent data: map had GPS but inventory was empty.
            _sync_coverage_positions_to_inventory(
                tenant_id=active_tenant,
                only_missing=True,
                state=_load_coverage_positions_state(),
                allowed_keys=allowed_keys,
            )

            state = _sync_inventory_gps_to_coverage_positions()
            if state:
                filtered_state = dict(state)
                positions = state.get("positions", {})
                if isinstance(positions, dict):
                    filtered_state["positions"] = {
                        key: value for key, value in positions.items()
                        if key in allowed_keys
                    }
                return jsonify(filtered_state)
            if os.path.exists(positions_file):
                with open(positions_file, 'r') as f:
                    raw_state = json.load(f)
                if isinstance(raw_state, dict) and isinstance(raw_state.get("positions"), dict):
                    filtered_state = dict(raw_state)
                    filtered_state["positions"] = {
                        key: value for key, value in raw_state.get("positions", {}).items()
                        if key in allowed_keys
                    }
                    return jsonify(filtered_state)
                return jsonify(raw_state)
            return jsonify({})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

@app.route('/api/coverage/device-gps', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def api_coverage_device_gps():
    """Update device GPS from coverage map and sync into device settings."""
    try:
        payload = request.get_json() or {}
        raw_type = str(payload.get("device_type", "") or "").strip().lower()
        device_type = "sensor" if raw_type == "sensor" else ("bs" if raw_type in {"bs", "base_station"} else "")
        if not device_type:
            return jsonify({"success": False, "error": "Invalid device_type"}), 400

        eui = _normalize_eui_upper(payload.get("eui", ""))
        if not eui:
            return jsonify({"success": False, "error": "EUI is required"}), 400

        gps_lat, gps_lng = _normalize_gps_coordinates(payload.get("gps_lat"), payload.get("gps_lng"))
        if gps_lat is None or gps_lng is None:
            return jsonify({"success": False, "error": "Both latitude and longitude are required"}), 400

        if device_type == "sensor":
            _update_sensor_gps_by_eui(eui, gps_lat, gps_lng)
            _try_record_inventory_event(
                "sensor",
                "updated",
                eui,
                {"gps_lat": gps_lat, "gps_lng": gps_lng, "source": "coverage_map"}
            )
        else:
            _update_base_station_gps_by_eui(eui, gps_lat, gps_lng)
            _try_record_inventory_event(
                "base_station",
                "updated",
                eui.lower(),
                {"gps_lat": gps_lat, "gps_lng": gps_lng, "source": "coverage_map"}
            )

        _upsert_device_gps_position(device_type, eui, gps_lat, gps_lng)
        return jsonify({
            "success": True,
            "device_type": device_type,
            "eui": eui,
            "gps_lat": gps_lat,
            "gps_lng": gps_lng
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Failed to update GPS from coverage map")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/coverage/floorplan', methods=['GET', 'POST'])
@login_required
def api_coverage_floorplan():
    """Get or save floorplan image (base64 encoded)"""
    floorplan_file = 'coverage_floorplan.txt'
    
    if request.method == 'POST':
        perms = get_user_permissions()
        if not perms.get('can_edit_sensors', False):
            return jsonify({'success': False, 'error': 'Insufficient permissions'}), 403
        try:
            data = request.get_json()
            image_data = data.get('image', '')
            with open(floorplan_file, 'w') as f:
                f.write(image_data)
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        try:
            if os.path.exists(floorplan_file):
                with open(floorplan_file, 'r') as f:
                    return jsonify({'image': f.read()})
            return jsonify({'image': None})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

@app.route('/api/network')
@login_required
def api_network():
    """Get network topology data for visualization"""
    try:
        snapshot = _collect_network_snapshot()
        return jsonify({
            'success': True,
            'nodes': snapshot["topology"]["nodes"],
            'edges': snapshot["topology"]["edges"],
            'meta': snapshot["meta"]
        })
    except Exception as e:
        logger.exception("Failed to build network topology snapshot")
        return jsonify({'success': False, 'error': str(e)}), 500

def load_base_station_config():
    """Load base station configuration from JSON file"""
    try:
        config_path = getattr(bssci_config, 'BASE_STATION_CONFIG_FILE', 'base_stations.json')
        with open(config_path, 'r') as f:
            return json.load(f)
    except:
        return {"base_stations": {}}

def save_base_station_config(config):
    """Save base station configuration to JSON file"""
    import os
    config_path = getattr(bssci_config, 'BASE_STATION_CONFIG_FILE', 'base_stations.json')
    try:
        dir_path = os.path.dirname(config_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
    except:
        pass
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

@app.route('/api/base-stations', methods=['GET'])
@login_required
def get_base_stations():
    """Get all base stations with status and health data"""
    try:
        global tls_server_instance
        config = load_base_station_config()
        active_tenant = _active_tenant_id()
        bs_config = _filter_base_stations_for_tenant(config.get("base_stations", {}), tenant_id=active_tenant)
        
        connected_bs = {}
        connecting_bs = {}
        bs_health = {}
        bs_sensors = {}
        
        if tls_server_instance:
            status = tls_server_instance.get_base_station_status()
            for bs in status.get("connected", []):
                eui = bs["eui"].lower()
                connected_bs[eui] = bs
            for bs in status.get("connecting", []):
                eui = bs["eui"].lower()
                connecting_bs[eui] = bs
            if hasattr(tls_server_instance, 'base_station_health'):
                bs_health = tls_server_instance.base_station_health
            if hasattr(tls_server_instance, 'sensor_config'):
                for sensor in tls_server_instance.sensor_config:
                    if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                        continue
                    preferred = sensor.get('preferredDownlinkPath', {})
                    if isinstance(preferred, dict):
                        bs_eui = preferred.get('baseStation', '').lower()
                        if bs_eui:
                            bs_sensors[bs_eui] = bs_sensors.get(bs_eui, 0) + 1
        
        normalized_bs_config = {}
        for raw_eui, raw_bs_data in (bs_config or {}).items():
            eui_lower = str(raw_eui or "").strip().lower()
            if not eui_lower:
                continue
            payload = dict(raw_bs_data) if isinstance(raw_bs_data, dict) else {}
            existing = normalized_bs_config.get(eui_lower)
            if existing is None:
                normalized_bs_config[eui_lower] = payload
            else:
                merged = dict(existing)
                merged.update({k: v for k, v in payload.items() if v not in (None, "", [], {})})
                normalized_bs_config[eui_lower] = merged

        result = []
        for eui_lower, bs_data in normalized_bs_config.items():
            
            if eui_lower in connected_bs:
                status = "connected"
            elif eui_lower in connecting_bs:
                status = "connecting"
            else:
                status = "offline"
            
            health = bs_health.get(eui_lower, {})
            last_status_change = ""
            last_status_event = ""
            status_age_seconds = None
            bs_events = bs_uptime_events.get(eui_lower, [])
            if bs_events:
                last = bs_events[-1]
                last_status_change = last.get("timestamp", "")
                last_status_event = last.get("event", "")
                try:
                    ts = datetime.fromisoformat(last_status_change)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    status_age_seconds = max(0, int((datetime.now(timezone.utc) - ts).total_seconds()))
                except:
                    status_age_seconds = None
            
            result.append({
                "eui": eui_lower,
                "name": bs_data.get("name", ""),
                "tags": bs_data.get("tags", []),
                "tenant_id": bs_data.get("tenant_id", active_tenant),
                "status": status,
                "configured_ip": bs_data.get("ip", ""),
                "gps_lat": bs_data.get("gps_lat"),
                "gps_lng": bs_data.get("gps_lng"),
                "health": health,
                "connected_sensors": bs_sensors.get(eui_lower, 0),
                "last_status_event": last_status_event,
                "last_status_change": last_status_change,
                "status_age_seconds": status_age_seconds
            })
        
        result.sort(key=lambda x: (x["status"] != "connected", x["status"] != "connecting", x["eui"]))
        
        current_statuses = {bs["eui"]: bs["status"] for bs in result}
        _track_bs_status_changes(current_statuses)
        
        return jsonify({"success": True, "base_stations": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/certificates/status')
@login_required
@permission_required('can_manage_certificates')
def get_bs_certificates_status():
    """Get cert status for ALL base stations"""
    try:
        config = load_base_station_config()
        bs_config = _filter_base_stations_for_tenant(config.get("base_stations", {}), tenant_id=_active_tenant_id())
        result = []
        for eui_key, bs_data in bs_config.items():
            eui_lower = eui_key.lower()
            bs_cert_dir = os.path.join('certs', f'bs_{eui_lower}')
            cert_path = os.path.join(bs_cert_dir, f'{eui_lower}_cert.pem')
            cert_exists = os.path.exists(cert_path)
            cert_expires = bs_data.get("cert_expires", "")
            cert_generated = bs_data.get("cert_generated", "")
            status = "missing"
            if cert_exists and cert_expires:
                try:
                    exp_dt = datetime.strptime(cert_expires, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    if exp_dt < now:
                        status = "expired"
                    elif (exp_dt - now).days < 30:
                        status = "expiring_soon"
                    else:
                        status = "valid"
                except:
                    status = "valid" if cert_exists else "missing"
            elif cert_exists:
                status = "valid"
            result.append({
                "eui": eui_lower,
                "name": bs_data.get("name", ""),
                "tenant_id": bs_data.get("tenant_id", _active_tenant_id()),
                "cert_exists": cert_exists,
                "cert_status": status,
                "cert_generated": cert_generated,
                "cert_expires": cert_expires
            })
        return jsonify({"success": True, "certificates": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/uptime')
@login_required
def get_bs_uptime():
    """Get uptime data for all base stations"""
    try:
        active_tenant = _active_tenant_id()
        allowed_bs = {
            str(eui).strip().upper()
            for eui in _filter_base_stations_for_tenant(
                load_base_station_config().get("base_stations", {}),
                tenant_id=active_tenant,
            ).keys()
        }
        requested_source = (request.args.get("source") or bssci_config.TELEMETRY_SOURCE or "auto").strip().lower()
        if requested_source not in {"auto", "runtime", "influx"}:
            requested_source = "auto"

        filtered_runtime_events = {}
        for eui, events in (bs_uptime_events or {}).items():
            eui_upper = str(eui or "").strip().upper()
            if eui_upper in allowed_bs:
                filtered_runtime_events[eui_upper] = events

        runtime_payload = {
            "success": True,
            "source": "runtime_events_memory",
            "requested_source": requested_source,
            "uptime_events": filtered_runtime_events
        }

        if requested_source == "runtime":
            return jsonify(runtime_payload)

        if requested_source in {"auto", "influx"}:
            influx_result = _get_influx_uptime_events()
            if influx_result.get("success"):
                filtered_influx = {}
                for eui, events in (influx_result.get("uptime_events", {}) or {}).items():
                    if str(eui or "").strip().upper() in allowed_bs:
                        filtered_influx[str(eui or "").strip().upper()] = events
                return jsonify({
                    "success": True,
                    "source": influx_result.get("source", "influxdb"),
                    "requested_source": requested_source,
                    "uptime_events": filtered_influx
                })

            if requested_source == "influx":
                # Explicit influx requested - return runtime fallback with reason to keep UI alive.
                runtime_payload["fallback_reason"] = influx_result.get("error", "Influx query failed")
                runtime_payload["source"] = "runtime_events_memory_fallback"
                return jsonify(runtime_payload)

            # auto mode fallback
            runtime_payload["fallback_reason"] = influx_result.get("error", "Influx query failed")
            return jsonify(runtime_payload)

        return jsonify(runtime_payload)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/influx/sync-inventory', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def sync_inventory_to_influx():
    """Force one-time snapshot sync of sensors/base stations to InfluxDB."""
    try:
        trigger = (request.args.get("trigger") or "manual").strip().lower()
        result = _sync_inventory_snapshot_to_influx(trigger=trigger)
        if result.get("success"):
            return jsonify({
                "success": True,
                "message": f'Snapshot written: {result.get("line_count", 0)} points',
                **result
            })
        return jsonify({
            "success": False,
            "message": f'Influx sync failed: {result.get("error", "unknown error")}',
            **result
        }), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/timescale/status', methods=['GET'])
@login_required
@permission_required('can_edit_config')
def timescale_status():
    """Check TimescaleDB connectivity and readiness."""
    worker_stats = get_timescale_uplink_runtime_stats()
    ok, err = _timescale_is_ready()
    if not ok:
        return jsonify({
            "success": False,
            "enabled": bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)),
            "error": err,
            "telemetry_worker": worker_stats,
        }), 200
    conn, conn_err = _timescale_connect()
    if conn is None:
        return jsonify({
            "success": False,
            "enabled": True,
            "error": conn_err,
            "telemetry_worker": worker_stats,
        }), 200
    try:
        _ensure_timescale_schema(conn)
        if _timescale_telemetry_enabled():
            _ensure_timescale_uplink_worker_started()
        with conn.cursor() as cur:
            cur.execute("SELECT NOW()")
            now_value = cur.fetchone()[0]
        return jsonify({
            "success": True,
            "enabled": True,
            "message": "TimescaleDB reachable",
            "db_time": str(now_value),
            "host": getattr(bssci_config, "TIMESCALE_HOST", "timescaledb"),
            "database": getattr(bssci_config, "TIMESCALE_DB", "bssci"),
            "policies": {
                "retention_enabled": bool(getattr(bssci_config, "TIMESCALE_RETENTION_ENABLED", True)),
                "telemetry_retention_days": int(getattr(bssci_config, "TIMESCALE_TELEMETRY_RETENTION_DAYS", 90)),
                "inventory_retention_days": int(getattr(bssci_config, "TIMESCALE_INVENTORY_RETENTION_DAYS", 365)),
                "compression_enabled": bool(getattr(bssci_config, "TIMESCALE_COMPRESSION_ENABLED", True)),
                "compression_after_days": int(getattr(bssci_config, "TIMESCALE_COMPRESSION_AFTER_DAYS", 7)),
            },
            "telemetry_worker": get_timescale_uplink_runtime_stats(),
        })
    except Exception as exc:
        return jsonify({
            "success": False,
            "enabled": True,
            "error": str(exc),
            "telemetry_worker": get_timescale_uplink_runtime_stats(),
        }), 200
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.route('/api/timescale/sync-inventory', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def sync_inventory_to_timescale():
    """Force one-time snapshot sync of sensors/base stations to TimescaleDB."""
    try:
        trigger = (request.args.get("trigger") or "manual").strip().lower()
        result = _sync_inventory_snapshot_to_timescale(trigger=trigger)
        if result.get("success"):
            return jsonify({
                "success": True,
                "message": f'Snapshot written: {result.get("line_count", 0)} records',
                **result
            })
        return jsonify({
            "success": False,
            "message": f'Timescale sync failed: {result.get("error", "unknown error")}',
            **result
        }), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/timescale/policies/apply', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def apply_timescale_policies():
    """Apply retention/compression policy settings to TimescaleDB."""
    conn, err = _timescale_connect()
    if conn is None:
        return jsonify({"success": False, "error": err}), 500
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            _timescale_apply_policies(cur)
        return jsonify({
            "success": True,
            "message": "Timescale policies applied",
            "policies": {
                "retention_enabled": bool(getattr(bssci_config, "TIMESCALE_RETENTION_ENABLED", True)),
                "telemetry_retention_days": int(getattr(bssci_config, "TIMESCALE_TELEMETRY_RETENTION_DAYS", 90)),
                "inventory_retention_days": int(getattr(bssci_config, "TIMESCALE_INVENTORY_RETENTION_DAYS", 365)),
                "compression_enabled": bool(getattr(bssci_config, "TIMESCALE_COMPRESSION_ENABLED", True)),
                "compression_after_days": int(getattr(bssci_config, "TIMESCALE_COMPRESSION_AFTER_DAYS", 7)),
            },
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.route('/api/timescale/telemetry', methods=['GET'])
@login_required
def timescale_telemetry_summary():
    """Get aggregated telemetry uplink summary from TimescaleDB."""
    try:
        window_minutes = int(request.args.get("minutes", 60))
        bucket_seconds = int(request.args.get("bucket_seconds", 60))
        top_limit = int(request.args.get("top", 10))
        result = _timescale_fetch_telemetry_summary(
            window_minutes=window_minutes,
            bucket_seconds=bucket_seconds,
            top_limit=top_limit,
        )
        result["telemetry_worker"] = get_timescale_uplink_runtime_stats()
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/<eui>', methods=['GET'])
@login_required
def get_base_station(eui):
    """Get single base station details"""
    try:
        config = load_base_station_config()
        bs_data = config.get("base_stations", {}).get(eui.lower(), {}) or {}
        if not bs_data:
            return jsonify({"success": False, "error": "Base station not found"}), 404
        if not _tenant_matches(_tenant_id_from_base_station(bs_data), _active_tenant_id()):
            return jsonify({"success": False, "error": "Base station not found in active tenant"}), 404
        return jsonify({"eui": eui.lower(), **bs_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def add_base_station():
    """Add new base station"""
    try:
        data = request.get_json() or {}
        eui = data.get("eui", "").lower()
        gps_lat, gps_lng = _normalize_gps_coordinates(data.get("gps_lat"), data.get("gps_lng"))
        
        if not eui or not _validate_eui(eui):
            return jsonify({"success": False, "error": "Invalid EUI (must be 16 hex characters)"}), 400
        
        config = load_base_station_config()
        if eui in config.get("base_stations", {}):
            return jsonify({"success": False, "error": "Base station already exists"}), 400
        tenant_id = _active_tenant_id()
        
        config["base_stations"][eui] = {
            "name": data.get("name", ""),
            "tags": data.get("tags", []),
            "ip": data.get("ip", ""),
            "gps_lat": gps_lat,
            "gps_lng": gps_lng,
            "tenant_id": tenant_id,
        }
        save_base_station_config(config)

        _try_record_inventory_event(
            "base_station",
            "created",
            eui,
            {
                "name": data.get("name", ""),
                "ip": data.get("ip", ""),
                "tags": data.get("tags", []),
                "gps_lat": gps_lat,
                "gps_lng": gps_lng,
                "tenant_id": tenant_id,
            }
        )

        _upsert_device_gps_position("bs", eui, gps_lat, gps_lng)
        _record_admin_audit(
            action='base_station.create',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                "name": data.get("name", ""),
                "ip": data.get("ip", ""),
                "tags_count": len(data.get("tags", [])),
                "gps_lat": gps_lat,
                "gps_lng": gps_lng,
                "tenant_id": tenant_id,
                "generate_cert": bool(data.get("generate_cert", True)),
            },
        )
        
        generate_cert = data.get("generate_cert", True)
        cert_download_url = None
        if generate_cert:
            success, msg = _generate_bs_certificate(eui)
            if success:
                cert_download_url = f"/api/base-stations/{eui}/certificate/download"
        
        result = {"success": True}
        if cert_download_url:
            result["cert_download_url"] = cert_download_url
        return jsonify(result)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/<eui>', methods=['PUT'])
@login_required
@permission_required('can_edit_sensors')
def update_base_station(eui):
    """Update base station"""
    try:
        data = request.get_json() or {}
        eui = eui.lower()
        gps_lat, gps_lng = _normalize_gps_coordinates(data.get("gps_lat"), data.get("gps_lng"))
        
        config = load_base_station_config()
        if "base_stations" not in config:
            config["base_stations"] = {}
        if eui in config["base_stations"]:
            existing_tenant = _tenant_id_from_base_station(config["base_stations"].get(eui, {}))
            if not _tenant_matches(existing_tenant, _active_tenant_id()):
                return jsonify({"success": False, "error": "Base station not found in active tenant"}), 404
        
        previous_data = dict(config["base_stations"].get(eui, {}))
        config["base_stations"][eui] = {
            "name": data.get("name", ""),
            "tags": data.get("tags", []),
            "ip": data.get("ip", ""),
            "gps_lat": gps_lat,
            "gps_lng": gps_lng,
            "tenant_id": _normalize_tenant_id(
                data.get("tenant_id"),
                fallback=previous_data.get("tenant_id") or _active_tenant_id()
            ),
        }
        save_base_station_config(config)

        _try_record_inventory_event(
            "base_station",
            "updated",
            eui,
            {
                "name": data.get("name", ""),
                "ip": data.get("ip", ""),
                "tags": data.get("tags", []),
                "gps_lat": gps_lat,
                "gps_lng": gps_lng,
                "previous_name": previous_data.get("name", ""),
                "previous_ip": previous_data.get("ip", ""),
                "tenant_id": config["base_stations"][eui].get("tenant_id", _active_tenant_id()),
            }
        )

        _upsert_device_gps_position("bs", eui, gps_lat, gps_lng)
        _record_admin_audit(
            action='base_station.update',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                "name": data.get("name", ""),
                "ip": data.get("ip", ""),
                "tags_count": len(data.get("tags", [])),
                "gps_lat": gps_lat,
                "gps_lng": gps_lng,
                "tenant_id": config["base_stations"][eui].get("tenant_id", _active_tenant_id()),
                "previous_name": previous_data.get("name", ""),
                "previous_ip": previous_data.get("ip", ""),
            },
        )
        
        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/<eui>', methods=['DELETE'])
@login_required
@permission_required('can_edit_sensors')
def delete_base_station(eui):
    """Delete base station from config"""
    try:
        config = load_base_station_config()
        eui = eui.lower()
        
        removed = config.get("base_stations", {}).get(eui, {})
        if removed and not _tenant_matches(_tenant_id_from_base_station(removed), _active_tenant_id()):
            return jsonify({"success": False, "error": "Base station not found in active tenant"}), 404
        if eui in config.get("base_stations", {}):
            del config["base_stations"][eui]
            save_base_station_config(config)

        _try_record_inventory_event(
            "base_station",
            "deleted",
            eui,
            {
                "name": removed.get("name", ""),
                "ip": removed.get("ip", ""),
                "tags": removed.get("tags", []),
                "gps_lat": removed.get("gps_lat"),
                "gps_lng": removed.get("gps_lng"),
                "tenant_id": _tenant_id_from_base_station(removed),
            }
        )

        _remove_device_position("bs", eui)
        _record_admin_audit(
            action='base_station.delete',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                "name": removed.get("name", ""),
                "ip": removed.get("ip", ""),
                "tenant_id": _tenant_id_from_base_station(removed),
            },
        )
        
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/traffic/metrics')
@login_required
def get_traffic_metrics():
    """Get traffic metrics for visualization"""
    try:
        global tls_server_instance
        timescale_summary = _timescale_fetch_telemetry_summary(window_minutes=60, bucket_seconds=60, top_limit=10)
        timescale_summary["telemetry_worker"] = get_timescale_uplink_runtime_stats()
        if tls_server_instance and hasattr(tls_server_instance, 'get_traffic_metrics'):
            data = tls_server_instance.get_traffic_metrics()
            return jsonify({'success': True, **data, 'timescale': timescale_summary})
        return jsonify({
            'success': True,
            'metrics': {
                'messages_in': 0,
                'messages_out': 0,
                'messages_dropped': 0,
                'bytes_in': 0,
                'bytes_out': 0,
                'vm_messages': 0,
                'attach_requests': 0,
                'detach_requests': 0,
                'status_requests': 0,
                'start_time': 0
            },
            'dedup_stats': {'total_messages': 0, 'duplicate_messages': 0, 'published_messages': 0},
            'history': [],
            'connections': 0,
            'timescale': timescale_summary
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/traffic/reset', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def reset_traffic_metrics():
    """Reset traffic metrics"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'reset_traffic_metrics'):
            tls_server_instance.reset_traffic_metrics()
            return jsonify({'success': True, 'message': 'Traffic metrics reset successfully'})
        return jsonify({'success': False, 'message': 'TLS server not available'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/oms/meters')
@login_required
def get_oms_meters():
    """Get all tracked OMS meters"""
    try:
        if not getattr(bssci_config, 'OMS_ENABLED', True):
            return jsonify({'success': False, 'message': 'OMS module is disabled'}), 404
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_oms_meters'):
            meters = tls_server_instance.get_oms_meters()
            return jsonify({'success': True, 'meters': list(meters.values())})
        return jsonify({'success': True, 'meters': []})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/oms/stats')
@login_required
def get_oms_stats():
    """Get OMS statistics"""
    try:
        if not getattr(bssci_config, 'OMS_ENABLED', True):
            return jsonify({'success': False, 'message': 'OMS module is disabled'}), 404
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_oms_stats'):
            stats = tls_server_instance.get_oms_stats()
            return jsonify({'success': True, **stats})
        return jsonify({'success': True, 'total_meters': 0, 'total_messages': 0})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/logs')
@login_required
def get_logs():
    global log_entries
    ensure_web_log_handler()

    # Get query parameters for filtering
    level_filter = str(request.args.get('level', 'all') or 'all').strip().upper()
    logger_filter = str(request.args.get('logger', 'all') or 'all').strip()
    text_filter = str(request.args.get('q', '') or '').strip().lower()
    try:
        limit = int(request.args.get('limit', 100))
    except (TypeError, ValueError):
        limit = 100
    limit = max(10, min(limit, 1000))

    # Filter logs based on parameters
    filtered_logs = list(log_entries)

    if level_filter != 'ALL':
        filtered_logs = [log for log in filtered_logs if str(log.get('level', '')).upper() == level_filter]

    if logger_filter.lower() != 'all':
        needle = logger_filter.lower()
        filtered_logs = [log for log in filtered_logs if needle in str(log.get('logger', '')).lower()]

    if text_filter:
        def _log_matches_text(entry):
            message = str(entry.get('message', '')).lower()
            logger_name = str(entry.get('logger', '')).lower()
            level_name = str(entry.get('level', '')).lower()
            timestamp = str(entry.get('timestamp', '')).lower()
            source = str(entry.get('source', '')).lower()
            return (
                text_filter in message
                or text_filter in logger_name
                or text_filter in level_name
                or text_filter in timestamp
                or text_filter in source
            )
        filtered_logs = [log for log in filtered_logs if _log_matches_text(log)]

    # Return the most recent logs (up to limit)
    recent_logs = filtered_logs[-limit:] if len(filtered_logs) > limit else filtered_logs

    level_counts = {'ERROR': 0, 'WARNING': 0, 'INFO': 0, 'DEBUG': 0}
    for log in filtered_logs:
        level = str(log.get('level', '')).upper()
        if level in level_counts:
            level_counts[level] += 1
    logger_names = sorted({str(log.get('logger', '')).strip() for log in log_entries if str(log.get('logger', '')).strip()})

    return jsonify({
        'logs': recent_logs,
        'total_logs': len(log_entries),
        'filtered_logs': len(filtered_logs),
        'source': 'memory',
        'level_counts': level_counts,
        'logger_names': logger_names
    })


def _evaluate_monitor_level(value, warning_threshold, critical_threshold):
    number = float(value or 0.0)
    if number >= float(critical_threshold):
        return "critical"
    if number >= float(warning_threshold):
        return "warning"
    return "good"


def _build_mqtt_monitor_insights(runtime_status):
    mqtt_queue = dict((runtime_status or {}).get("queue", {}) or {})
    mqtt_stats = dict((runtime_status or {}).get("stats", {}) or {})
    timescale_stats = get_timescale_uplink_runtime_stats()

    def _safe_threshold(value, fallback):
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    queue_warn = max(0.0, _safe_threshold(getattr(bssci_config, "MONITOR_QUEUE_WARN_PCT", 65.0), 65.0))
    queue_crit = max(queue_warn, _safe_threshold(getattr(bssci_config, "MONITOR_QUEUE_CRIT_PCT", 85.0), 85.0))
    retry_warn = max(0.0, _safe_threshold(getattr(bssci_config, "MONITOR_RETRY_FAIL_WARN_PCT", 5.0), 5.0))
    retry_crit = max(retry_warn, _safe_threshold(getattr(bssci_config, "MONITOR_RETRY_FAIL_CRIT_PCT", 20.0), 20.0))
    latency_warn = max(0.0, _safe_threshold(getattr(bssci_config, "MONITOR_DB_WRITE_LATENCY_WARN_MS", 500.0), 500.0))
    latency_crit = max(latency_warn, _safe_threshold(getattr(bssci_config, "MONITOR_DB_WRITE_LATENCY_CRIT_MS", 1500.0), 1500.0))
    reconnect_warn = max(
        0.0,
        _safe_threshold(getattr(bssci_config, "MONITOR_MQTT_RECONNECT_WARN_PER_HOUR", 3.0), 3.0),
    )
    reconnect_crit = max(
        reconnect_warn,
        _safe_threshold(getattr(bssci_config, "MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR", 8.0), 8.0),
    )

    in_util = float(mqtt_queue.get("in_utilization_pct") or 0.0)
    out_util = float(mqtt_queue.get("out_utilization_pct") or 0.0)
    db_util = float(timescale_stats.get("queue_utilization_pct") or 0.0)
    queue_depth_pct = max(in_util, out_util, db_util)
    queue_level = _evaluate_monitor_level(queue_depth_pct, warning_threshold=queue_warn, critical_threshold=queue_crit)

    mqtt_retry_exhausted = int(mqtt_stats.get("outgoing_retry_exhausted", 0) or 0)
    mqtt_retry_attempts = int(mqtt_stats.get("outgoing_retried", 0) or 0)
    mqtt_retry_fail_rate_pct = (
        (mqtt_retry_exhausted / mqtt_retry_attempts) * 100.0
        if mqtt_retry_attempts > 0 else 0.0
    )
    db_failed_batches = int(timescale_stats.get("failed_batches", 0) or 0)
    db_retried_batches = int(timescale_stats.get("retried_batches", 0) or 0)
    db_retry_denominator = db_failed_batches + db_retried_batches
    db_retry_fail_rate_pct = (
        (db_failed_batches / db_retry_denominator) * 100.0
        if db_retry_denominator > 0 else 0.0
    )
    retry_fail_rate_pct = max(mqtt_retry_fail_rate_pct, db_retry_fail_rate_pct)
    retry_level = _evaluate_monitor_level(retry_fail_rate_pct, warning_threshold=retry_warn, critical_threshold=retry_crit)

    db_latency_samples = int(timescale_stats.get("write_latency_samples", 0) or 0)
    db_write_latency_ms = float(timescale_stats.get("write_latency_last_ms", 0.0) or 0.0)
    if db_latency_samples <= 0:
        db_latency_level = "neutral"
        db_write_latency_display = None
    else:
        db_latency_level = _evaluate_monitor_level(
            db_write_latency_ms,
            warning_threshold=latency_warn,
            critical_threshold=latency_crit,
        )
        db_write_latency_display = round(db_write_latency_ms, 2)

    reconnects_last_hour = int((runtime_status or {}).get("reconnects_last_hour", 0) or 0)
    reconnect_total = int(mqtt_stats.get("reconnect_count", 0) or 0)
    reconnect_level = _evaluate_monitor_level(
        reconnects_last_hour,
        warning_threshold=reconnect_warn,
        critical_threshold=reconnect_crit,
    )

    alerts = []
    if queue_level in {"warning", "critical"}:
        alerts.append({
            "level": queue_level,
            "metric": "queue_depth",
            "text": f"Queue depth is high ({round(queue_depth_pct, 1)}%).",
        })
    if retry_level in {"warning", "critical"}:
        alerts.append({
            "level": retry_level,
            "metric": "retry_fail_rate",
            "text": f"Retry failure rate is elevated ({round(retry_fail_rate_pct, 2)}%).",
        })
    if db_latency_level in {"warning", "critical"}:
        alerts.append({
            "level": db_latency_level,
            "metric": "db_write_latency",
            "text": f"DB write latency is high ({round(db_write_latency_ms, 1)} ms).",
        })
    if reconnect_level in {"warning", "critical"}:
        alerts.append({
            "level": reconnect_level,
            "metric": "mqtt_reconnect_count",
            "text": f"MQTT reconnect frequency is high ({reconnects_last_hour}/h).",
        })
    if not alerts:
        alerts.append({
            "level": "good",
            "metric": "overall",
            "text": "No critical runtime signals detected.",
        })

    return {
        "metrics": {
            "queue_depth": {
                "value_pct": round(queue_depth_pct, 2),
                "level": queue_level,
                "mqtt_in_pct": round(in_util, 2),
                "mqtt_out_pct": round(out_util, 2),
                "db_queue_pct": round(db_util, 2),
            },
            "retry_fail_rate": {
                "value_pct": round(retry_fail_rate_pct, 2),
                "level": retry_level,
                "mqtt_retry_fail_rate_pct": round(mqtt_retry_fail_rate_pct, 2),
                "db_retry_fail_rate_pct": round(db_retry_fail_rate_pct, 2),
                "mqtt_retry_exhausted": mqtt_retry_exhausted,
                "db_failed_batches": db_failed_batches,
            },
            "db_write_latency": {
                "value_ms": db_write_latency_display,
                "level": db_latency_level,
                "last_ms": round(db_write_latency_ms, 2),
                "avg_ms": round(float(timescale_stats.get("write_latency_avg_ms", 0.0) or 0.0), 2),
                "max_ms": round(float(timescale_stats.get("write_latency_max_ms", 0.0) or 0.0), 2),
                "samples": db_latency_samples,
            },
            "mqtt_reconnect_count": {
                "value_per_hour": reconnects_last_hour,
                "level": reconnect_level,
                "total": reconnect_total,
            },
        },
        "alerts": alerts,
        "timescale": {
            "queue_size": int(timescale_stats.get("queue_size", 0) or 0),
            "queue_maxsize": int(timescale_stats.get("queue_maxsize", 0) or 0),
            "queue_utilization_pct": round(float(timescale_stats.get("queue_utilization_pct", 0.0) or 0.0), 2),
            "retry_attempts": int(timescale_stats.get("retry_attempts", 0) or 0),
            "failed_batches": db_failed_batches,
            "retried_batches": db_retried_batches,
            "last_error": str(timescale_stats.get("last_error", "") or ""),
            "last_write_ts": float(timescale_stats.get("last_write_ts", 0.0) or 0.0),
        },
        "thresholds": {
            "queue_warn_pct": round(queue_warn, 2),
            "queue_crit_pct": round(queue_crit, 2),
            "retry_warn_pct": round(retry_warn, 2),
            "retry_crit_pct": round(retry_crit, 2),
            "db_latency_warn_ms": round(latency_warn, 2),
            "db_latency_crit_ms": round(latency_crit, 2),
            "reconnect_warn_per_hour": round(reconnect_warn, 2),
            "reconnect_crit_per_hour": round(reconnect_crit, 2),
        },
    }

@app.route('/api/mqtt/monitor')
@login_required
def get_mqtt_monitor():
    try:
        global mqtt_client_instance
        if not mqtt_client_instance:
            runtime_status = {
                "connected": False,
                "message": "MQTT client is not initialized.",
                "stats": {},
                "queue": {"in_size": 0, "out_size": 0, "in_utilization_pct": 0.0, "out_utilization_pct": 0.0},
                "recent_incoming_topics": [],
                "recent_outgoing_topics": [],
                "recent_errors": [],
                "last_error": "",
                "connected_since": 0,
                "disconnected_since": 0,
                "connected_seconds": 0,
                "reconnects_last_hour": 0,
                "broker_host": "",
                "broker_port": int(getattr(bssci_config, "MQTT_PORT", 1883)),
                "base_topic": "",
                "username_set": bool(getattr(bssci_config, "MQTT_USERNAME", "")),
            }
            return jsonify({
                "success": True,
                "available": False,
                "status": runtime_status,
                "insights": _build_mqtt_monitor_insights(runtime_status),
            })

        if hasattr(mqtt_client_instance, "get_runtime_status"):
            runtime_status = mqtt_client_instance.get_runtime_status()
        else:
            runtime_status = {
                "connected": bool(getattr(mqtt_client_instance, "connected", False)),
                "stats": dict(getattr(mqtt_client_instance, "stats", {}) or {}),
                "queue": {"in_size": 0, "out_size": 0},
                "recent_incoming_topics": [],
                "recent_outgoing_topics": [],
                "recent_errors": [],
                "last_error": "",
                "connected_since": 0,
                "disconnected_since": 0,
                "connected_seconds": 0,
                "reconnects_last_hour": 0,
                "broker_host": str(getattr(mqtt_client_instance, "broker_host", "") or ""),
                "broker_port": int(getattr(bssci_config, "MQTT_PORT", 1883)),
                "base_topic": str(getattr(mqtt_client_instance, "base_topic", "") or ""),
                "username_set": bool(getattr(bssci_config, "MQTT_USERNAME", "")),
            }
        insights = _build_mqtt_monitor_insights(runtime_status)

        return jsonify({
            "success": True,
            "available": True,
            "status": runtime_status,
            "insights": insights,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/mqtt/publish-test', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def mqtt_publish_test():
    try:
        global mqtt_client_instance
        if not mqtt_client_instance:
            return jsonify({"success": False, "error": "MQTT client is not initialized"}), 503

        connected = bool(getattr(mqtt_client_instance, "connected", False))
        if not connected:
            return jsonify({"success": False, "error": "MQTT is currently disconnected"}), 409

        out_queue = getattr(mqtt_client_instance, "mqtt_out_queue", None)
        if out_queue is None:
            return jsonify({"success": False, "error": "MQTT outgoing queue is unavailable"}), 503

        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({"success": False, "error": "Invalid JSON body"}), 400

        raw_topic = str(data.get("topic", "") or "").strip()
        if not raw_topic:
            return jsonify({"success": False, "error": "Topic is required"}), 400
        if "+" in raw_topic or "#" in raw_topic:
            return jsonify({"success": False, "error": "Wildcard topics are not allowed"}), 400

        base_topic = str(
            getattr(
                mqtt_client_instance,
                "base_topic",
                globals().get("BASE_TOPIC", "mioty") or "mioty",
            )
            or "mioty"
        ).rstrip("/")
        if raw_topic == base_topic:
            return jsonify({"success": False, "error": "Use a topic below the base topic"}), 400
        if raw_topic.startswith(base_topic + "/"):
            topic_suffix = raw_topic[len(base_topic) + 1 :].strip("/")
        else:
            topic_suffix = raw_topic.strip("/")
        if not topic_suffix:
            return jsonify({"success": False, "error": "Topic suffix is required"}), 400
        if len(topic_suffix) > 240:
            return jsonify({"success": False, "error": "Topic is too long"}), 400

        payload_mode = str(data.get("payload_mode", "text") or "text").strip().lower()
        payload_raw = data.get("payload", "")
        if payload_mode == "json":
            try:
                payload_value = json.loads(str(payload_raw or ""))
            except json.JSONDecodeError as exc:
                return jsonify({"success": False, "error": f"Invalid JSON payload: {exc.msg}"}), 400
        elif payload_mode == "text":
            payload_value = str(payload_raw or "")
        else:
            return jsonify({"success": False, "error": "payload_mode must be 'text' or 'json'"}), 400

        try:
            qos = int(data.get("qos", 0))
        except (TypeError, ValueError):
            qos = 0
        qos = 0 if qos < 0 else (2 if qos > 2 else qos)
        retain = bool(data.get("retain", False))

        msg = {
            "topic": topic_suffix,
            "payload": payload_value,
            "qos": qos,
            "retain": retain,
        }

        try:
            out_queue.put_nowait(msg)
        except asyncio.QueueFull:
            return jsonify({"success": False, "error": "MQTT publish queue is full"}), 503

        payload_size = 0
        try:
            payload_size = len(json.dumps(payload_value, ensure_ascii=True))
        except Exception:
            payload_size = len(str(payload_value))
        _record_admin_audit(
            action='mqtt.publish_test',
            entity='mqtt',
            target_id=f"{base_topic}/{topic_suffix}",
            status='success',
            details={
                "topic": f"{base_topic}/{topic_suffix}",
                "payload_mode": payload_mode,
                "payload_size": payload_size,
                "qos": qos,
                "retain": retain,
            },
        )

        return jsonify({
            "success": True,
            "message": "MQTT message queued for publish",
            "topic": f"{base_topic}/{topic_suffix}",
            "qos": qos,
            "retain": retain,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# =========================
# UPDATE MANAGEMENT SYSTEM
# =========================

def get_current_version():
    """Get current version from VERSION file or fallback methods"""
    try:
        # First: Try to read VERSION file (preferred method)
        if os.path.exists('VERSION'):
            try:
                with open('VERSION', 'r') as f:
                    version = f.read().strip()
                    if version:
                        return f"v{version}" if not version.startswith('v') else version
            except:
                pass
        
        # Fallback: Try Git commands
        try:
            lock_file = '.git/index.lock'
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
            
            result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                commit_hash = result.stdout.strip()
                
                tag_result = subprocess.run(['git', 'describe', '--tags', '--exact-match', 'HEAD'], 
                                          capture_output=True, text=True, timeout=10)
                if tag_result.returncode == 0:
                    return tag_result.stdout.strip()
                else:
                    return f"commit-{commit_hash}"
        except FileNotFoundError:
            pass
        except Exception as e:
            if "No such file or directory" not in str(e):
                print(f"Git command error: {e}")
        
        # Fallback 1: try to read .git/HEAD directly
        try:
            with open('.git/HEAD', 'r') as f:
                head_ref = f.read().strip()
                if head_ref.startswith('ref: refs/heads/'):
                    # Get branch name and try to read commit
                    branch = head_ref.split('/')[-1]
                    ref_path = f'.git/refs/heads/{branch}'
                    try:
                        with open(ref_path, 'r') as ref_file:
                            commit = ref_file.read().strip()[:7]
                            return f"local-{commit}"
                    except:
                        return f"branch-{branch}"
                else:
                    # Direct commit hash
                    return f"local-{head_ref[:7]}"
        except:
            pass
        
        # Fallback 2: Use file modification timestamps
        try:
            import time
            main_files = ['main.py', 'web_ui.py', 'TLSServer.py', 'mqtt_interface.py']
            latest_time = 0
            for file in main_files:
                if os.path.exists(file):
                    mtime = os.path.getmtime(file)
                    latest_time = max(latest_time, mtime)
            
            if latest_time > 0:
                date_str = time.strftime('%Y%m%d', time.localtime(latest_time))
                return f"local-{date_str}"
        except:
            pass
            
        return "local-version"
    except Exception as e:
        print(f"Error getting current version: {e}")
        return "version-unknown"

def get_remote_version():
    """Get latest remote version - checks releases first, then commits"""
    GITHUB_REPO = "plasmonized/containerized-mioty-Service-Center"
    
    try:
        import urllib.request
        import ssl
        
        ctx = ssl.create_default_context()
        
        # First: Try to get latest release/tag
        try:
            releases_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            req = urllib.request.Request(releases_url, headers={'User-Agent': 'BSSCI-Service-Center'})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                data = json.loads(response.read().decode())
                tag_name = data.get('tag_name', '')
                if tag_name:
                    return tag_name if tag_name.startswith('v') else f"v{tag_name}"
        except:
            pass
        
        # Fallback: Try to get latest tag
        try:
            tags_url = f"https://api.github.com/repos/{GITHUB_REPO}/tags"
            req = urllib.request.Request(tags_url, headers={'User-Agent': 'BSSCI-Service-Center'})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                tags = json.loads(response.read().decode())
                if tags and len(tags) > 0:
                    tag_name = tags[0].get('name', '')
                    if tag_name:
                        return tag_name if tag_name.startswith('v') else f"v{tag_name}"
        except:
            pass
        
        # Fallback: Get latest commit
        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/main"
        req = urllib.request.Request(api_url, headers={'User-Agent': 'BSSCI-Service-Center'})
        
        try:
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                data = json.loads(response.read().decode())
                commit_hash = data.get('sha', '')[:7]
                commit_date = data.get('commit', {}).get('committer', {}).get('date', '')[:10]
                return f"commit-{commit_hash} ({commit_date})"
        except Exception as api_error:
            logger.error(f"GitHub API error: {api_error}")
        
        # Fallback to Git commands if API fails
        try:
            lock_file = '.git/index.lock'
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
            
            subprocess.run(['git', 'fetch', '--tags', 'origin', 'main'], 
                         capture_output=True, text=True, timeout=30)
            
            result = subprocess.run(['git', 'rev-parse', '--short', 'origin/main'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return f"commit-{result.stdout.strip()}"
        except:
            pass
        
        return "remote-unavailable"
    except Exception as e:
        print(f"Error getting remote version: {e}")
        return "remote-check-unavailable"

def get_commit_log(limit=5):
    """Get recent commit log - works with or without Git"""
    try:
        # First try Git commands
        try:
            # Try to unlock git if needed
            lock_file = '.git/index.lock'
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
            
            result = subprocess.run(['git', 'log', '--oneline', f'-{limit}'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                commits = []
                for line in result.stdout.strip().split('\n'):
                    if line:
                        parts = line.split(' ', 1)
                        commits.append({
                            'hash': parts[0],
                            'message': parts[1] if len(parts) > 1 else ''
                        })
                return commits
        except FileNotFoundError:
            # Git not installed
            pass
        except Exception as e:
            if "No such file or directory" in str(e):
                # Git not installed
                pass
            else:
                print(f"Git command error: {e}")
        
        # Fallback: Return info about the current installation
        return [
            {'hash': 'local', 'message': 'Local installation - Git not available'},
            {'hash': 'info', 'message': 'Install Git to see commit history'},
            {'hash': 'note', 'message': 'Version checking works without Git'}
        ]
    except Exception as e:
        print(f"Error getting commit log: {e}")
        return [{'hash': 'error', 'message': f'Unable to get history: {str(e)}'}]

def parse_version(version_str):
    """Parse version string to comparable tuple"""
    try:
        v = version_str.lstrip('v').split('-')[0]
        parts = v.replace('.', ' ').split()
        return tuple(int(p) for p in parts if p.isdigit())
    except:
        return (0,)

def check_for_updates():
    """Check if updates are available using GitHub API"""
    GITHUB_REPO = "plasmonized/containerized-mioty-Service-Center"
    
    try:
        current = get_current_version()
        remote = get_remote_version()
        
        updates_available = False
        status_message = None
        
        if remote in ['remote-unavailable', 'remote-check-unavailable']:
            updates_available = False
            status_message = 'Cannot connect to GitHub to check for updates'
        elif current.startswith('v') and remote.startswith('v'):
            # Both are version numbers - compare them
            current_ver = parse_version(current)
            remote_ver = parse_version(remote)
            if remote_ver > current_ver:
                updates_available = True
                status_message = f'Update available: {current} → {remote}'
        elif "commit-" in current and "commit-" in remote:
            # Both are commit hashes
            current_hash = current.split("commit-")[1].split()[0][:7]
            remote_hash = remote.split("commit-")[1].split()[0][:7]
            if current_hash != remote_hash:
                updates_available = True
        elif current.startswith("local-") or current.startswith("v"):
            # Local installation or version, but remote is commit-based
            updates_available = True
            status_message = 'Update available'
        
        # Get recent commits via GitHub API
        recent_commits = []
        try:
            import urllib.request
            import ssl
            ctx = ssl.create_default_context()
            
            api_url = f"https://api.github.com/repos/{GITHUB_REPO}/commits?per_page=5"
            req = urllib.request.Request(api_url, headers={'User-Agent': 'BSSCI-Service-Center'})
            
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                commits_data = json.loads(response.read().decode())
                for commit in commits_data:
                    recent_commits.append({
                        'hash': commit['sha'][:7],
                        'message': commit['commit']['message'].split('\n')[0][:60]
                    })
                
                # If we got commits but remote was unavailable, use first commit as remote version
                if commits_data and remote in ['remote-unavailable', 'remote-check-unavailable']:
                    first_commit = commits_data[0]
                    remote_hash = first_commit['sha'][:7]
                    commit_date = first_commit['commit']['committer']['date'][:10]
                    remote = f"commit-{remote_hash} ({commit_date})"
                    updates_available = True
                    status_message = 'Update available'
        except Exception as e:
            logger.error(f"Error fetching commits: {e}")

        result = {
            'current_version': current,
            'remote_version': remote,
            'updates_available': updates_available,
            'recent_commits': recent_commits,
            'status': 'success'
        }
        
        if status_message:
            result['message'] = status_message
            
        return result
    except Exception as e:
        logger.error(f"Error checking for updates: {e}")
        return {'status': 'error', 'error': str(e)}

def create_backup():
    """Create backup before update"""
    try:
        # Use /tmp for Docker compatibility, fallback to current dir
        backup_base = '/tmp' if os.path.exists('/tmp') and os.access('/tmp', os.W_OK) else '.'
        backup_dir = os.path.join(backup_base, f"bssci_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Create backup directory
        os.makedirs(backup_dir, exist_ok=True)
        
        # Backup important files
        files_to_backup = ['.env', 'endpoints.json', 'VERSION']
        dirs_to_backup = ['certs']
        
        for file in files_to_backup:
            if os.path.exists(file):
                shutil.copy2(file, backup_dir)
                
        for dir_name in dirs_to_backup:
            if os.path.exists(dir_name):
                try:
                    shutil.copytree(dir_name, os.path.join(backup_dir, dir_name))
                except Exception as e:
                    logger.warning(f"Could not backup {dir_name}: {e}")
        
        return {'success': True, 'backup_dir': backup_dir}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def perform_update():
    """Perform update by downloading from GitHub"""
    GITHUB_REPO = "plasmonized/containerized-mioty-Service-Center"
    
    try:
        # Create backup first
        backup_result = create_backup()
        if not backup_result['success']:
            return {'success': False, 'error': f"Backup failed: {backup_result['error']}"}
        
        # First try git pull if we have a git repo
        if os.path.exists('.git'):
            try:
                subprocess.run(['git', 'reset', '--hard', 'HEAD'], capture_output=True, timeout=30)
                result = subprocess.run(['git', 'pull', 'origin', 'main'], 
                                      capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    return {
                        'success': True, 
                        'message': 'Update completed successfully via git',
                        'backup_dir': backup_result['backup_dir'],
                        'git_output': result.stdout
                    }
            except Exception as e:
                logger.error(f"Git pull failed, trying ZIP download: {e}")
        
        # Fallback: Download ZIP from GitHub
        import urllib.request
        import ssl
        import zipfile
        import tempfile
        
        ctx = ssl.create_default_context()
        
        # Try main branch first, then master
        for branch in ['main', 'master']:
            zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{branch}.zip"
            
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_file:
                    tmp_path = tmp_file.name
                    req = urllib.request.Request(zip_url, headers={'User-Agent': 'BSSCI-Service-Center'})
                    
                    with urllib.request.urlopen(req, timeout=60, context=ctx) as response:
                        tmp_file.write(response.read())
                
                # Extract ZIP
                extract_dir = tempfile.mkdtemp()
                with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                extracted_folders = os.listdir(extract_dir)
                if extracted_folders:
                    source_dir = os.path.join(extract_dir, extracted_folders[0])
                    
                    files_to_update = ['web_ui.py', 'TLSServer.py', 'main.py', 'web_main.py', 
                                     'mqtt_interface.py', 'messages.py', 'requirements.txt', 'VERSION']
                    dirs_to_update = ['templates', 'static']
                    
                    updated_files = []
                    for filename in files_to_update:
                        src = os.path.join(source_dir, filename)
                        if os.path.exists(src):
                            shutil.copy2(src, filename)
                            updated_files.append(filename)
                    
                    for dirname in dirs_to_update:
                        src_dir = os.path.join(source_dir, dirname)
                        if os.path.exists(src_dir):
                            for item in os.listdir(src_dir):
                                shutil.copy2(os.path.join(src_dir, item), 
                                           os.path.join(dirname, item))
                                updated_files.append(f'{dirname}/{item}')
                
                os.unlink(tmp_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                
                return {
                    'success': True, 
                    'message': f'Update completed via GitHub ({branch} branch)',
                    'backup_dir': backup_result['backup_dir'],
                    'updated_files': updated_files
                }
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    logger.warning(f"Branch {branch} not found, trying next...")
                    continue
                raise
        
        return {'success': False, 'error': 'Could not download from GitHub (no valid branch found)'}
        
    except PermissionError as e:
        logger.error(f"Update failed - permission denied: {e}")
        return {
            'success': False, 
            'error': 'Permission denied - files are read-only. For Docker: rebuild container with "docker-compose up -d --build"'
        }
    except Exception as e:
        logger.error(f"Update failed: {e}")
        if 'Permission denied' in str(e):
            return {
                'success': False, 
                'error': 'Permission denied - files are read-only. For Docker: rebuild container with "docker-compose up -d --build"'
            }
        return {'success': False, 'error': str(e)}

@app.route('/api/system/version')
def api_get_version():
    """Get current and remote version info"""
    try:
        version_info = check_for_updates()
        # Use remote commits from check_for_updates() - don't override with local git
        return jsonify(version_info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/check-updates')
def api_check_updates():
    """Check for available updates"""
    try:
        return jsonify(check_for_updates())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/update', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def api_perform_update():
    """System update is disabled in customized deployments."""
    return jsonify({
        'success': False,
        'error': 'Auto-update is disabled for this customized deployment. Use Git workflow and container rebuild.'
    }), 403

@app.route('/api/system/restart', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def api_restart_system():
    """Restart the service after update"""
    try:
        # Schedule restart in a separate thread to allow response to be sent
        def restart_service():
            time.sleep(2)  # Give time for response to be sent
            os._exit(0)  # Force exit - service manager should restart
            
        restart_thread = threading.Thread(target=restart_service)
        restart_thread.daemon = True
        restart_thread.start()
        
        return jsonify({'success': True, 'message': 'Service restart initiated'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Global variable to store TLS server instance
# tls_server_instance = None # Already defined at the top

def set_tls_server(server):
    """Set the TLS server instance"""
    global tls_server_instance
    tls_server_instance = server
    _ensure_influx_snapshot_worker_started()
    if _timescale_telemetry_enabled():
        _ensure_timescale_uplink_worker_started()


def set_mqtt_client(client):
    """Set the MQTT client instance for runtime status reporting."""
    global mqtt_client_instance
    mqtt_client_instance = client

def get_bssci_service_status():
    """Get the status of the BSSCI service - thread-safe version"""
    try:
        global tls_server_instance, mqtt_client_instance
        tls_server = tls_server_instance
        
        if not tls_server:
            return {
                'running': False,
                'service_type': 'web_ui',
                'tls_server': {'active': False},
                'mqtt_broker': {'active': False},
                'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []},
                'total_sensors': 0,
                'registered_sensors': 0,
                'pending_requests': 0,
                'error': 'TLS server not available'
            }
            
        # Get base station status safely without asyncio operations
        bs_status = {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []}
        try:
            # Thread-safe access to base station collections
            connected_count = 0
            connecting_count = 0
            connected_stations = []
            connecting_stations = []
            active_tenant = _active_tenant_id()
            allowed_bs = {
                str(eui).strip().upper()
                for eui in _filter_base_stations_for_tenant(
                    load_base_station_config().get("base_stations", {}),
                    tenant_id=active_tenant,
                ).keys()
            }
            
            if hasattr(tls_server, 'connected_base_stations'):
                connected_dict = getattr(tls_server, 'connected_base_stations', {})
                for writer, bs_eui in list(connected_dict.items()):
                    bs_upper = str(bs_eui or "").strip().upper()
                    if bs_upper not in allowed_bs:
                        continue
                    connected_stations.append({
                        "eui": bs_upper,
                        "address": "connected",
                        "status": "connected"
                    })
                connected_count = len(connected_stations)
            
            if hasattr(tls_server, 'connecting_base_stations'):
                connecting_dict = getattr(tls_server, 'connecting_base_stations', {})
                for writer, bs_eui in list(connecting_dict.items()):
                    bs_upper = str(bs_eui or "").strip().upper()
                    if bs_upper not in allowed_bs:
                        continue
                    connecting_stations.append({
                        "eui": bs_upper,
                        "address": "connecting", 
                        "status": "connecting"
                    })
                connecting_count = len(connecting_stations)
                
            bs_status = {
                "connected": connected_stations,
                "connecting": connecting_stations,
                "total_connected": connected_count,
                "total_connecting": connecting_count
            }
        except Exception as e:
            print(f"Error getting base station status: {e}")
            
        # Get sensor count safely
        total_sensors = 0
        registered_sensors = 0
        try:
            # Count sensors from config file instead of runtime status to avoid asyncio issues
            sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=_active_tenant_id())
            total_sensors = len(sensors)
            # For now, assume all configured sensors could be registered
            registered_sensors = total_sensors
        except Exception as e:
            print(f"Error counting sensors: {e}")

        # Build response safely
        mqtt_active = bool(mqtt_client_instance and getattr(mqtt_client_instance, "connected", False))
        mqtt_stats = dict(getattr(mqtt_client_instance, "stats", {}) or {}) if mqtt_client_instance else {}
        runtime_status = None
        if mqtt_client_instance and hasattr(mqtt_client_instance, "get_runtime_status"):
            try:
                runtime_status = mqtt_client_instance.get_runtime_status()
            except Exception:
                runtime_status = None
        if not isinstance(runtime_status, dict):
            runtime_status = {
                "connected": mqtt_active,
                "stats": mqtt_stats,
                "queue": {"in_size": 0, "out_size": 0, "in_utilization_pct": 0.0, "out_utilization_pct": 0.0},
                "reconnects_last_hour": 0,
            }
        monitor_insights = _build_mqtt_monitor_insights(runtime_status)

        response = {
            'running': True,
            'service_type': 'web_ui',
            'base_stations': bs_status,
            'tls_server': {
                'active': True,
                'listening_port': getattr(bssci_config, 'LISTEN_PORT', 16018),
                'connected_base_stations': bs_status.get('total_connected', 0),
                'total_sensors': total_sensors,
                'registered_sensors': registered_sensors
            },
            'mqtt_broker': {
                'active': mqtt_active,
                'broker_host': getattr(bssci_config, 'MQTT_BROKER', 'localhost'),
                'broker_port': getattr(bssci_config, 'MQTT_PORT', 1883),
                'stats': mqtt_stats,
                'runtime': runtime_status,
                'monitor_insights': monitor_insights,
            },
            'total_sensors': total_sensors,
            'registered_sensors': registered_sensors,
            'pending_requests': 0  # Avoid accessing asyncio objects
        }
        
        return response
        
    except Exception as e:
        print(f"Error in get_bssci_service_status: {e}")
        import traceback
        traceback.print_exc()
        return {
            'running': False,
            'service_type': 'web_ui',
            'tls_server': {'active': False},
            'mqtt_broker': {'active': False},
            'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []},
            'total_sensors': 0,
            'registered_sensors': 0,
            'pending_requests': 0,
            'error': f'Status error: {str(e)}'
        }

@app.route('/api/logs/clear', methods=['POST'])
@login_required
@admin_scope_required('clear_service_logs')
def clear_logs():
    global log_entries
    log_entries = []
    return jsonify({'success': True, 'message': 'Logs cleared successfully'})


@app.route('/api/audit/logs', methods=['GET'])
@login_required
@admin_scope_required('view_admin_audit')
def get_admin_audit_logs():
    action_filter = str(request.args.get('action', 'all') or 'all').strip().lower()
    entity_filter = str(request.args.get('entity', 'all') or 'all').strip().lower()
    actor_filter = str(request.args.get('actor', 'all') or 'all').strip().lower()
    status_filter = str(request.args.get('status', 'all') or 'all').strip().lower()
    text_filter = str(request.args.get('q', '') or '').strip().lower()
    try:
        limit = int(request.args.get('limit', 200))
    except (TypeError, ValueError):
        limit = 200
    limit = max(20, min(limit, 2000))

    filtered, source_entries = _filter_admin_audit_entries(
        action_filter=action_filter,
        entity_filter=entity_filter,
        actor_filter=actor_filter,
        status_filter=status_filter,
        text_filter=text_filter,
    )
    with _admin_audit_lock:
        total = len(admin_audit_entries)
    filtered_total = len(filtered)
    recent = filtered[-limit:] if filtered_total > limit else filtered
    recent = list(reversed(recent))

    actions = sorted({str(item.get('action', '')).strip() for item in source_entries if str(item.get('action', '')).strip()})
    entities = sorted({str(item.get('entity', '')).strip() for item in source_entries if str(item.get('entity', '')).strip()})
    actors = sorted({str(item.get('actor', '')).strip() for item in source_entries if str(item.get('actor', '')).strip()})

    status_counts = {'success': 0, 'warning': 0, 'error': 0}
    for item in filtered:
        normalized = str(item.get('status', 'success')).strip().lower()
        if normalized in status_counts:
            status_counts[normalized] += 1
        elif normalized:
            status_counts[normalized] = status_counts.get(normalized, 0) + 1

    return jsonify({
        'success': True,
        'entries': recent,
        'total': total,
        'filtered_total': filtered_total,
        'shown': len(recent),
        'actions': actions,
        'entities': entities,
        'actors': actors,
        'status_counts': status_counts,
        'source': 'memory+file',
    })


def _filter_admin_audit_entries(action_filter='all', entity_filter='all', actor_filter='all', status_filter='all', text_filter=''):
    with _admin_audit_lock:
        all_entries = list(admin_audit_entries)
    filtered = list(all_entries)

    if action_filter != 'all':
        filtered = [item for item in filtered if str(item.get('action', '')).strip().lower() == action_filter]
    if entity_filter != 'all':
        filtered = [item for item in filtered if str(item.get('entity', '')).strip().lower() == entity_filter]
    if actor_filter != 'all':
        filtered = [item for item in filtered if str(item.get('actor', '')).strip().lower() == actor_filter]
    if status_filter != 'all':
        filtered = [item for item in filtered if str(item.get('status', '')).strip().lower() == status_filter]
    if text_filter:
        text_filter = str(text_filter).strip().lower()

        def _matches_text(entry):
            blob = " ".join([
                str(entry.get('action', '')),
                str(entry.get('entity', '')),
                str(entry.get('target_id', '')),
                str(entry.get('actor', '')),
                str(entry.get('status', '')),
                str(entry.get('path', '')),
                json.dumps(entry.get('details', {}), ensure_ascii=True),
            ]).lower()
            return text_filter in blob

        filtered = [item for item in filtered if _matches_text(item)]
    return filtered, all_entries


@app.route('/api/audit/logs/export', methods=['GET'])
@login_required
@admin_scope_required('export_admin_audit')
def export_admin_audit_logs():
    action_filter = str(request.args.get('action', 'all') or 'all').strip().lower()
    entity_filter = str(request.args.get('entity', 'all') or 'all').strip().lower()
    actor_filter = str(request.args.get('actor', 'all') or 'all').strip().lower()
    status_filter = str(request.args.get('status', 'all') or 'all').strip().lower()
    text_filter = str(request.args.get('q', '') or '').strip().lower()
    export_format = str(request.args.get('format', 'json') or 'json').strip().lower()
    if export_format not in {'json', 'csv'}:
        export_format = 'json'

    try:
        requested_limit = int(request.args.get('limit', 5000))
    except (TypeError, ValueError):
        requested_limit = 5000
    max_rows = max(100, int(getattr(bssci_config, "ADMIN_AUDIT_EXPORT_MAX_ROWS", 50000) or 50000))
    limit = max(1, min(requested_limit, max_rows))

    filtered, _ = _filter_admin_audit_entries(
        action_filter=action_filter,
        entity_filter=entity_filter,
        actor_filter=actor_filter,
        status_filter=status_filter,
        text_filter=text_filter,
    )
    exported_rows = filtered[-limit:] if len(filtered) > limit else filtered
    exported_rows = list(reversed(exported_rows))

    _record_admin_audit(
        action='audit.export',
        entity='audit',
        target_id='admin_audit',
        status='success',
        details={
            'format': export_format,
            'limit': limit,
            'filtered_total': len(filtered),
            'exported_total': len(exported_rows),
            'filters': {
                'action': action_filter,
                'entity': entity_filter,
                'actor': actor_filter,
                'status': status_filter,
                'q': text_filter,
            },
        },
    )

    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if export_format == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'timestamp', 'action', 'entity', 'target_id', 'status',
            'actor', 'role', 'actor_tenant', 'active_tenant',
            'method', 'path', 'ip', 'details',
        ])
        for entry in exported_rows:
            writer.writerow([
                entry.get('timestamp', ''),
                entry.get('action', ''),
                entry.get('entity', ''),
                entry.get('target_id', ''),
                entry.get('status', ''),
                entry.get('actor', ''),
                entry.get('role', ''),
                entry.get('actor_tenant', ''),
                entry.get('active_tenant', ''),
                entry.get('method', ''),
                entry.get('path', ''),
                entry.get('ip', ''),
                json.dumps(entry.get('details', {}), ensure_ascii=True),
            ])
        content = output.getvalue()
        filename = f'admin-audit-{stamp}.csv'
        return Response(
            content,
            mimetype='text/csv;charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename={filename}'},
        )

    payload = {
        'exported_at': datetime.now(timezone.utc).isoformat(),
        'filters': {
            'action': action_filter,
            'entity': entity_filter,
            'actor': actor_filter,
            'status': status_filter,
            'q': text_filter,
        },
        'total_filtered': len(filtered),
        'exported': len(exported_rows),
        'entries': exported_rows,
    }
    filename = f'admin-audit-{stamp}.json'
    return Response(
        json.dumps(payload, indent=2, ensure_ascii=True),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


@app.route('/api/audit/logs/clear', methods=['POST'])
@login_required
@admin_scope_required('clear_admin_audit')
def clear_admin_audit_logs():
    global admin_audit_entries
    with _admin_audit_lock:
        admin_audit_entries = []
        try:
            os.makedirs(os.path.dirname(admin_audit_log_file), exist_ok=True)
            with open(admin_audit_log_file, "w", encoding="utf-8") as f:
                f.write("")
        except Exception as exc:
            return jsonify({'success': False, 'message': f'Failed to clear admin audit log: {exc}'}), 500
    return jsonify({'success': True, 'message': 'Admin audit log cleared successfully'})

@app.route('/api/bssci/status')
@app.route('/api/service/status')  # Support both endpoints for compatibility
@login_required
def bssci_status():
    try:
        status = get_bssci_service_status()
        return jsonify(status)
    except Exception as e:
        app.logger.error(f"Error in bssci_status endpoint: {e}")
        error_response = {
            'running': False,
            'error': f'Service status error: {str(e)}',
            'service_type': 'web_ui',
            'tls_server': {'active': False},
            'mqtt_broker': {'active': False},
            'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []},
            'total_sensors': 0,
            'registered_sensors': 0,
            'pending_requests': 0
        }
        return jsonify(error_response), 500

@app.route('/api/base_stations')
@login_required
def api_base_stations():
    """Get base stations for coverage map (tenant-aware, normalized and deduplicated)."""
    try:
        global tls_server_instance

        active_tenant = _active_tenant_id()
        _sync_coverage_positions_to_inventory(tenant_id=active_tenant, only_missing=True)
        raw_bs_config = load_base_station_config().get("base_stations", {})
        bs_config = _filter_base_stations_for_tenant(raw_bs_config, tenant_id=active_tenant)

        # Normalize configured entries by EUI to prevent duplicate records caused by case/format drift.
        normalized_config = {}
        for raw_eui, raw_config in (bs_config or {}).items():
            eui_upper = _normalize_eui_upper(raw_eui)
            if not eui_upper:
                continue

            config = dict(raw_config) if isinstance(raw_config, dict) else {}
            existing = normalized_config.get(eui_upper)
            if existing is None:
                normalized_config[eui_upper] = config
                continue

            # Merge duplicate definitions and keep the richer one.
            merged = dict(existing)
            merged.update({k: v for k, v in config.items() if v not in (None, "", [], {})})

            existing_has_gps = existing.get("gps_lat") is not None and existing.get("gps_lng") is not None
            config_has_gps = config.get("gps_lat") is not None and config.get("gps_lng") is not None
            if config_has_gps and not existing_has_gps:
                merged["gps_lat"] = config.get("gps_lat")
                merged["gps_lng"] = config.get("gps_lng")

            if not merged.get("name"):
                merged["name"] = config.get("name") or existing.get("name") or eui_upper[:8]

            normalized_config[eui_upper] = merged

        # Build owner map from full config to avoid leaking connected stations from other tenants.
        eui_owner_tenant = {}
        for owner_eui, owner_cfg in (raw_bs_config or {}).items():
            owner_upper = _normalize_eui_upper(owner_eui)
            if not owner_upper:
                continue
            eui_owner_tenant[owner_upper] = _tenant_id_from_base_station(owner_cfg)

        connected_euis = set()
        if tls_server_instance and hasattr(tls_server_instance, 'connected_base_stations'):
            for _, bs_eui in (getattr(tls_server_instance, 'connected_base_stations', {}) or {}).items():
                bs_upper = _normalize_eui_upper(bs_eui)
                if bs_upper:
                    connected_euis.add(bs_upper)

        base_stations = []
        for eui_upper, config in normalized_config.items():
            bs_tenant = _normalize_tenant_id(
                (config or {}).get('tenant_id'),
                fallback=active_tenant,
            )
            base_stations.append({
                'eui': eui_upper,
                'EUI': eui_upper,
                'name': (config or {}).get('name', eui_upper[:8]),
                'connected': eui_upper in connected_euis,
                'gps_lat': (config or {}).get('gps_lat'),
                'gps_lng': (config or {}).get('gps_lng'),
                'tenant_id': bs_tenant,
            })

        # Include connected stations that are not configured in current tenant inventory.
        for eui_upper in sorted(connected_euis):
            if eui_upper in normalized_config:
                continue
            owner_tenant = eui_owner_tenant.get(eui_upper)
            if owner_tenant and not _tenant_matches(owner_tenant, active_tenant):
                continue
            base_stations.append({
                'eui': eui_upper,
                'EUI': eui_upper,
                'name': eui_upper[:8],
                'connected': True,
                'gps_lat': None,
                'gps_lng': None,
                'tenant_id': active_tenant,
            })

        base_stations.sort(key=lambda item: (not bool(item.get('connected')), item.get('name', ''), item.get('eui', '')))
        return jsonify({'base_stations': base_stations})
    except Exception as e:
        return jsonify({'base_stations': [], 'error': str(e)})

@app.route('/api/base_stations/status')
def get_base_stations_status():
    """Get status of connected base stations - thread-safe version (legacy endpoint)"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance

        if not tls_server:
            return jsonify({
                "connected": [],
                "connecting": [],
                "total_connected": 0,
                "total_connecting": 0,
                "error": "TLS server not initialized"
            }), 503

        connected_stations = []
        connecting_stations = []
        
        try:
            if hasattr(tls_server, 'connected_base_stations'):
                connected_dict = getattr(tls_server, 'connected_base_stations', {})
                for writer, bs_eui in list(connected_dict.items()):
                    try:
                        connected_stations.append({
                            "eui": bs_eui,
                            "address": "connected",
                            "status": "connected"
                        })
                    except Exception as e:
                        print(f"Error processing connected station {bs_eui}: {e}")
            
            if hasattr(tls_server, 'connecting_base_stations'):
                connecting_dict = getattr(tls_server, 'connecting_base_stations', {})
                for writer, bs_eui in list(connecting_dict.items()):
                    try:
                        connecting_stations.append({
                            "eui": bs_eui,
                            "address": "connecting",
                            "status": "connecting"
                        })
                    except Exception as e:
                        print(f"Error processing connecting station {bs_eui}: {e}")
                        
        except Exception as e:
            print(f"Error accessing base station collections: {e}")

        return jsonify({
            "connected": connected_stations,
            "connecting": connecting_stations,
            "total_connected": len(connected_stations),
            "total_connecting": len(connecting_stations)
        })
            
    except Exception as e:
        print(f"Error in get_base_stations_status endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "connected": [],
            "connecting": [],
            "total_connected": 0,
            "total_connecting": 0,
            "error": f"Base stations error: {str(e)}"
        })

# ==================== Variable MAC (VM) Sub-Channel API ====================

@app.route('/api/vm/status')
@login_required
def get_vm_status():
    """Get VM sub-channel status for all sensors"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_vm_status'):
            status = tls_server_instance.get_vm_status()
            return jsonify({'success': True, **status})
        return jsonify({'success': False, 'message': 'TLS server not available', 'active_sensors': {}})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/activate', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_activate():
    """Activate VM sub-channel reception on VM-capable base stations only
    
    Per BSSCI VM specification, this sends vm.activate with macType parameter.
    Only sends to base stations that have been confirmed as VM-capable.
    """
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.get_json(silent=True) or {}
        mac_type = data.get('macType', 0)
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_activate(mac_type, only_vm_capable=True))
        finally:
            loop.close()
        
        if success:
            return jsonify({'success': True, 'message': f'VM activate sent to VM-capable base stations (macType={mac_type})'})
        else:
            return jsonify({'success': False, 'message': 'No VM-capable base stations found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/deactivate', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_deactivate():
    """Deactivate VM sub-channel reception on VM-capable base stations only
    
    Per BSSCI VM specification, this sends vm.deactivate with macType parameter.
    Only sends to base stations that have been confirmed as VM-capable.
    """
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.get_json(silent=True) or {}
        mac_type = data.get('macType', 0)
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_deactivate(mac_type, only_vm_capable=True))
        finally:
            loop.close()
        
        if success:
            return jsonify({'success': True, 'message': f'VM deactivate sent to VM-capable base stations (macType={mac_type})'})
        else:
            return jsonify({'success': False, 'message': 'No VM-capable base stations found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/status', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_query_status():
    """Query VM sub-channel status - returns list of activated macTypes
    
    Per BSSCI VM specification, this sends vm.status to VM-capable base stations.
    Use discover=true to query ALL base stations (for initial VM capability detection).
    """
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.get_json(silent=True) or {}
        discover = data.get('discover', False)  # If true, query ALL base stations
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_status(only_vm_capable=not discover))
        finally:
            loop.close()
        
        if success:
            if discover:
                return jsonify({'success': True, 'message': 'VM status query sent to ALL base stations (discovery mode)'})
            else:
                return jsonify({'success': True, 'message': 'VM status query sent to VM-capable base stations'})
        else:
            if discover:
                return jsonify({'success': False, 'message': 'No base stations connected'})
            else:
                return jsonify({'success': False, 'message': 'No VM-capable base stations found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/log')
@login_required
def get_vm_log():
    """Get VM operation log entries"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'vm_log'):
            return jsonify({
                'success': True,
                'log': tls_server_instance.vm_log[-50:]
            })
        return jsonify({'success': True, 'log': []})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/capable')
@login_required
def get_vm_capable_base_stations():
    """Get list of base stations that support VM (Variable MAC)"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_vm_capable_base_stations'):
            vm_capable = tls_server_instance.get_vm_capable_base_stations()
            connected = list(tls_server_instance.connected_base_stations.values()) if hasattr(tls_server_instance, 'connected_base_stations') else []
            return jsonify({
                'success': True,
                'vm_capable': vm_capable,
                'total_connected': len(connected),
                'connected_base_stations': connected
            })
        return jsonify({
            'success': True,
            'vm_capable': [],
            'total_connected': 0,
            'connected_base_stations': []
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/send/<eui>', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_send_data_to_sensor(eui):
    """Send data to sensor via VM sub-channel (downlink)"""
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.json
        if not data or 'data' not in data:
            return jsonify({'success': False, 'message': 'Missing data field'}), 400
        
        payload = bytes.fromhex(data['data']) if isinstance(data['data'], str) else bytes(data['data'])
        port = data.get('port', 1)
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_send_data(eui, payload, port))
        finally:
            loop.close()
        
        if success:
            return jsonify({'success': True, 'message': f'VM downlink data sent to sensor {eui}'})
        else:
            return jsonify({'success': False, 'message': 'Failed to send VM data - VM may not be active for this sensor'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/certificates/status')
@login_required
@permission_required('can_manage_certificates')
def get_certificate_status():
    """Get status of SSL certificates"""
    import os
    from datetime import datetime
    try:
        cert_files = {
            'ca': 'certs/ca_cert.pem',
            'service': 'certs/service_center_cert.pem',
            'key': 'certs/service_center_key.pem'
        }

        status = {'certificates': {}}

        for cert_type, file_path in cert_files.items():
            if os.path.exists(file_path):
                status['certificates'][cert_type] = True
                # Try to get certificate expiry date
                try:
                    if cert_type != 'key':  # Don't try to parse private key as certificate
                        import ssl
                        import socket
                        from cryptography import x509
                        from cryptography.hazmat.backends import default_backend

                        with open(file_path, 'rb') as f:
                            cert_data = f.read()
                            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
                            expiry = cert.not_valid_after
                            status['certificates'][f'{cert_type}_expires'] = expiry.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass  # If we can't read the certificate, just mark as present
            else:
                status['certificates'][cert_type] = False

        return jsonify({'success': True, 'certificates': status['certificates']})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/download/<filename>')
@login_required
@permission_required('can_manage_certificates')
def download_certificate(filename):
    """Download a certificate file"""
    import os
    from flask import send_file, abort

    # Security: only allow specific certificate files
    allowed_files = ['ca_cert.pem', 'service_center_cert.pem', 'service_center_key.pem']
    if filename not in allowed_files:
        abort(404)

    file_path = os.path.join('certs', filename)
    if not os.path.exists(file_path):
        abort(404)

    return send_file(file_path, as_attachment=True, download_name=filename)

@app.route('/api/certificates/upload/<cert_type>', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def upload_certificate(cert_type):
    """Upload a new certificate"""
    import os
    from werkzeug.utils import secure_filename

    if 'certificate' not in request.files:
        return jsonify({'success': False, 'message': 'No file provided'})

    file = request.files['certificate']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'})

    # Map cert types to filenames
    cert_mapping = {
        'ca': 'ca_cert.pem',
        'service': 'service_center_cert.pem',
        'key': 'service_center_key.pem'
    }

    if cert_type not in cert_mapping:
        return jsonify({'success': False, 'message': 'Invalid certificate type'})

    try:
        # Ensure certs directory exists
        os.makedirs('certs', exist_ok=True)

        # Backup existing file
        target_file = os.path.join('certs', cert_mapping[cert_type])
        if os.path.exists(target_file):
            backup_file = target_file + '.backup'
            os.rename(target_file, backup_file)

        # Save new file
        file.save(target_file)

        return jsonify({'success': True, 'message': f'{cert_type.upper()} certificate uploaded successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/generate', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def generate_certificates():
    """Generate new SSL certificates"""
    import os
    import subprocess

    try:
        # Ensure certs directory exists
        os.makedirs('certs', exist_ok=True)

        # Generate new certificates using OpenSSL with static, validated commands
        import shlex

        # Execute certificate generation commands with completely static strings

        # Generate CA private key
        result = subprocess.run(['openssl', 'genrsa', '-out', 'certs/ca_key.pem', '2048'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'CA key generation failed: {result.stderr}'})

        # Generate CA certificate
        result = subprocess.run(['openssl', 'req', '-new', '-x509', '-key', 'certs/ca_key.pem', '-out', 'certs/ca_cert.pem', '-days', '365', '-subj', '/C=US/ST=State/L=City/O=BSSCI/CN=BSSCI-CA'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'CA certificate generation failed: {result.stderr}'})

        # Generate service private key
        result = subprocess.run(['openssl', 'genrsa', '-out', 'certs/service_center_key.pem', '2048'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'Service key generation failed: {result.stderr}'})

        # Generate service certificate request
        result = subprocess.run(['openssl', 'req', '-new', '-key', 'certs/service_center_key.pem', '-out', 'certs/service_center.csr', '-subj', '/C=US/ST=State/L=City/O=BSSCI/CN=bssci-service'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'Service certificate request generation failed: {result.stderr}'})

        # Sign service certificate with CA
        result = subprocess.run(['openssl', 'x509', '-req', '-in', 'certs/service_center.csr', '-CA', 'certs/ca_cert.pem', '-CAkey', 'certs/ca_key.pem', '-CAcreateserial', '-out', 'certs/service_center_cert.pem', '-days', '365'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'Service certificate signing failed: {result.stderr}'})

        # Clean up temporary files
        temp_files = ['certs/service_center.csr', 'certs/ca_cert.srl']
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

        return jsonify({'success': True, 'message': 'New certificates generated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/backup')
@login_required
@permission_required('can_manage_certificates')
def backup_certificates():
    """Download all certificates as ZIP"""
    import os
    import tempfile
    import zipfile
    from flask import send_file

    try:
        # Create temporary ZIP file
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')

        with zipfile.ZipFile(temp_zip.name, 'w') as zipf:
            cert_files = ['ca_cert.pem', 'service_center_cert.pem', 'service_center_key.pem']
            for cert_file in cert_files:
                file_path = os.path.join('certs', cert_file)
                if os.path.exists(file_path):
                    zipf.write(file_path, cert_file)

        return send_file(temp_zip.name, as_attachment=True, download_name='bssci_certificates_backup.zip')
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/restore', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def restore_certificates():
    """Restore certificates from ZIP backup"""
    import os
    import tempfile
    import zipfile

    if 'backup' not in request.files:
        return jsonify({'success': False, 'message': 'No backup file provided'})

    file = request.files['backup']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'})

    try:
        # Save uploaded ZIP to temporary location
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        file.save(temp_zip.name)

        # Extract certificates
        with zipfile.ZipFile(temp_zip.name, 'r') as zipf:
            # Ensure certs directory exists
            os.makedirs('certs', exist_ok=True)

            # Extract only certificate files
            cert_files = ['ca_cert.pem', 'service_center_cert.pem', 'service_center_key.pem']
            for cert_file in cert_files:
                if cert_file in zipf.namelist():
                    target_path = os.path.join('certs', cert_file)
                    # Backup existing file
                    if os.path.exists(target_path):
                        os.rename(target_path, target_path + '.backup')
                    # Extract new file
                    zipf.extract(cert_file, 'certs')

        # Clean up temporary file
        os.unlink(temp_zip.name)

        return jsonify({'success': True, 'message': 'Certificates restored successfully from backup'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/base-stations/<eui>/certificate/generate', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def generate_bs_certificate(eui):
    """Generate certificate pair for specific base station"""
    try:
        eui = eui.lower()
        if not _validate_eui(eui):
            return jsonify({'success': False, 'message': 'Invalid EUI format'}), 400
        config = load_base_station_config()
        if eui not in config.get("base_stations", {}):
            return jsonify({'success': False, 'message': 'Base station not found'}), 404
        success, msg = _generate_bs_certificate(eui)
        if success:
            return jsonify({'success': True, 'message': msg, 'download_url': f'/api/base-stations/{eui}/certificate/download'})
        else:
            return jsonify({'success': False, 'message': msg}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/base-stations/<eui>/certificate/download')
@login_required
@permission_required('can_manage_certificates')
def download_bs_certificate(eui):
    """Download ZIP with CA cert + BS cert + BS key"""
    try:
        eui = eui.lower()
        if not _validate_eui(eui):
            return jsonify({'success': False, 'message': 'Invalid EUI format'}), 400
        bs_cert_dir = os.path.join('certs', f'bs_{eui}')
        cert_path = os.path.join(bs_cert_dir, f'{eui}_cert.pem')
        key_path = os.path.join(bs_cert_dir, f'{eui}_key.pem')
        ca_path = 'certs/ca_cert.pem'
        if not os.path.exists(cert_path) or not os.path.exists(key_path):
            return jsonify({'success': False, 'message': 'Certificate not found for this base station'}), 404
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        with zipfile.ZipFile(temp_zip.name, 'w') as zipf:
            if os.path.exists(ca_path):
                zipf.write(ca_path, 'ca_cert.pem')
            zipf.write(cert_path, f'{eui}_cert.pem')
            zipf.write(key_path, f'{eui}_key.pem')
        return send_file(temp_zip.name, as_attachment=True, download_name=f'bs_{eui}_certificates.zip')
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/container/restart', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def restart_container():
    """Force restart the entire container"""
    import subprocess
    import threading
    import time
    import os

    def container_restart_in_background():
        """Perform container restart in a separate thread"""
        try:
            time.sleep(1)  # Small delay to allow response to be sent
            
            logger.info("Forcing container restart")
            try:
                # Send SIGTERM to PID 1 (init process) to restart the container
                subprocess.run(['kill', '-TERM', '1'], check=False, timeout=5)
            except Exception as e:
                logger.error(f"Container restart failed: {e}")
                # Fallback: exit the main process which should cause container restart
                os._exit(0)
                
        except Exception as e:
            logger.error(f"Error during container restart: {e}")
            os._exit(1)

    try:
        # Start restart in background thread
        restart_thread = threading.Thread(target=container_restart_in_background)
        restart_thread.daemon = True
        restart_thread.start()
        
        return jsonify({'success': True, 'message': 'Container restart initiated. The container will restart completely to reload all environment variables.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/service/restart', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def restart_service():
    """Restart the BSSCI service with full environment reload"""
    import subprocess
    import threading
    import time
    import os

    def restart_in_background():
        """Perform the restart operation in a separate thread"""
        try:
            time.sleep(1)  # Small delay to allow response to be sent

            # Check if we're running in Docker
            is_docker = os.path.exists('/.dockerenv') or os.getenv('CONTAINER') == '1'
            
            # Check environment type
            is_replit = os.getenv('REPLIT_ENVIRONMENT') or os.getenv('REPL_SLUG')
            
            if is_replit:
                # In Replit, workflows auto-restart when the process exits
                logger.info("Replit environment detected - restarting via process exit")
                _restart_processes()
            elif is_docker:
                # In Docker (including Synology), use process exit with Docker restart policy
                # This avoids using kill/pkill commands that may not be available
                logger.info("Docker environment detected - restarting via process exit")
                logger.info("Docker restart policy will automatically restart the container")
                _restart_processes()
            else:
                # In regular environment without Docker
                logger.info("Regular environment detected - attempting process restart")
                _restart_processes()
                
        except Exception as e:
            logger.error(f"Error during restart: {e}")
            # Fallback to basic process restart
            _restart_processes()

    def _restart_processes():
        """Restart Python processes (Docker-compatible version without kill/pkill)"""
        try:
            logger.info("Initiating service restart for Docker environment...")
            
            # Give time for the response to be sent before restarting
            time.sleep(2)
            
            # In Docker with restart policy, we can simply exit and let Docker restart us
            # This works for Synology Docker and other containerized environments
            logger.info("Exiting process - Docker will restart automatically")
            
            # Use os._exit to bypass cleanup handlers and exit immediately
            import os
            os._exit(0)
            
        except Exception as e:
            logger.error(f"Error during process exit: {e}")
            # Fallback: try standard exit
            try:
                import sys
                sys.exit(0)
            except:
                # Last resort: force exit
                import os
                os._exit(1)

    try:
        # Start restart in background thread
        restart_thread = threading.Thread(target=restart_in_background)
        restart_thread.daemon = True
        restart_thread.start()

        return jsonify({'success': True, 'message': 'Service restart initiated. In Docker environments, the entire container will restart to reload environment variables.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

