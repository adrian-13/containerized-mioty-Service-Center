from __future__ import annotations

import json
import math
import os
import sys
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Host-side execution needs the published Timescale port instead of the Docker
# service DNS name used inside the app container.
os.environ["TIMESCALE_ENABLED"] = "true"
os.environ.setdefault("TIMESCALE_HOST", "localhost")

import web_ui  # noqa: E402


TENANT_ID = "test"
LEGACY_TENANT_IDS = {"kinet"}
DEMO_MARKER = "viewer_demo_test"
DEMO_TAG = "demo"
BASE_TIME = datetime.now(timezone.utc).replace(second=0, microsecond=0)


@dataclass(frozen=True)
class DemoSensor:
    eui: str
    name: str
    profile: str
    gps_lat: Optional[float]
    gps_lng: Optional[float]
    marker_color: str
    base_station_eui: str
    created_offset_hours: float
    tags: tuple[str, ...] = ()


BASE_STATIONS = [
    {
        "eui": "129AF3FFFE01F124",
        "name": "Test HQ Strecha",
        "ip": "192.168.10.254",
        "gps_lat": 48.767193,
        "gps_lng": 18.636599,
    },
    {
        "eui": "129A13FFFE01F124",
        "name": "Test Park Juh",
        "ip": "192.168.10.253",
        "gps_lat": 48.765942,
        "gps_lng": 18.638122,
    },
    {
        "eui": "129AB3FFFE01F124",
        "name": "Test Sklad",
        "ip": "192.168.10.252",
        "gps_lat": 48.768141,
        "gps_lng": 18.633522,
    },
]


DEMO_SENSORS = [
    DemoSensor(
        eui="A0412D2A10000101",
        name="CO2 Recepcia",
        profile="lansen_e2_co2_indoor",
        gps_lat=48.767061,
        gps_lng=18.636184,
        marker_color="#16a34a",
        base_station_eui="129AF3FFFE01F124",
        created_offset_hours=72,
        tags=("Lansen", DEMO_TAG, "recepcia"),
    ),
    DemoSensor(
        eui="A0412D2A10000102",
        name="CO2 Zasadacka",
        profile="lansen_e2_co2_indoor",
        gps_lat=48.767406,
        gps_lng=18.636019,
        marker_color="#dc2626",
        base_station_eui="129AF3FFFE01F124",
        created_offset_hours=72,
        tags=("Lansen", DEMO_TAG, "meeting-room"),
    ),
    DemoSensor(
        eui="A0412D2A10000103",
        name="CO2 Sklad",
        profile="lansen_e2_co2_auto",
        gps_lat=48.767745,
        gps_lng=18.634816,
        marker_color="#f59e0b",
        base_station_eui="129AB3FFFE01F124",
        created_offset_hours=120,
        tags=("Lansen", DEMO_TAG, "sklad"),
    ),
    DemoSensor(
        eui="A0412D1D10000104",
        name="Dvere Hlavny vstup",
        profile="lansen_m2_contact",
        gps_lat=48.766873,
        gps_lng=18.636764,
        marker_color="#0ea5e9",
        base_station_eui="129A13FFFE01F124",
        created_offset_hours=168,
        tags=("Lansen", DEMO_TAG, "door"),
    ),
    DemoSensor(
        eui="A0412D1D10000105",
        name="Nudzovy vychod",
        profile="lansen_m2_contact",
        gps_lat=48.767948,
        gps_lng=18.635562,
        marker_color="#b45309",
        base_station_eui="129AB3FFFE01F124",
        created_offset_hours=240,
        tags=("Lansen", DEMO_TAG, "exit"),
    ),
    DemoSensor(
        eui="A0412D2A10000106",
        name="CO2 Nova kancelaria",
        profile="lansen_e2_co2_indoor",
        gps_lat=48.766551,
        gps_lng=18.635949,
        marker_color="#64748b",
        base_station_eui="129AF3FFFE01F124",
        created_offset_hours=0.2,
        tags=("Lansen", DEMO_TAG, "new"),
    ),
]


def dt_hours_ago(hours: float) -> datetime:
    return BASE_TIME - timedelta(hours=hours)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def clamp(value: int, lower: int, upper: int) -> int:
    return max(lower, min(value, upper))


def bits_to_bytes(bit_string: str) -> List[int]:
    if len(bit_string) % 8 != 0:
        raise ValueError("bit string length must be a multiple of 8")
    return [int(bit_string[idx:idx + 8], 2) for idx in range(0, len(bit_string), 8)]


def pack_bit_fields(fields: Iterable[tuple[int, int]]) -> List[int]:
    bits = "".join(f"{clamp(int(value), 0, (1 << width) - 1):0{width}b}" for value, width in fields)
    return bits_to_bytes(bits)


def encode_co2_payload(
    *,
    temperature_1_c: float,
    humidity_1_pct: int,
    co2_1_ppm: int,
    temperature_2_c: float,
    humidity_2_pct: int,
    co2_2_ppm: int,
    battery_v_est: float,
    co2_last_calibration_ppm: int,
    days_to_next_calibration: int,
    calibration_not_done: bool = False,
    co2_error: bool = False,
) -> List[int]:
    return pack_bit_fields(
        [
            (round((temperature_1_c + 10.0) / 0.125), 9),
            (humidity_1_pct, 7),
            (round(co2_1_ppm / 20.0), 8),
            (0, 6),
            (round((temperature_2_c + 10.0) / 0.125), 9),
            (humidity_2_pct, 7),
            (round(co2_2_ppm / 20.0), 8),
            (0, 6),
            (round(battery_v_est / 0.1), 5),
            (round(co2_last_calibration_ppm / 20.0), 8),
            (days_to_next_calibration, 5),
            (1 if calibration_not_done else 0, 1),
            (1 if co2_error else 0, 1),
        ]
    )


def encode_m2_payload(
    *,
    total_openings: int,
    internal_magnet_alarm: bool,
    external_alarm: bool,
    internal_magnet_alarm_last_5min: bool,
    internal_magnet_alarm_last_10min: bool,
    internal_magnet_alarm_last_1h: bool,
    internal_magnet_alarm_last_24h: bool,
    external_alarm_last_5min: bool,
    external_alarm_last_10min: bool,
    external_alarm_last_1h: bool,
    external_alarm_last_24h: bool,
    minutes_since_last_alarm: int,
    duration_last_alarm_minutes: int,
    last_alarm_input_external: bool,
    operating_years: int,
    runtime_years: int,
    battery_mv: int,
    low_batt: bool = False,
    sabotage_internal: bool = False,
    sabotage_external: bool = False,
    async_message: bool = False,
) -> List[int]:
    battery_raw = round((battery_mv - 1800) / 100.0)
    return pack_bit_fields(
        [
            (total_openings, 20),
            (1 if internal_magnet_alarm else 0, 1),
            (1 if external_alarm else 0, 1),
            (1 if internal_magnet_alarm_last_5min else 0, 1),
            (1 if internal_magnet_alarm_last_10min else 0, 1),
            (1 if internal_magnet_alarm_last_1h else 0, 1),
            (1 if internal_magnet_alarm_last_24h else 0, 1),
            (1 if external_alarm_last_5min else 0, 1),
            (1 if external_alarm_last_10min else 0, 1),
            (1 if external_alarm_last_1h else 0, 1),
            (1 if external_alarm_last_24h else 0, 1),
            (minutes_since_last_alarm, 18),
            (duration_last_alarm_minutes, 13),
            (1 if last_alarm_input_external else 0, 1),
            (operating_years, 5),
            (runtime_years, 5),
            (battery_raw, 4),
            (1 if low_batt else 0, 1),
            (1 if sabotage_internal else 0, 1),
            (1 if sabotage_external else 0, 1),
            (1 if async_message else 0, 1),
        ]
    )


def build_sensor_payload(sensor: DemoSensor) -> Dict[str, Any]:
    payload = {
        "eui": sensor.eui,
        "nwKey": uuid.uuid5(uuid.NAMESPACE_DNS, f"{TENANT_ID}:{sensor.eui}").hex.upper(),
        "shortAddr": sensor.eui[-4:],
        "bidi": False,
        "name": sensor.name,
        "tags": list(sensor.tags),
        "gps_lat": sensor.gps_lat,
        "gps_lng": sensor.gps_lng,
        "tenant_id": TENANT_ID,
        "shared_tenants": [],
        "marker_color": sensor.marker_color,
        "preferredDownlinkPath": {
            "baseStation": sensor.base_station_eui,
            "snr": 18.2,
            "lastUpdated": dt_hours_ago(1).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "messageCount": 24,
        },
        "attached_base_stations": [sensor.base_station_eui],
        "sensor_profile": sensor.profile,
        "created_at": iso(dt_hours_ago(sensor.created_offset_hours)),
        "demo_seed": DEMO_MARKER,
    }
    payload = web_ui._apply_sensor_profile_defaults(payload)
    return payload


def build_alerts() -> List[Dict[str, Any]]:
    by_name = {sensor.name: sensor for sensor in DEMO_SENSORS}
    created_at = iso(dt_hours_ago(12))
    return [
        {
            "id": f"{TENANT_ID}:rule:co2-critical-zasadacka",
            "tenant_id": TENANT_ID,
            "sensor_eui": by_name["CO2 Zasadacka"].eui,
            "kind": "threshold",
            "name": "CO2 Zasadacka > 1500 ppm",
            "metric": "co2_1_ppm",
            "condition": "gt",
            "threshold": 1500,
            "severity": "critical",
            "enabled": True,
            "created_at": created_at,
        },
        {
            "id": f"{TENANT_ID}:rule:door-alarm-hlavny-vstup",
            "tenant_id": TENANT_ID,
            "sensor_eui": by_name["Dvere Hlavny vstup"].eui,
            "kind": "threshold",
            "name": "Hlavny vstup alarm aktivny",
            "metric": "any_alarm_active",
            "condition": "eq",
            "threshold": 1,
            "severity": "critical",
            "enabled": True,
            "created_at": created_at,
        },
        {
            "id": f"{TENANT_ID}:rule:offline-sklad",
            "tenant_id": TENANT_ID,
            "sensor_eui": by_name["CO2 Sklad"].eui,
            "kind": "sensor_offline",
            "name": "CO2 Sklad bez dat",
            "metric": "",
            "condition": "",
            "threshold": None,
            "severity": "warning",
            "enabled": True,
            "created_at": created_at,
        },
        {
            "id": f"{TENANT_ID}:rule:offline-nudzovy-vychod",
            "tenant_id": TENANT_ID,
            "sensor_eui": by_name["Nudzovy vychod"].eui,
            "kind": "sensor_offline",
            "name": "Nudzovy vychod bez eventu",
            "metric": "",
            "condition": "",
            "threshold": None,
            "severity": "critical",
            "enabled": True,
            "created_at": created_at,
        },
    ]


def build_demo_telemetry(sensor_by_eui: Dict[str, DemoSensor]) -> Dict[str, List[Dict[str, Any]]]:
    online = sensor_by_eui["A0412D2A10000101"]
    critical = sensor_by_eui["A0412D2A10000102"]
    warning = sensor_by_eui["A0412D2A10000103"]
    recent_door = sensor_by_eui["A0412D1D10000104"]
    stale_door = sensor_by_eui["A0412D1D10000105"]

    rows: Dict[str, List[Dict[str, Any]]] = {sensor.eui: [] for sensor in sensor_by_eui.values()}

    for idx in range(24):
        ts = BASE_TIME - timedelta(minutes=idx * 6)
        rows[online.eui].append(
            telemetry_row(
                online,
                ts=ts,
                packet_cnt=500 + idx,
                snr=16.8 + (idx % 3) * 0.4,
                rssi=-79 + (idx % 4),
                payload=encode_co2_payload(
                    temperature_1_c=22.4 + (idx % 3) * 0.2,
                    humidity_1_pct=44 + (idx % 4),
                    co2_1_ppm=720 + idx * 3,
                    temperature_2_c=22.0 + (idx % 2) * 0.2,
                    humidity_2_pct=42 + (idx % 3),
                    co2_2_ppm=700 + idx * 2,
                    battery_v_est=2.9,
                    co2_last_calibration_ppm=420,
                    days_to_next_calibration=18,
                ),
            )
        )

    for idx in range(18):
        ts = BASE_TIME - timedelta(minutes=idx * 6)
        rows[critical.eui].append(
            telemetry_row(
                critical,
                ts=ts,
                packet_cnt=820 + idx,
                snr=15.4 + (idx % 2) * 0.6,
                rssi=-82 + (idx % 3),
                payload=encode_co2_payload(
                    temperature_1_c=24.8 + (idx % 2) * 0.3,
                    humidity_1_pct=46 + (idx % 5),
                    co2_1_ppm=1760 + idx * 8,
                    temperature_2_c=24.2 + (idx % 3) * 0.2,
                    humidity_2_pct=45 + (idx % 4),
                    co2_2_ppm=1580 + idx * 6,
                    battery_v_est=2.8,
                    co2_last_calibration_ppm=440,
                    days_to_next_calibration=12,
                ),
            )
        )

    for idx in range(8):
        ts = BASE_TIME - timedelta(hours=2, minutes=idx * 7)
        rows[warning.eui].append(
            telemetry_row(
                warning,
                ts=ts,
                packet_cnt=310 + idx,
                snr=11.5 + (idx % 3) * 0.5,
                rssi=-88 + (idx % 4),
                payload=encode_co2_payload(
                    temperature_1_c=18.1 + (idx % 2) * 0.2,
                    humidity_1_pct=53 + (idx % 3),
                    co2_1_ppm=860 + idx * 4,
                    temperature_2_c=17.9 + (idx % 3) * 0.2,
                    humidity_2_pct=51 + (idx % 4),
                    co2_2_ppm=840 + idx * 3,
                    battery_v_est=2.7,
                    co2_last_calibration_ppm=420,
                    days_to_next_calibration=20,
                ),
            )
        )

    for idx in range(6):
        ts = BASE_TIME - timedelta(minutes=idx * 12)
        alarm_active = idx == 0
        rows[recent_door.eui].append(
            telemetry_row(
                recent_door,
                ts=ts,
                packet_cnt=190 + idx,
                snr=20.1 - idx * 0.2,
                rssi=-74 + (idx % 2),
                payload=encode_m2_payload(
                    total_openings=1200 + idx,
                    internal_magnet_alarm=False,
                    external_alarm=alarm_active,
                    internal_magnet_alarm_last_5min=False,
                    internal_magnet_alarm_last_10min=False,
                    internal_magnet_alarm_last_1h=False,
                    internal_magnet_alarm_last_24h=False,
                    external_alarm_last_5min=alarm_active,
                    external_alarm_last_10min=alarm_active,
                    external_alarm_last_1h=True,
                    external_alarm_last_24h=True,
                    minutes_since_last_alarm=2 if alarm_active else 18 + idx,
                    duration_last_alarm_minutes=4 if alarm_active else 1,
                    last_alarm_input_external=True,
                    operating_years=2,
                    runtime_years=2,
                    battery_mv=2800,
                    async_message=alarm_active,
                ),
            )
        )

    rows[stale_door.eui].append(
        telemetry_row(
            stale_door,
            ts=BASE_TIME - timedelta(days=10, hours=3),
            packet_cnt=75,
            snr=13.9,
            rssi=-86,
            payload=encode_m2_payload(
                total_openings=83,
                internal_magnet_alarm=False,
                external_alarm=False,
                internal_magnet_alarm_last_5min=False,
                internal_magnet_alarm_last_10min=False,
                internal_magnet_alarm_last_1h=False,
                internal_magnet_alarm_last_24h=False,
                external_alarm_last_5min=False,
                external_alarm_last_10min=False,
                external_alarm_last_1h=False,
                external_alarm_last_24h=False,
                minutes_since_last_alarm=340,
                duration_last_alarm_minutes=1,
                last_alarm_input_external=False,
                operating_years=4,
                runtime_years=4,
                battery_mv=2600,
            ),
        )
    )

    return rows


def telemetry_row(sensor: DemoSensor, *, ts: datetime, packet_cnt: int, snr: float, rssi: float, payload: List[int]) -> Dict[str, Any]:
    return {
        "tenant_id": TENANT_ID,
        "sensor_eui": sensor.eui.lower(),
        "base_station_eui": sensor.base_station_eui.lower(),
        "packet_cnt": packet_cnt,
        "snr": float(snr),
        "rssi": float(rssi),
        "msg_type": "ul",
        "ts": ts.astimezone(timezone.utc),
        "payload": {
            "data": payload,
            "rxTime": int(ts.timestamp() * 1_000_000_000),
        },
    }


def ensure_viewer_user() -> None:
    users_data = web_ui.load_users()
    users = users_data.setdefault("users", {})
    users["test"] = {
        "password": "test",
        "role": "viewer",
        "name": "test",
        "tenant_id": TENANT_ID,
    }
    if not web_ui.save_users(users_data):
        raise RuntimeError("Failed to save users.json")


def ensure_tenant_registry(conn: Any | None) -> None:
    registry = web_ui.load_tenant_registry()
    registry["tenants"] = [
        item
        for item in registry.get("tenants", [])
        if web_ui._normalize_tenant_id((item or {}).get("id"), fallback=web_ui._default_tenant_id()) not in LEGACY_TENANT_IDS
    ]
    web_ui.save_tenant_registry(registry)
    ok, err, _entry = web_ui._upsert_tenant_registry_entry(
        TENANT_ID,
        name="Testovaci tenant",
        description="Testovaci viewer tenant so seeded demo senzormi, alertmi a telemetriou.",
    )
    if not ok:
        raise RuntimeError(err or "Failed to upsert tenant registry entry")
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tenants (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id)
                DO UPDATE SET name = EXCLUDED.name
                """,
                (TENANT_ID, "Testovaci tenant"),
            )


def seed_base_stations() -> int:
    config = web_ui.load_base_station_config()
    existing = config.get("base_stations", {}) or {}
    next_map = {
        eui: data
        for eui, data in existing.items()
        if web_ui._tenant_id_from_base_station(data) not in (set(LEGACY_TENANT_IDS) | {TENANT_ID})
    }
    for station in BASE_STATIONS:
        next_map[station["eui"].lower()] = {
            "name": station["name"],
            "tags": [DEMO_TAG, "base-station"],
            "ip": station["ip"],
            "gps_lat": station["gps_lat"],
            "gps_lng": station["gps_lng"],
            "tenant_id": TENANT_ID,
            "demo_seed": DEMO_MARKER,
        }
    config["base_stations"] = next_map
    web_ui.save_base_station_config(config)
    return len(BASE_STATIONS)


def seed_sensors() -> int:
    sensors = web_ui._load_all_sensors()
    next_sensors = []
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        sensor_tenant = web_ui._tenant_id_from_sensor(sensor)
        if sensor_tenant in (set(LEGACY_TENANT_IDS) | {TENANT_ID}):
            continue
        shared = sensor.get("shared_tenants") or []
        if isinstance(shared, list):
            filtered_shared = [
                tenant
                for tenant in shared
                if web_ui._normalize_tenant_id(tenant, fallback=web_ui._default_tenant_id()) not in (set(LEGACY_TENANT_IDS) | {TENANT_ID})
            ]
            if filtered_shared != shared:
                sensor = dict(sensor)
                sensor["shared_tenants"] = filtered_shared
        next_sensors.append(sensor)

    for sensor in DEMO_SENSORS:
        next_sensors.append(build_sensor_payload(sensor))

    web_ui._save_all_sensors(next_sensors)
    return len(DEMO_SENSORS)


def seed_alerts() -> int:
    other_alerts = [
        alert
        for alert in web_ui._load_alerts()
        if web_ui._normalize_tenant_id(alert.get("tenant_id"), fallback=web_ui._default_tenant_id()) not in (set(LEGACY_TENANT_IDS) | {TENANT_ID})
    ]
    merged = other_alerts + build_alerts()
    web_ui._save_alerts(merged)
    return len(merged) - len(other_alerts)


def cleanup_alert_files() -> None:
    events_path = REPO_ROOT / "alert_events.json"
    state_path = REPO_ROOT / "alert_state.json"
    if events_path.exists():
        try:
            events = json.loads(events_path.read_text(encoding="utf-8"))
        except Exception:
            events = []
        if isinstance(events, list):
            kept = [
                item
                for item in events
                if web_ui._normalize_tenant_id((item or {}).get("tenant_id"), fallback=web_ui._default_tenant_id()) not in (set(LEGACY_TENANT_IDS) | {TENANT_ID})
            ]
            events_path.write_text(json.dumps(kept, indent=2, ensure_ascii=False), encoding="utf-8")
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}
        if isinstance(state, dict):
            kept_state = {}
            for key, value in state.items():
                tenant_prefix = str(key).split("::", 1)[0]
                if web_ui._normalize_tenant_id(tenant_prefix, fallback=web_ui._default_tenant_id()) not in (set(LEGACY_TENANT_IDS) | {TENANT_ID}):
                    kept_state[key] = value
            state_path.write_text(json.dumps(kept_state, indent=2, ensure_ascii=False), encoding="utf-8")


def clear_timescale_tenant(conn: Any) -> None:
    with conn.cursor() as cur:
        tenant_ids = sorted(set(LEGACY_TENANT_IDS) | {TENANT_ID})
        for table_name in (
            "alert_events",
            "alert_state_current",
            "telemetry_uplink",
            "inventory_events",
            "inventory_snapshot_points",
            "inventory_snapshot_latest",
            "alert_rules",
        ):
            cur.execute(f"DELETE FROM {table_name} WHERE tenant_id = ANY(%s)", (tenant_ids,))
        cur.execute("DELETE FROM tenants WHERE id = ANY(%s)", (sorted(set(LEGACY_TENANT_IDS)),))


def insert_telemetry(conn: Any, rows_by_sensor: Dict[str, List[Dict[str, Any]]]) -> int:
    rows = [row for sensor_rows in rows_by_sensor.values() for row in sensor_rows]
    rows.sort(key=lambda item: item["ts"])
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO telemetry_uplink
                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            [
                (
                    row["ts"],
                    row["tenant_id"],
                    row["sensor_eui"],
                    row["base_station_eui"],
                    row["snr"],
                    row["rssi"],
                    None,
                    row["packet_cnt"],
                    row["msg_type"],
                    json.dumps(row["payload"], separators=(",", ":"), ensure_ascii=True),
                )
                for row in rows
            ],
        )
    return len(rows)


def inject_history_scenario(conn: Any, sensor_by_eui: Dict[str, DemoSensor]) -> None:
    sensor = sensor_by_eui["A0412D2A10000101"]
    temp_high_ts = BASE_TIME + timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telemetry_uplink
                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                temp_high_ts,
                TENANT_ID,
                sensor.eui.lower(),
                sensor.base_station_eui.lower(),
                17.9,
                -78.0,
                None,
                9990,
                "ul",
                json.dumps(
                    {
                        "data": encode_co2_payload(
                            temperature_1_c=29.6,
                            humidity_1_pct=40,
                            co2_1_ppm=860,
                            temperature_2_c=29.0,
                            humidity_2_pct=39,
                            co2_2_ppm=840,
                            battery_v_est=2.9,
                            co2_last_calibration_ppm=420,
                            days_to_next_calibration=18,
                        ),
                        "rxTime": int(temp_high_ts.timestamp() * 1_000_000_000),
                    },
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
            ),
        )


def evaluate_history_sequence(conn: Any, sensor_by_eui: Dict[str, DemoSensor]) -> None:
    temp_rule = {
        "id": f"{TENANT_ID}:rule:temp-demo-history",
        "tenant_id": TENANT_ID,
        "sensor_eui": sensor_by_eui["A0412D2A10000101"].eui,
        "kind": "threshold",
        "name": "Recepcia teplota demo historia",
        "metric": "temperature_1_c",
        "condition": "gt",
        "threshold": 28.0,
        "severity": "warning",
        "enabled": True,
        "created_at": iso(dt_hours_ago(8)),
    }
    alerts = web_ui._load_alerts()
    alerts.append(temp_rule)
    web_ui._save_alerts(alerts)
    web_ui._evaluate_triggered_alerts(TENANT_ID, persist_state=True)

    sensor = sensor_by_eui["A0412D2A10000101"]
    temp_ok_ts = BASE_TIME + timedelta(minutes=2)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telemetry_uplink
                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                temp_ok_ts,
                TENANT_ID,
                sensor.eui.lower(),
                sensor.base_station_eui.lower(),
                18.4,
                -77.0,
                None,
                9991,
                "ul",
                json.dumps(
                    {
                        "data": encode_co2_payload(
                            temperature_1_c=22.8,
                            humidity_1_pct=41,
                            co2_1_ppm=830,
                            temperature_2_c=22.4,
                            humidity_2_pct=40,
                            co2_2_ppm=810,
                            battery_v_est=2.9,
                            co2_last_calibration_ppm=420,
                            days_to_next_calibration=18,
                        ),
                        "rxTime": int(temp_ok_ts.timestamp() * 1_000_000_000),
                    },
                    separators=(",", ":"),
                    ensure_ascii=True,
                ),
            ),
        )

    alerts = [alert for alert in web_ui._load_alerts() if alert.get("id") != temp_rule["id"]]
    web_ui._save_alerts(alerts)
    web_ui._evaluate_triggered_alerts(TENANT_ID, persist_state=True)


def timescale_summary(conn: Any) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    with conn.cursor() as cur:
        for table_name in ("telemetry_uplink", "alert_events", "inventory_snapshot_latest", "alert_rules"):
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE tenant_id = %s", (TENANT_ID,))
            summary[table_name] = int((cur.fetchone() or [0])[0] or 0)
    return summary


def main() -> int:
    ensure_viewer_user()
    ensure_tenant_registry(conn=None)
    cleanup_alert_files()
    seeded_base_stations = seed_base_stations()
    seeded_sensors = seed_sensors()
    seeded_alerts = seed_alerts()
    web_ui._sync_inventory_gps_to_coverage_positions()

    rows_by_sensor = build_demo_telemetry({sensor.eui: sensor for sensor in DEMO_SENSORS})
    conn = None
    db_summary: Dict[str, Any] = {"connected": False}
    try:
        conn, err = web_ui._timescale_connect()
        if conn is None:
            raise RuntimeError(err or "Timescale is not reachable")
        web_ui._ensure_timescale_schema(conn)
        ensure_tenant_registry(conn)
        clear_timescale_tenant(conn)
        inserted_rows = insert_telemetry(conn, rows_by_sensor)
        inject_history_scenario(conn, {sensor.eui: sensor for sensor in DEMO_SENSORS})
        web_ui._sync_inventory_snapshot_to_timescale(trigger="seed_viewer_demo")
        evaluate_history_sequence(conn, {sensor.eui: sensor for sensor in DEMO_SENSORS})
        web_ui._build_current_incidents(TENANT_ID)
        db_summary = {
            "connected": True,
            "inserted_telemetry_rows": inserted_rows + 2,
            **timescale_summary(conn),
        }
    except Exception as exc:
        db_summary = {
            "connected": False,
            "error": str(exc),
        }
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    output = {
        "tenant_id": TENANT_ID,
        "user": {"username": "test", "password": "test", "role": "viewer"},
        "seeded_base_stations": seeded_base_stations,
        "seeded_sensors": seeded_sensors,
        "seeded_alert_rules": seeded_alerts,
        "telemetry_rows_planned": sum(len(items) for items in rows_by_sensor.values()) + 2,
        "db": db_summary,
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
