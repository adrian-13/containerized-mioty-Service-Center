import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

LISTEN_HOST = os.getenv("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "16018"))

CERT_FILE = os.getenv("CERT_FILE", "certs/service_center_cert.pem")
KEY_FILE = os.getenv("KEY_FILE", "certs/service_center_key.pem")
CA_FILE = os.getenv("CA_FILE", "certs/ca_cert.pem")
TLS_CLIENT_CERT_MODE = os.getenv("TLS_CLIENT_CERT_MODE", "required").strip().lower()

# MQTT Configuration - read from .env
MQTT_ENABLED = os.getenv("MQTT_ENABLED", "true").strip().lower() == "true"
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
BASE_TOPIC = os.getenv("BASE_TOPIC", "bssci/")
MQTT_IN_QUEUE_MAXSIZE = int(os.getenv("MQTT_IN_QUEUE_MAXSIZE", "5000"))
MQTT_OUT_QUEUE_MAXSIZE = int(os.getenv("MQTT_OUT_QUEUE_MAXSIZE", "5000"))
MQTT_QUEUE_PUT_TIMEOUT_SECONDS = float(os.getenv("MQTT_QUEUE_PUT_TIMEOUT_SECONDS", "1.5"))
MQTT_PUBLISH_MAX_RETRIES = int(os.getenv("MQTT_PUBLISH_MAX_RETRIES", "5"))
MQTT_PUBLISH_RETRY_BASE_DELAY_SECONDS = float(os.getenv("MQTT_PUBLISH_RETRY_BASE_DELAY_SECONDS", "0.5"))
MQTT_PUBLISH_RETRY_MAX_DELAY_SECONDS = float(os.getenv("MQTT_PUBLISH_RETRY_MAX_DELAY_SECONDS", "8.0"))

# Monitoring / alerting thresholds
MONITOR_QUEUE_WARN_PCT = float(os.getenv("MONITOR_QUEUE_WARN_PCT", "65"))
MONITOR_QUEUE_CRIT_PCT = float(os.getenv("MONITOR_QUEUE_CRIT_PCT", "85"))
MONITOR_RETRY_FAIL_WARN_PCT = float(os.getenv("MONITOR_RETRY_FAIL_WARN_PCT", "5"))
MONITOR_RETRY_FAIL_CRIT_PCT = float(os.getenv("MONITOR_RETRY_FAIL_CRIT_PCT", "20"))
MONITOR_DB_WRITE_LATENCY_WARN_MS = float(os.getenv("MONITOR_DB_WRITE_LATENCY_WARN_MS", "500"))
MONITOR_DB_WRITE_LATENCY_CRIT_MS = float(os.getenv("MONITOR_DB_WRITE_LATENCY_CRIT_MS", "1500"))
MONITOR_MQTT_RECONNECT_WARN_PER_HOUR = float(os.getenv("MONITOR_MQTT_RECONNECT_WARN_PER_HOUR", "3"))
MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR = float(os.getenv("MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR", "8"))

SENSOR_CONFIG_FILE = os.getenv("SENSOR_CONFIG_FILE", "endpoints.json")
BASE_STATION_CONFIG_FILE = os.getenv("BASE_STATION_CONFIG_FILE", "base_stations.json")
STATUS_INTERVAL = int(os.getenv("STATUS_INTERVAL", "30"))
DEDUPLICATION_DELAY = float(os.getenv("DEDUPLICATION_DELAY", "2.0"))
ATTACH_RECONCILE_INTERVAL_SECONDS = int(os.getenv("ATTACH_RECONCILE_INTERVAL_SECONDS", "60"))
ATTACH_RETRY_MIN_INTERVAL_SECONDS = int(os.getenv("ATTACH_RETRY_MIN_INTERVAL_SECONDS", "120"))
ATTACH_RESPONSE_TIMEOUT_SECONDS = int(os.getenv("ATTACH_RESPONSE_TIMEOUT_SECONDS", "12"))
ATTACH_POST_CONNECT_RETRY_DELAY_SECONDS = int(os.getenv("ATTACH_POST_CONNECT_RETRY_DELAY_SECONDS", "8"))

# Auto-detach Configuration
AUTO_DETACH_ENABLED = os.getenv("AUTO_DETACH_ENABLED", "true").lower() == "true"
AUTO_DETACH_TIMEOUT = int(os.getenv("AUTO_DETACH_TIMEOUT", "259200"))  # 72 hours in seconds
AUTO_DETACH_WARNING_TIMEOUT = int(os.getenv("AUTO_DETACH_WARNING_TIMEOUT", "129600"))  # 36 hours in seconds
AUTO_DETACH_CHECK_INTERVAL = int(os.getenv("AUTO_DETACH_CHECK_INTERVAL", "3600"))  # Check every hour

# Timezone Configuration
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")  # Default to Europe/Berlin (CET/CEST)
APP_LANGUAGE = os.getenv("APP_LANGUAGE", "sk").strip().lower() or "sk"

# UI module toggles
OMS_ENABLED = os.getenv("OMS_ENABLED", "true").strip().lower() == "true"
MQTT_UI_ENABLED = os.getenv("MQTT_UI_ENABLED", "true").strip().lower() == "true"
BS_UPTIME_PANEL_ENABLED = os.getenv("BS_UPTIME_PANEL_ENABLED", "false").strip().lower() == "true"

# Telemetry source configuration (runtime memory vs InfluxDB)
TELEMETRY_SOURCE = os.getenv("TELEMETRY_SOURCE", "auto").strip().lower()

# InfluxDB configuration (optional)
INFLUX_ENABLED = os.getenv("INFLUX_ENABLED", "false").strip().lower() == "true"
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "").strip()
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "").strip()
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "").strip()
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "").strip()
INFLUXDB_VERIFY_SSL = os.getenv("INFLUXDB_VERIFY_SSL", "true").strip().lower() == "true"

# Influx uptime query settings
INFLUX_UPTIME_MEASUREMENT = os.getenv("INFLUX_UPTIME_MEASUREMENT", "bssci_bs_uptime").strip()
INFLUX_UPTIME_FIELD = os.getenv("INFLUX_UPTIME_FIELD", "status").strip()
INFLUX_UPTIME_EUI_TAG = os.getenv("INFLUX_UPTIME_EUI_TAG", "eui").strip()
INFLUX_UPTIME_QUERY = os.getenv("INFLUX_UPTIME_QUERY", "").strip()

# Influx inventory event writes (sensor/base station CRUD)
INFLUX_INVENTORY_WRITE_ENABLED = os.getenv("INFLUX_INVENTORY_WRITE_ENABLED", "true").strip().lower() == "true"
INFLUX_INVENTORY_MEASUREMENT = os.getenv("INFLUX_INVENTORY_MEASUREMENT", "bssci_inventory_events").strip()

# Influx periodic snapshot writes (current inventory/runtime values)
INFLUX_SNAPSHOT_ENABLED = os.getenv("INFLUX_SNAPSHOT_ENABLED", "true").strip().lower() == "true"
INFLUX_SNAPSHOT_INTERVAL_SECONDS = int(os.getenv("INFLUX_SNAPSHOT_INTERVAL_SECONDS", "60"))
INFLUX_SNAPSHOT_MEASUREMENT = os.getenv("INFLUX_SNAPSHOT_MEASUREMENT", "bssci_inventory_snapshot").strip()

# TimescaleDB / PostgreSQL configuration (multi-tenant operational store)
TIMESCALE_ENABLED = os.getenv("TIMESCALE_ENABLED", "false").strip().lower() == "true"
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "timescaledb").strip()
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB = os.getenv("TIMESCALE_DB", "bssci").strip()
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "bssci_user").strip()
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "").strip()
TIMESCALE_SSLMODE = os.getenv("TIMESCALE_SSLMODE", "disable").strip().lower()
TIMESCALE_DEFAULT_TENANT = os.getenv("TIMESCALE_DEFAULT_TENANT", "default").strip() or "default"
TIMESCALE_INVENTORY_WRITE_ENABLED = os.getenv("TIMESCALE_INVENTORY_WRITE_ENABLED", "true").strip().lower() == "true"
TIMESCALE_TELEMETRY_WRITE_ENABLED = os.getenv("TIMESCALE_TELEMETRY_WRITE_ENABLED", "true").strip().lower() == "true"
TIMESCALE_SNAPSHOT_ENABLED = os.getenv("TIMESCALE_SNAPSHOT_ENABLED", "true").strip().lower() == "true"
TIMESCALE_SNAPSHOT_INTERVAL_SECONDS = int(os.getenv("TIMESCALE_SNAPSHOT_INTERVAL_SECONDS", "60"))
TIMESCALE_RETENTION_ENABLED = os.getenv("TIMESCALE_RETENTION_ENABLED", "true").strip().lower() == "true"
TIMESCALE_TELEMETRY_RETENTION_DAYS = int(os.getenv("TIMESCALE_TELEMETRY_RETENTION_DAYS", "90"))
TIMESCALE_INVENTORY_RETENTION_DAYS = int(os.getenv("TIMESCALE_INVENTORY_RETENTION_DAYS", "365"))
TIMESCALE_COMPRESSION_ENABLED = os.getenv("TIMESCALE_COMPRESSION_ENABLED", "true").strip().lower() == "true"
TIMESCALE_COMPRESSION_AFTER_DAYS = int(os.getenv("TIMESCALE_COMPRESSION_AFTER_DAYS", "7"))
TIMESCALE_UPLINK_QUEUE_MAXSIZE = int(os.getenv("TIMESCALE_UPLINK_QUEUE_MAXSIZE", "10000"))
TIMESCALE_UPLINK_BATCH_SIZE = int(os.getenv("TIMESCALE_UPLINK_BATCH_SIZE", "200"))
TIMESCALE_UPLINK_WRITE_MAX_RETRIES = int(os.getenv("TIMESCALE_UPLINK_WRITE_MAX_RETRIES", "5"))
TIMESCALE_UPLINK_RETRY_BASE_SECONDS = float(os.getenv("TIMESCALE_UPLINK_RETRY_BASE_SECONDS", "0.5"))
TIMESCALE_UPLINK_RETRY_MAX_SECONDS = float(os.getenv("TIMESCALE_UPLINK_RETRY_MAX_SECONDS", "10.0"))

# Grafana integration
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000").strip()
GRAFANA_INTERNAL_URL = os.getenv("GRAFANA_INTERNAL_URL", "").strip()
GRAFANA_DASHBOARD_UID = os.getenv("GRAFANA_DASHBOARD_UID", "service-center-overview").strip() or "service-center-overview"
GRAFANA_DASHBOARD_SLUG = os.getenv("GRAFANA_DASHBOARD_SLUG", "service-center-overview").strip() or "service-center-overview"
GRAFANA_ORG_ID = int(os.getenv("GRAFANA_ORG_ID", "1"))
GRAFANA_EMBED_ENABLED = os.getenv("GRAFANA_EMBED_ENABLED", "true").strip().lower() == "true"
GRAFANA_ANONYMOUS_ENABLED = os.getenv("GRAFANA_ANONYMOUS_ENABLED", "true").strip().lower() == "true"
GRAFANA_ANONYMOUS_ORG_ROLE = os.getenv("GRAFANA_ANONYMOUS_ORG_ROLE", "Viewer").strip() or "Viewer"
GRAFANA_PROXY_ENABLED = os.getenv("GRAFANA_PROXY_ENABLED", "true").strip().lower() == "true"
GRAFANA_PROXY_TIMEOUT_SECONDS = int(os.getenv("GRAFANA_PROXY_TIMEOUT_SECONDS", "20"))
GRAFANA_PROXY_BEARER_TOKEN = os.getenv("GRAFANA_PROXY_BEARER_TOKEN", "").strip()
GRAFANA_PROXY_BASIC_USER = os.getenv(
    "GRAFANA_PROXY_BASIC_USER",
    os.getenv("GRAFANA_ADMIN_USER", ""),
).strip()
GRAFANA_PROXY_BASIC_PASSWORD = os.getenv(
    "GRAFANA_PROXY_BASIC_PASSWORD",
    os.getenv("GRAFANA_ADMIN_PASSWORD", ""),
).strip()
GRAFANA_HEALTH_PANEL_MAP = os.getenv(
    "GRAFANA_HEALTH_PANEL_MAP",
    "throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6",
).strip()

# Web auth/security hardening
AUTH_BOOTSTRAP_DEFAULT_USERS = os.getenv("AUTH_BOOTSTRAP_DEFAULT_USERS", "true").strip().lower() == "true"
AUTH_BOOTSTRAP_DEMO_USERS = os.getenv(
    "AUTH_BOOTSTRAP_DEMO_USERS",
    "true",
).strip().lower() == "true"
AUTH_FORCE_INITIAL_ADMIN_PASSWORD_CHANGE = os.getenv(
    "AUTH_FORCE_INITIAL_ADMIN_PASSWORD_CHANGE",
    "true",
).strip().lower() == "true"
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").strip().lower() == "true"
AUTH_SESSION_TIMEOUT_MINUTES = int(os.getenv("AUTH_SESSION_TIMEOUT_MINUTES", "30"))
AUTH_RATE_LIMIT_ENABLED = os.getenv("AUTH_RATE_LIMIT_ENABLED", "true").strip().lower() == "true"
AUTH_LOGIN_RATE_LIMIT_ATTEMPTS = int(os.getenv("AUTH_LOGIN_RATE_LIMIT_ATTEMPTS", "8"))
AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS", "300"))
AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS = int(os.getenv("AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS", "900"))
AUTH_API_RATE_LIMIT_REQUESTS = int(os.getenv("AUTH_API_RATE_LIMIT_REQUESTS", "300"))
AUTH_API_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("AUTH_API_RATE_LIMIT_WINDOW_SECONDS", "60"))
ADMIN_AUDIT_EXPORT_MAX_ROWS = int(os.getenv("ADMIN_AUDIT_EXPORT_MAX_ROWS", "50000"))
