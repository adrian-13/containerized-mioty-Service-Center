
# BSSCI Service Center - Docker Deployment

This guide explains how to deploy the BSSCI Service Center using Docker and Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+

## Quick Start

### Development Deployment

```bash
# Build and start the service
docker-compose up --build

# Run in background
docker-compose up -d --build
```

The service will be available at:
- Web UI: http://localhost:5056
- TLS Server: localhost:8000
- TimescaleDB: localhost:5432
- Grafana: http://localhost:3000

### Production Deployment

If you maintain your own production override file, run:

```bash
# Use production configuration
docker-compose -f docker-compose.prod.yml up -d --build
```

The service will be available at:
- Depends on your `docker-compose.prod.yml` port mappings

## Configuration

### Environment Variables

Set these in your environment or `.env` file:

```bash
UID=1000          # User ID for file permissions
GID=1000          # Group ID for file permissions
```

### Optional: InfluxDB Integration (legacy, disabled by default)

If InfluxDB is running, Service Center can:
- read base-station uptime events from Influx
- write inventory CRUD events when sensors/base stations are created, updated, deleted, imported, or cleared

Add to `.env`:

```bash
TELEMETRY_SOURCE=auto
INFLUXDB_URL=http://influxdb2:8086
INFLUXDB_ORG=<your-org>
INFLUXDB_BUCKET=<your-bucket>
INFLUXDB_TOKEN=<your-token>
INFLUXDB_VERIFY_SSL=true

INFLUX_INVENTORY_WRITE_ENABLED=true
INFLUX_INVENTORY_MEASUREMENT=bssci_inventory_events
INFLUX_SNAPSHOT_ENABLED=true
INFLUX_SNAPSHOT_INTERVAL_SECONDS=60
INFLUX_SNAPSHOT_MEASUREMENT=bssci_inventory_snapshot
```

Quick Flux check in Influx UI:

```flux
from(bucket: "<your-bucket>")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "bssci_inventory_events")
  |> sort(columns: ["_time"], desc: true)
```

For periodic value snapshots (sensor/base-station current state):

```flux
from(bucket: "<your-bucket>")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "bssci_inventory_snapshot")
  |> sort(columns: ["_time"], desc: true)
```

Manual one-time sync trigger:

```bash
curl -X POST http://localhost:5056/api/influx/sync-inventory
```

### TimescaleDB + Grafana Integration (Operational Store)

Service Center can also write operational inventory events/snapshots into TimescaleDB
and expose them in Grafana.

Add to `.env`:

```bash
TIMESCALE_ENABLED=true
TIMESCALE_HOST=timescaledb
TIMESCALE_PORT=5432
TIMESCALE_DB=bssci
TIMESCALE_USER=bssci_user
TIMESCALE_PASSWORD=change_me
TIMESCALE_SSLMODE=disable
TIMESCALE_DEFAULT_TENANT=default
TIMESCALE_INVENTORY_WRITE_ENABLED=true
TIMESCALE_TELEMETRY_WRITE_ENABLED=true
TIMESCALE_SNAPSHOT_ENABLED=true
TIMESCALE_SNAPSHOT_INTERVAL_SECONDS=60
TIMESCALE_RETENTION_ENABLED=true
TIMESCALE_TELEMETRY_RETENTION_DAYS=90
TIMESCALE_INVENTORY_RETENTION_DAYS=365
TIMESCALE_COMPRESSION_ENABLED=true
TIMESCALE_COMPRESSION_AFTER_DAYS=7

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
GRAFANA_URL=http://localhost:3000
GRAFANA_INTERNAL_URL=http://grafana:3000
GRAFANA_DASHBOARD_UID=service-center-overview
GRAFANA_DASHBOARD_SLUG=service-center-overview
GRAFANA_ORG_ID=1
GRAFANA_EMBED_ENABLED=true
GRAFANA_ANONYMOUS_ENABLED=true
GRAFANA_ANONYMOUS_ORG_ROLE=Viewer
GRAFANA_PROXY_ENABLED=true
GRAFANA_PROXY_TIMEOUT_SECONDS=20
GRAFANA_PROXY_BEARER_TOKEN=
GRAFANA_PROXY_BASIC_USER=admin
GRAFANA_PROXY_BASIC_PASSWORD=admin
GRAFANA_HEALTH_PANEL_MAP=throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6
```

To avoid extra login prompts in embedded System Health panels, keep `GRAFANA_ANONYMOUS_ENABLED=true`
and restart Grafana after config change. System Health now uses Grafana panel self-refresh
(`refresh=...`) and relative range (`from=now-...`) to avoid iframe reload stutter.

For tenant-safe embedding without creating every app user in Grafana, keep `GRAFANA_PROXY_ENABLED=true`.
The app serves Grafana through `/grafana-proxy/*` and enforces tenant context from current session.
If anonymous is disabled or unstable, set proxy auth (`GRAFANA_PROXY_BEARER_TOKEN` or
`GRAFANA_PROXY_BASIC_USER` + `GRAFANA_PROXY_BASIC_PASSWORD`) so embedded queries stay authenticated.
Use `GRAFANA_INTERNAL_URL` as container-to-container address (usually `http://grafana:3000`) and
`GRAFANA_URL` as external/admin URL.

What is created automatically:
- Timescale extension and schema from `timescaledb/init/001_schema.sql`
- Grafana datasource from `grafana/provisioning/datasources/timescaledb.yml`

Quick checks:

```bash
# Timescale connectivity check through Service Center API
curl http://localhost:5056/api/timescale/status

# Force one-time inventory snapshot sync to Timescale
curl -X POST http://localhost:5056/api/timescale/sync-inventory

# Telemetry summary (last 60 min, 60s buckets)
curl "http://localhost:5056/api/timescale/telemetry?minutes=60&bucket_seconds=60"

# Re-apply retention/compression policies after config changes
curl -X POST http://localhost:5056/api/timescale/policies/apply

# Export/import one tenant dataset (inventory + optional Timescale data)
curl "http://localhost:5056/api/tenants/export?tenant_id=default" -o tenant_default.json
curl -X POST -F "file=@tenant_default.json" "http://localhost:5056/api/tenants/import?tenant_id=default&merge=true"
```

Grafana access:
- URL: `http://localhost:3000`
- Login: values from `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`
- Default datasource: `TimescaleDB`
- Tenant-aware dashboard variable: use `var-tenant=<tenant_id>` in URL (for example `...?var-tenant=default`)

### DB-First Bootstrap and Recovery Notes

When Timescale-backed config storage is enabled, access and tenant registry are DB-first.

Primary DB tables:
- `app_users`
- `tenant_registry_meta`
- `admin_audit_log`
- `app_config_state`

Seed / recovery files:
- `users.default.json`
  - seed source only for first bootstrap
- `users.json`
  - recovery/import fallback only
- `tenants.json`
  - tenant seed/recovery input

Operational implications:
- a clean clone can bootstrap `admin`, `customer` and `test` from `users.default.json`
- bootstrap admin is forced to change password on first login
- user and tenant runtime state should be backed up from DB, not reconstructed from `users.json`
- keep `users.default.json` versioned in Git as bootstrap seed content

### Volumes

The following directories are mounted as volumes:

- `./certs` - SSL certificates (read-only)
- `./endpoints.json` - Sensor configuration
- `./bssci_config.py` - Service configuration
- `./logs` - Application logs
- `timescaledb_data` - TimescaleDB persistent data
- `grafana_data` - Grafana persistent data

### Certificates

The container will automatically generate self-signed SSL certificates if none are provided in the `./certs` directory.

For production, provide your own certificates:

```bash
certs/
├── ca_cert.pem
├── service_center_cert.pem
└── service_center_key.pem
```

## Management Commands

```bash
# View logs
docker-compose logs -f

# Stop service
docker-compose down

# Restart service
docker-compose restart

# Update and restart
docker-compose pull && docker-compose up -d

# Remove everything (including volumes)
docker-compose down -v
```

## Backup / Restore Playbook (Timescale + Config + Certs + Tenants/Users)

This repository now includes PowerShell scripts:

- `scripts/backup.ps1`
- `scripts/restore.ps1`
- `scripts/deploy.ps1`

They are designed for this stack and backup/restore:
- TimescaleDB (`bssci-timescaledb`)
- runtime config files (`.env`, `docker-compose.yml`, `bssci_config.py`, `endpoints.json`, `endpoints.default.json`, `base_stations.json`, `base_stations.default.json`, `alerts.json`, `alerts.default.json`, `coverage_positions.json`, `coverage_floorplan.txt`)
- DB-first access and tenant registry state (`app_users`, `tenant_registry_meta`, `admin_audit_log`, `app_config_state`, alert state/history tables, notification delivery log)
- recovery/seed files (`users.json`, `users.default.json`, `tenants.json`, `alerts.json`, `alerts.default.json`, `alert_state.json`, `alert_events.json`, `viewer_demo_telemetry.py`) when you use them operationally
- admin audit fallback file (`logs/admin_audit.jsonl`) when present
- certificates (`certs/`)

### Create backup

From project root:

```powershell
pwsh .\scripts\backup.ps1
```

Output:
- Folder: `.\backups\bssci_backup_YYYYMMDD_HHMMSS\`
- Archive: `.\backups\bssci_backup_YYYYMMDD_HHMMSS.zip`
- Metadata: `manifest.json`
- Integrity file: `checksums.sha256`

Optional:

```powershell
# Keep only folder (no zip archive)
pwsh .\scripts\backup.ps1 -NoArchive

# Custom output location
pwsh .\scripts\backup.ps1 -OutputRoot D:\bssci-backups
```

### Restore backup

Restore from folder or zip:

```powershell
# From zip
pwsh .\scripts\restore.ps1 -BackupPath .\backups\bssci_backup_YYYYMMDD_HHMMSS.zip

# From extracted folder
pwsh .\scripts\restore.ps1 -BackupPath .\backups\bssci_backup_YYYYMMDD_HHMMSS
```

What restore does:
1. Verifies checksums (if `checksums.sha256` exists)
2. Creates safety copy in `.\backups\pre_restore_YYYYMMDD_HHMMSS\`
3. Stops `bssci-service-center` (if running)
4. Restores files + logs + certs
5. Restores Timescale dump into configured DB
6. Starts `bssci-service-center` again (unless `-NoStart`)

Optional restore modes:

```powershell
# Restore only files/certs (skip DB)
pwsh .\scripts\restore.ps1 -BackupPath .\backups\bssci_backup_*.zip -SkipDb

# Restore only DB (skip files/certs)
pwsh .\scripts\restore.ps1 -BackupPath .\backups\bssci_backup_*.zip -SkipFiles

# Non-interactive restore
pwsh .\scripts\restore.ps1 -BackupPath .\backups\bssci_backup_*.zip -Force
```

### Post-restore checks

```bash
docker-compose ps
curl http://localhost:5056/api/timescale/status
```

If retention/compression settings changed, re-apply policies:

```bash
curl -X POST http://localhost:5056/api/timescale/policies/apply
```

## Health Check

The container includes a health check that verifies both TLS and Web UI using the active configured ports (`LISTEN_PORT`, `WEB_PORT`) from runtime config.

Check health status:
```bash
docker-compose ps
```

## Deploy / Upgrade Playbook

Use `scripts/deploy.ps1` for repeatable upgrades of the service center stack.

What it does:
1. Optionally creates a pre-deploy backup.
2. Pulls the selected branch with `--rebase --autostash`.
3. Rebuilds and restarts the Docker stack.
4. Waits for the service container to become healthy.
5. Runs an HTTP smoke test against the login page.

Examples:

```powershell
# Default deploy from the repo root
pwsh .\scripts\deploy.ps1

# Create a backup before upgrade
pwsh .\scripts\deploy.ps1 -BackupBeforeDeploy

# Skip pull/build/smoke when you only need a controlled restart
pwsh .\scripts\deploy.ps1 -NoPull -NoBuild -NoSmokeTest
```

Optional overrides:

```powershell
pwsh .\scripts\deploy.ps1 -Branch release -ServiceName bssci-service-center -SmokeTestUrl http://localhost:5056/login
```

Rollback:

```powershell
# Restore from a backup created by the backup script
pwsh .\scripts\restore.ps1 -BackupPath .\backups\bssci_backup_YYYYMMDD_HHMMSS.zip -Force
```

Recommended post-deploy checks:

```bash
docker-compose ps
curl http://localhost:5056/login
curl http://localhost:5056/api/timescale/status
```

## Troubleshooting

### Permission Issues

If you encounter permission issues with logs or certificates:

```bash
# Set correct ownership
sudo chown -R $USER:$USER certs/ logs/

# Or run with specific user
UID=$(id -u) GID=$(id -g) docker-compose up
```

### Certificate Issues

Regenerate certificates:

```bash
# Remove old certificates
rm -rf certs/*

# Restart container (will auto-generate)
docker-compose restart
```

### Port Conflicts

If ports are already in use, modify the port mappings in `docker-compose.yml`:

```yaml
ports:
  - "16020:8000"   # Change external TLS port
  - "5057:5000"    # Change external port
```

## Security Notes

- Change default MQTT credentials in `bssci_config.py`
- Use proper SSL certificates for production
- Restrict network access to required ports only
- Regular security updates of base images
