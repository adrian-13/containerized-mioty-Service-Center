# BSSCI Service Center

Operational control plane for MIOTY base stations and sensors, with secure TLS ingestion, MQTT integration, tenant-aware management, topology mapping, runtime monitoring, and admin auditability.

## What This Project Provides

- Secure base-station communication via BSSCI/TLS server.
- Sensor lifecycle operations:
  - create/update/delete
  - register
  - attach/detach to base stations
  - bulk workflows
- Base station inventory and runtime status management.
- Network workspace:
  - Coverage map (OpenStreetMap + floorplan)
  - Topology graph
  - GPS sync between map and device settings
  - missing-GPS assistant
- Dashboard with configurable layout widgets.
- System Health with runtime counters and telemetry panels.
- MQTT monitoring and test publish utilities.
- Tenant and user administration with RBAC scopes.
- Admin audit log stream and export.
- Optional storage/analytics integrations:
  - TimescaleDB (operational store + telemetry)
  - InfluxDB (optional telemetry/inventory path)
  - Grafana (dashboard integration/proxy)

## In-App Documentation

Detailed docs are available directly in UI:

- Sidebar item: `Documentation` (bottom, above user profile)
- Route: `/documentation`

This is the primary user guide for page-level functionality and workflows.

## High-Level Architecture

```text
Base Stations
  -> TLS/BSSCI Server (TLSServer.py)
    -> Internal queues (backpressure + retry metrics)
      -> MQTT publish path
      -> Optional Timescale/Influx writes
        -> Web UI APIs (Flask)
          -> Dashboard / Network / Sensors / Base Stations / Health / Logs
```

## Main UI Sections

- `Dashboard`
  - Live operational overview.
  - Layout customization (drag/resize where enabled).
- `Sensors`
  - Inventory, attach/detach, status, details.
- `Base Stations`
  - Gateway inventory, state, metadata, certificates entry-point.
- `Network Topology`
  - Coverage mode + topology mode with link/issue filters.
- `System Health`
  - Runtime + telemetry reliability indicators.
- `MQTT`
  - Broker monitoring and publish test workflows.
- `Logs`
  - Runtime logs + admin audit logs (scope-based visibility).
- `Administration (Access & Tenants)`
  - Users, tenants, scoped admin actions.
- `Configuration`
  - Server, MQTT, storage, maintenance, optional modules.
- `Certificates`
  - TLS certificate management.

## Security and Access Model

- Login-protected UI/API.
- Roles: admin/user/viewer.
- Admin scopes for granular permissions (user/tenant/config/audit operations).
- Tenant-aware filtering for non-admin data visibility.
- Admin users can be configured for global visibility.

## Docker Deployment (Recommended)

Use `docker-compose.yml` in this repository.

### Quick Start

```bash
docker compose up -d --build
```

Default exposed services (as configured):

- Service Center UI: `http://localhost:5056`
- TLS server: `localhost:8000`
- TimescaleDB: `localhost:5432`
- Grafana: `http://localhost:3000`

For detailed Docker setup, see `README-Docker.md`.

## Local Run (Without Docker)

```bash
pip install -r requirements.txt
python web_main.py
```

Open `http://localhost:5000` (unless configured otherwise).

## Configuration

Configuration is split between:

- `.env` (runtime/env integration settings)
- `bssci_config.py` (core server defaults)
- JSON runtime files:
  - `endpoints.json`
  - `base_stations.json`
  - `coverage_positions.json`
  - `users.json`
  - `tenants.json`

Important integration families:

- MQTT
- TimescaleDB
- InfluxDB
- Grafana proxy/embed

## Data and State Files

Typical persisted state:

- `users.json`, `tenants.json` (access and tenancy)
- `base_stations.json` (BS inventory)
- `endpoints.json` (sensor inventory/config)
- `coverage_positions.json` (map coordinates)
- `logs/` (runtime logs)

## Reliability Features

- Queue depth metrics for runtime backpressure.
- Retry policy for asynchronous write/publish paths.
- DB write latency observations.
- MQTT reconnect tracking.
- Admin audit trail for critical actions.

These are surfaced in monitoring APIs/UI and can be forwarded to external alerting.

## Backup and Restore

Included scripts:

- `scripts/backup.ps1`
- `scripts/restore.ps1`

Suggested backup scope:

- TimescaleDB data
- configuration files (`.env`, `bssci_config.py`, JSON state)
- certificates (`certs/`)
- users and tenants

See `README-Docker.md` for practical commands and examples.

## Testing

Test directory:

- `tests/`

Critical-flow coverage includes areas like:

- attach/detach
- GPS sync behavior
- tenant filtering
- MQTT test publish path
- audit log flows

Run tests (example):

```bash
pytest -q
```

## Common Operational Notes

- If map positions and device GPS differ, run save/sync workflow in Network section.
- If sensor shows `BS: none` after attach, verify attach response and tenant filters.
- If telemetry is missing, check:
  - sensor registration
  - attach mapping
  - MQTT status
  - queue/retry metrics
  - logs

## Repository Structure (Key Files)

- `web_ui.py` - Flask routes, API endpoints, auth, UI backend logic
- `web_main.py` - application startup
- `TLSServer.py` / `tls_server.py` - transport/protocol handling
- `mqtt_interface.py` - MQTT integration
- `templates/` - web pages
- `docker-compose.yml` - container stack
- `grafana/` - provisioning and dashboards
- `timescaledb/` - initialization SQL

## Notes for Customized Deployments

This project is actively customized for specific operational workflows. Treat automated "platform update" semantics as optional and prefer Git-based controlled releases in production.

## License

See `LICENSE`.
