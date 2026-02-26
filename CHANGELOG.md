# Changelog

All notable changes to this project are documented in this file.

## [2026-02-26] - Release Workspace Update

### Added
- New in-app Documentation section (`/documentation`) with:
  - structured navigation
  - search/filter in docs
  - operational workflows
  - API and runbook summaries
- New Administration page template with tenant/access workflows.
- MQTT dedicated page template and monitoring/test UX improvements.
- Grafana provisioning and dashboard artifacts:
  - datasource provisioning
  - dashboard provisioning
  - dashboard JSON bundle
- TimescaleDB initialization SQL schema (`timescaledb/init/001_schema.sql`).
- Backup and restore scripts:
  - `scripts/backup.ps1`
  - `scripts/restore.ps1`
- End-to-end critical flow tests (`tests/test_e2e_critical_flows.py`).
- Tenant persistence file (`tenants.json`).

### Changed
- Major UI/UX refactor across Dashboard, Sensors, Base Stations, Network, Health, Logs and Configuration sections.
- Sidebar behavior updated to show `Documentation` entry in bottom area above user profile/logout.
- README rewritten to match current architecture, deployment model, RBAC, monitoring and operations.
- Docker and environment example updates for Timescale/Grafana/monitoring flow.
- Backend/runtime updates in `web_ui.py`, `TLSServer.py`, `mqtt_interface.py`, and related app wiring.

### Operational Notes
- Branch `release` is the default branch for this repository.
- Current baseline commit for this update: `26716ce`.

