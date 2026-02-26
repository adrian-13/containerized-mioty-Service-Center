CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO tenants (id, name)
VALUES ('default', 'Default Tenant')
ON CONFLICT (id) DO NOTHING;

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
    payload JSONB,
    CONSTRAINT inventory_events_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT
);

SELECT create_hypertable('inventory_events', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
CREATE INDEX IF NOT EXISTS idx_inventory_events_tenant_ts ON inventory_events (tenant_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_inventory_events_eui_ts ON inventory_events (eui, ts DESC);

CREATE TABLE IF NOT EXISTS inventory_snapshot_points (
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id TEXT NOT NULL DEFAULT 'default',
    entity_type TEXT NOT NULL,
    eui TEXT NOT NULL,
    status TEXT,
    trigger TEXT NOT NULL DEFAULT 'manual',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT inventory_snapshot_points_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT
);

SELECT create_hypertable('inventory_snapshot_points', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_points_tenant_ts ON inventory_snapshot_points (tenant_id, ts DESC);

CREATE TABLE IF NOT EXISTS inventory_snapshot_latest (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    entity_type TEXT NOT NULL,
    eui TEXT NOT NULL,
    status TEXT,
    trigger TEXT NOT NULL DEFAULT 'manual',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT inventory_snapshot_latest_pk PRIMARY KEY (tenant_id, entity_type, eui),
    CONSTRAINT inventory_snapshot_latest_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_latest_updated ON inventory_snapshot_latest (tenant_id, updated_at DESC);

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
    payload JSONB,
    CONSTRAINT telemetry_uplink_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT
);

ALTER TABLE telemetry_uplink ADD COLUMN IF NOT EXISTS packet_cnt BIGINT;
ALTER TABLE telemetry_uplink ADD COLUMN IF NOT EXISTS msg_type TEXT NOT NULL DEFAULT 'ul';

SELECT create_hypertable('telemetry_uplink', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_ts ON telemetry_uplink (tenant_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_sensor_ts ON telemetry_uplink (tenant_id, sensor_eui, ts DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_bs_ts ON telemetry_uplink (tenant_id, base_station_eui, ts DESC);

DO $$
BEGIN
    BEGIN
        ALTER TABLE telemetry_uplink SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'ts DESC',
            timescaledb.compress_segmentby = 'tenant_id,sensor_eui,base_station_eui'
        );
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        PERFORM add_compression_policy('telemetry_uplink', INTERVAL '7 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        PERFORM add_retention_policy('telemetry_uplink', INTERVAL '90 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE inventory_events SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'ts DESC',
            timescaledb.compress_segmentby = 'tenant_id,entity_type,eui'
        );
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        PERFORM add_compression_policy('inventory_events', INTERVAL '14 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        PERFORM add_retention_policy('inventory_events', INTERVAL '365 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE inventory_snapshot_points SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'ts DESC',
            timescaledb.compress_segmentby = 'tenant_id,entity_type,eui'
        );
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        PERFORM add_compression_policy('inventory_snapshot_points', INTERVAL '14 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    BEGIN
        PERFORM add_retention_policy('inventory_snapshot_points', INTERVAL '365 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
END
$$;
