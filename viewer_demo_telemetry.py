from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List


TENANT_ID = "test"
TENANT_NAME = "Testovaci tenant"


@dataclass(frozen=True)
class DemoSensor:
    eui: str
    base_station_eui: str


DEMO_SENSORS = {
    "A0412D2A10000101": DemoSensor("A0412D2A10000101", "129AF3FFFE01F124"),
    "A0412D2A10000102": DemoSensor("A0412D2A10000102", "129AF3FFFE01F124"),
    "A0412D2A10000103": DemoSensor("A0412D2A10000103", "129AB3FFFE01F124"),
    "A0412D1D10000104": DemoSensor("A0412D1D10000104", "129A13FFFE01F124"),
    "A0412D1D10000105": DemoSensor("A0412D1D10000105", "129AB3FFFE01F124"),
    "A0412D2A10000106": DemoSensor("A0412D2A10000106", "129AF3FFFE01F124"),
}


def _clamp(value: int, lower: int, upper: int) -> int:
    return max(lower, min(value, upper))


def _bits_to_bytes(bit_string: str) -> List[int]:
    if len(bit_string) % 8 != 0:
        raise ValueError("bit string length must be a multiple of 8")
    return [int(bit_string[idx:idx + 8], 2) for idx in range(0, len(bit_string), 8)]


def _pack_bit_fields(fields: Iterable[tuple[int, int]]) -> List[int]:
    bits = "".join(f"{_clamp(int(value), 0, (1 << width) - 1):0{width}b}" for value, width in fields)
    return _bits_to_bytes(bits)


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
    return _pack_bit_fields(
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
    return _pack_bit_fields(
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


def _telemetry_row(sensor: DemoSensor, *, ts: datetime, packet_cnt: int, snr: float, rssi: float, payload: List[int]) -> Dict[str, Any]:
    return {
        "tenant_id": TENANT_ID,
        "sensor_eui": sensor.eui.lower(),
        "base_station_eui": sensor.base_station_eui.lower(),
        "packet_cnt": packet_cnt,
        "snr": float(snr),
        "rssi": float(rssi),
        "packet_loss_pct": None,
        "msg_type": "ul",
        "ts": ts.astimezone(timezone.utc),
        "payload": {
            "data": payload,
            "rxTime": int(ts.timestamp() * 1_000_000_000),
        },
    }


def build_demo_telemetry_rows(base_time: datetime | None = None) -> List[Dict[str, Any]]:
    current = (base_time or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(second=0, microsecond=0)
    online = DEMO_SENSORS["A0412D2A10000101"]
    critical = DEMO_SENSORS["A0412D2A10000102"]
    warning = DEMO_SENSORS["A0412D2A10000103"]
    recent_door = DEMO_SENSORS["A0412D1D10000104"]
    stale_door = DEMO_SENSORS["A0412D1D10000105"]

    rows: List[Dict[str, Any]] = []

    for idx in range(24):
        ts = current - timedelta(minutes=idx * 6)
        rows.append(
            _telemetry_row(
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
        ts = current - timedelta(minutes=idx * 6)
        rows.append(
            _telemetry_row(
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
        ts = current - timedelta(hours=2, minutes=idx * 7)
        rows.append(
            _telemetry_row(
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
        ts = current - timedelta(minutes=idx * 12)
        alarm_active = idx == 0
        rows.append(
            _telemetry_row(
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

    rows.append(
        _telemetry_row(
            stale_door,
            ts=current - timedelta(days=10, hours=3),
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

    rows.sort(key=lambda item: item["ts"])
    return rows
