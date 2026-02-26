import asyncio
from collections import deque
import json
import logging
import time
from typing import Any, Optional

from aiomqtt import Client, MqttError

from bssci_config import (
    BASE_TOPIC,
    MQTT_BROKER,
    MQTT_PASSWORD,
    MQTT_PORT,
    MQTT_PUBLISH_MAX_RETRIES,
    MQTT_PUBLISH_RETRY_BASE_DELAY_SECONDS,
    MQTT_PUBLISH_RETRY_MAX_DELAY_SECONDS,
    MQTT_QUEUE_PUT_TIMEOUT_SECONDS,
    MQTT_USERNAME,
)

logger = logging.getLogger(__name__)


class MQTTClient:
    """MQTT bridge between external topics and internal TLS server queues."""

    def __init__(
        self,
        mqtt_out_queue: asyncio.Queue[dict[str, Any]],
        mqtt_in_queue: asyncio.Queue[dict[str, Any]],
    ):
        self.broker_host = MQTT_BROKER
        self.base_topic = (BASE_TOPIC or "mioty").rstrip("/")
        self.config_topic = f"{self.base_topic}/ep/+/config"
        self.register_topic = f"{self.base_topic}/ep/+/register"
        self.command_topic = f"{self.base_topic}/ep/+/cmd"
        self.mqtt_out_queue = mqtt_out_queue
        self.mqtt_in_queue = mqtt_in_queue

        self.connected = False
        self.last_connected_at = 0.0
        self.last_disconnected_at = 0.0
        self.last_error = ""
        self.recent_incoming_topics: deque[dict[str, Any]] = deque(maxlen=40)
        self.recent_outgoing_topics: deque[dict[str, Any]] = deque(maxlen=40)
        self.recent_errors: deque[dict[str, Any]] = deque(maxlen=20)
        self.reconnect_events: deque[float] = deque(maxlen=240)
        self.queue_put_timeout_seconds = max(0.0, float(MQTT_QUEUE_PUT_TIMEOUT_SECONDS or 0.0))
        self.publish_max_retries = max(0, int(MQTT_PUBLISH_MAX_RETRIES or 0))
        self.publish_retry_base_delay_seconds = max(0.1, float(MQTT_PUBLISH_RETRY_BASE_DELAY_SECONDS or 0.1))
        self.publish_retry_max_delay_seconds = max(
            self.publish_retry_base_delay_seconds,
            float(MQTT_PUBLISH_RETRY_MAX_DELAY_SECONDS or self.publish_retry_base_delay_seconds),
        )

        self.stats: dict[str, int] = {
            "incoming_total": 0,
            "incoming_failed": 0,
            "incoming_queued": 0,
            "incoming_backpressure": 0,
            "incoming_dropped": 0,
            "outgoing_published": 0,
            "outgoing_failed": 0,
            "outgoing_retried": 0,
            "outgoing_requeued": 0,
            "outgoing_retry_exhausted": 0,
            "outgoing_backpressure": 0,
            "outgoing_dropped": 0,
            "connect_success": 0,
            "reconnect_count": 0,
            "connect_failures": 0,
        }
        self.queue_metrics: dict[str, float] = {
            "in_high_water": 0,
            "out_high_water": 0,
            "out_last_wait_ms": 0.0,
            "out_max_wait_ms": 0.0,
        }

        logger.info("MQTT client initialized for %s:%s", self.broker_host, MQTT_PORT)
        logger.info("MQTT base topic: %s", self.base_topic)
        logger.info("mqtt_out_queue id=%s mqtt_in_queue id=%s", id(self.mqtt_out_queue), id(self.mqtt_in_queue))
        logger.info(
            "MQTT publish retry policy: max_retries=%s base_delay=%.2fs max_delay=%.2fs queue_put_timeout=%.2fs",
            self.publish_max_retries,
            self.publish_retry_base_delay_seconds,
            self.publish_retry_max_delay_seconds,
            self.queue_put_timeout_seconds,
        )

    def _track_error(self, source: str, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"
        self.last_error = message
        self.recent_errors.append({"source": source, "error": message, "timestamp": time.time()})

    def _retry_delay(self, attempt: int) -> float:
        if attempt <= 0:
            return 0.0
        raw = self.publish_retry_base_delay_seconds * (2 ** (attempt - 1))
        return min(self.publish_retry_max_delay_seconds, raw)

    def _observe_queue_watermarks(self) -> None:
        in_size = self.mqtt_in_queue.qsize()
        out_size = self.mqtt_out_queue.qsize()
        if in_size > self.queue_metrics["in_high_water"]:
            self.queue_metrics["in_high_water"] = in_size
        if out_size > self.queue_metrics["out_high_water"]:
            self.queue_metrics["out_high_water"] = out_size

    async def _enqueue_incoming(self, item: dict[str, Any], *, source: str) -> bool:
        self._observe_queue_watermarks()
        try:
            self.mqtt_in_queue.put_nowait(item)
            self.stats["incoming_queued"] += 1
            self._observe_queue_watermarks()
            return True
        except asyncio.QueueFull:
            self.stats["incoming_backpressure"] += 1
            logger.warning("MQTT incoming queue is full (source=%s size=%s)", source, self.mqtt_in_queue.qsize())
            if self.queue_put_timeout_seconds <= 0:
                self.stats["incoming_failed"] += 1
                self.stats["incoming_dropped"] += 1
                return False
            try:
                await asyncio.wait_for(self.mqtt_in_queue.put(item), timeout=self.queue_put_timeout_seconds)
                self.stats["incoming_queued"] += 1
                self._observe_queue_watermarks()
                return True
            except asyncio.TimeoutError:
                self.stats["incoming_failed"] += 1
                self.stats["incoming_dropped"] += 1
                logger.error(
                    "MQTT incoming queue enqueue timeout after %.2fs (source=%s)",
                    self.queue_put_timeout_seconds,
                    source,
                )
                return False

    async def _enqueue_outgoing(self, item: dict[str, Any], *, source: str) -> bool:
        self._observe_queue_watermarks()
        item.setdefault("__first_queued_at", time.time())
        try:
            self.mqtt_out_queue.put_nowait(item)
            self._observe_queue_watermarks()
            return True
        except asyncio.QueueFull:
            self.stats["outgoing_backpressure"] += 1
            logger.warning("MQTT outgoing queue is full (source=%s size=%s)", source, self.mqtt_out_queue.qsize())
            if self.queue_put_timeout_seconds <= 0:
                self.stats["outgoing_dropped"] += 1
                return False
            try:
                await asyncio.wait_for(self.mqtt_out_queue.put(item), timeout=self.queue_put_timeout_seconds)
                self._observe_queue_watermarks()
                return True
            except asyncio.TimeoutError:
                self.stats["outgoing_dropped"] += 1
                logger.error(
                    "MQTT outgoing queue enqueue timeout after %.2fs (source=%s)",
                    self.queue_put_timeout_seconds,
                    source,
                )
                return False

    def get_runtime_status(self) -> dict[str, Any]:
        now = time.time()
        connected_seconds = 0
        if self.connected and self.last_connected_at:
            connected_seconds = max(0, int(now - self.last_connected_at))
        reconnects_last_hour = sum(1 for ts in self.reconnect_events if (now - float(ts)) <= 3600.0)
        self._observe_queue_watermarks()
        in_size = self.mqtt_in_queue.qsize()
        out_size = self.mqtt_out_queue.qsize()
        in_max = int(getattr(self.mqtt_in_queue, "maxsize", 0) or 0)
        out_max = int(getattr(self.mqtt_out_queue, "maxsize", 0) or 0)
        return {
            "connected": bool(self.connected),
            "broker_host": self.broker_host,
            "broker_port": MQTT_PORT,
            "username_set": bool(MQTT_USERNAME),
            "base_topic": self.base_topic,
            "connected_since": self.last_connected_at,
            "disconnected_since": self.last_disconnected_at,
            "connected_seconds": connected_seconds,
            "reconnects_last_hour": reconnects_last_hour,
            "last_error": self.last_error,
            "queue": {
                "in_size": in_size,
                "out_size": out_size,
                "in_maxsize": in_max,
                "out_maxsize": out_max,
                "in_utilization_pct": round((in_size / in_max) * 100.0, 2) if in_max > 0 else None,
                "out_utilization_pct": round((out_size / out_max) * 100.0, 2) if out_max > 0 else None,
                "in_high_water": int(self.queue_metrics["in_high_water"]),
                "out_high_water": int(self.queue_metrics["out_high_water"]),
                "out_last_wait_ms": round(self.queue_metrics["out_last_wait_ms"], 2),
                "out_max_wait_ms": round(self.queue_metrics["out_max_wait_ms"], 2),
            },
            "stats": dict(self.stats),
            "publish_policy": {
                "max_retries": self.publish_max_retries,
                "retry_base_delay_seconds": self.publish_retry_base_delay_seconds,
                "retry_max_delay_seconds": self.publish_retry_max_delay_seconds,
                "queue_put_timeout_seconds": self.queue_put_timeout_seconds,
            },
            "recent_incoming_topics": list(self.recent_incoming_topics),
            "recent_outgoing_topics": list(self.recent_outgoing_topics),
            "recent_errors": list(self.recent_errors),
        }

    def _full_topic(self, suffix: str) -> str:
        suffix = str(suffix or "").strip().lstrip("/")
        if not suffix:
            return self.base_topic
        return f"{self.base_topic}/{suffix}"

    @staticmethod
    def _decode_payload(payload: Any) -> str:
        if isinstance(payload, bytes):
            return payload.decode("utf-8", errors="replace")
        return str(payload)

    @staticmethod
    def _parse_payload(payload_text: str) -> Any:
        stripped = payload_text.strip()
        if not stripped:
            return {}
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped

    @staticmethod
    def _is_connection_error(exc: Exception) -> bool:
        if isinstance(exc, (MqttError, ConnectionError, TimeoutError, OSError)):
            return True
        text = str(exc).lower()
        return any(token in text for token in ("connection", "disconnected", "not currently connected", "broken pipe"))

    def _parse_message_topic(self, topic: str) -> Optional[dict[str, str]]:
        topic_parts = str(topic or "").strip("/").split("/")
        base_parts = self.base_topic.strip("/").split("/")
        if len(topic_parts) < len(base_parts) + 1:
            return None
        if topic_parts[: len(base_parts)] != base_parts:
            return None

        tail = topic_parts[len(base_parts) :]
        # Endpoint topic: <base>/ep/<EUI>/<leaf>
        if len(tail) == 3 and tail[0] == "ep":
            return {"kind": "endpoint", "eui": tail[1].upper(), "leaf": tail[2]}

        # System config topic: <base>/config/<key>
        if len(tail) == 2 and tail[0] == "config":
            return {"kind": "system_config", "key": tail[1]}

        return None

    @staticmethod
    def _extract_base_station_targets(payload: Any) -> list[str]:
        if not isinstance(payload, dict):
            return []
        raw = payload.get("base_stations", payload.get("baseStations", payload.get("baseStationEuis")))
        if isinstance(raw, list):
            result = []
            for item in raw:
                value = str(item or "").strip().upper()
                if value and value not in result:
                    result.append(value)
            return result
        if isinstance(raw, str):
            result = []
            for item in raw.split(","):
                value = item.strip().upper()
                if value and value not in result:
                    result.append(value)
            return result
        return []

    async def _enqueue_ack(self, eui: str, payload: dict[str, Any]) -> None:
        ok = await self._enqueue_outgoing(
            {
                "topic": f"ep/{str(eui).upper()}/response",
                "payload": json.dumps(payload, separators=(",", ":")),
            },
            source="ack",
        )
        if not ok:
            logger.error("ACK dropped due to outgoing queue backpressure (eui=%s)", str(eui).upper())

    async def start(self) -> None:
        """Start MQTT bridge with auto-reconnect."""
        retry_delay = 5.0
        max_delay = 60.0

        while True:
            try:
                logger.info("MQTT connecting to %s:%s (user=%s)", self.broker_host, MQTT_PORT, MQTT_USERNAME or "<none>")

                async with Client(
                    hostname=self.broker_host,
                    port=MQTT_PORT,
                    username=MQTT_USERNAME or None,
                    password=MQTT_PASSWORD or None,
                    keepalive=60,
                    timeout=30,
                ) as client:
                    self.connected = True
                    self.last_connected_at = time.time()
                    had_connection_before = int(self.stats.get("connect_success", 0) or 0) > 0
                    self.stats["connect_success"] += 1
                    if had_connection_before:
                        self.stats["reconnect_count"] += 1
                        self.reconnect_events.append(self.last_connected_at)
                    retry_delay = 5.0
                    logger.info("MQTT connected")

                    # Connectivity marker topic (best effort)
                    try:
                        await client.publish(
                            self._full_topic("connection_test"),
                            json.dumps({"status": "connected", "timestamp": time.time()}, separators=(",", ":")),
                        )
                    except Exception as ping_err:
                        logger.debug("Connection test publish skipped: %s", ping_err)

                    tasks = [
                        asyncio.create_task(self._handle_incoming(client), name="mqtt-incoming"),
                        asyncio.create_task(self._handle_outgoing(client), name="mqtt-outgoing"),
                        asyncio.create_task(self._connection_health_monitor(client), name="mqtt-health"),
                    ]

                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                    first_error = None
                    for task in done:
                        exc = task.exception()
                        if exc is not None:
                            first_error = exc
                            break
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)

                    if first_error is not None:
                        raise first_error
                    raise RuntimeError("MQTT task stopped unexpectedly")

            except asyncio.CancelledError:
                self.connected = False
                self.last_disconnected_at = time.time()
                raise
            except Exception as exc:
                self.connected = False
                self.last_disconnected_at = time.time()
                self.stats["connect_failures"] += 1
                self._track_error("connect_loop", exc)
                logger.error("MQTT connection loop error (%s): %s", type(exc).__name__, exc)
                logger.info("MQTT reconnect in %.1f seconds", retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, max_delay)

    async def _handle_incoming(self, client: Client) -> None:
        topics = [
            (f"{self.base_topic}/ep/+/dl", 0),
            (f"{self.base_topic}/ep/+/cmd", 0),
            (f"{self.base_topic}/ep/+/register", 0),
            (f"{self.base_topic}/ep/+/config", 0),
            (f"{self.base_topic}/config/+", 0),
        ]
        await client.subscribe(topics)
        logger.info("MQTT subscriptions ready")

        async for message in client.messages:
            self.stats["incoming_total"] += 1
            try:
                topic = str(message.topic)
                self.recent_incoming_topics.append({"topic": topic, "timestamp": time.time()})
                route = self._parse_message_topic(topic)
                if not route:
                    logger.debug("Ignoring MQTT topic outside schema: %s", topic)
                    continue

                payload_text = self._decode_payload(message.payload)
                payload = self._parse_payload(payload_text)

                kind = route.get("kind")
                if kind == "system_config":
                    if not isinstance(payload, dict):
                        logger.warning("Ignoring system config message with non-JSON payload (topic=%s)", topic)
                        continue
                    config_msg = dict(payload)
                    config_msg["message_type"] = "system_config"
                    config_msg["config_key"] = route.get("key", "")
                    queued = await self._enqueue_incoming(config_msg, source="system_config")
                    if not queued:
                        logger.error("System config dropped due to incoming queue backpressure (topic=%s)", topic)
                    continue

                eui = route.get("eui", "").upper()
                leaf = route.get("leaf", "")
                if not eui:
                    logger.warning("Ignoring endpoint topic without EUI: %s", topic)
                    continue

                if leaf == "cmd":
                    await self.handle_command_message(topic, payload, eui)
                    continue

                if leaf == "register":
                    await self.handle_register_message(topic, payload, eui)
                    continue

                if leaf == "config":
                    if not isinstance(payload, dict):
                        logger.warning("Ignoring config for %s: expected JSON object", eui)
                        continue
                    config = dict(payload)
                    config["eui"] = eui
                    config["message_type"] = "config"
                    queued = await self._enqueue_incoming(config, source="config")
                    if not queued:
                        logger.error("Config dropped due to incoming queue backpressure (eui=%s)", eui)
                    continue

                if leaf == "dl":
                    logger.debug("Received downlink topic for %s", eui)
                    continue

                logger.debug("Unhandled endpoint leaf '%s' on topic %s", leaf, topic)
            except Exception as exc:
                self.stats["incoming_failed"] += 1
                self._track_error("incoming", exc)
                logger.error("MQTT incoming processing error (%s): %s", type(exc).__name__, exc)

    async def _connection_health_monitor(self, client: Client) -> None:
        while True:
            await asyncio.sleep(180)
            payload = json.dumps({"timestamp": time.time(), "status": "alive"}, separators=(",", ":"))
            await client.publish(self._full_topic("health_check"), payload)

    async def _handle_outgoing(self, client: Client) -> None:
        while True:
            msg: Optional[dict[str, Any]] = None
            got_item = False
            failure_counted = False
            try:
                msg = await self.mqtt_out_queue.get()
                got_item = True
                if not isinstance(msg, dict):
                    raise ValueError("Outgoing MQTT message must be a dict")

                not_before = float(msg.get("__next_retry_not_before", 0) or 0)
                if not_before > 0:
                    delay = max(0.0, not_before - time.time())
                    if delay > 0:
                        await asyncio.sleep(delay)

                first_queued_at = float(msg.get("__first_queued_at", time.time()) or time.time())
                retry_attempt = int(msg.get("__publish_retry_attempt", 0) or 0)

                topic_suffix = str(msg.get("topic", "")).strip().lstrip("/")
                if not topic_suffix:
                    raise ValueError("Outgoing MQTT message missing topic")

                payload_value = msg.get("payload", "")
                if isinstance(payload_value, (dict, list)):
                    payload = json.dumps(payload_value, separators=(",", ":"), ensure_ascii=False)
                elif isinstance(payload_value, str):
                    payload = payload_value
                else:
                    payload = str(payload_value)

                qos_raw = msg.get("qos", 0)
                qos = int(qos_raw) if isinstance(qos_raw, (int, str)) else 0
                qos = 0 if qos < 0 else (2 if qos > 2 else qos)
                retain = bool(msg.get("retain", False))

                full_topic = self._full_topic(topic_suffix)
                while True:
                    try:
                        await client.publish(full_topic, payload, qos=qos, retain=retain)
                        break
                    except Exception as exc:
                        self.stats["outgoing_failed"] += 1
                        failure_counted = True
                        self._track_error("outgoing", exc)

                        if retry_attempt >= self.publish_max_retries:
                            self.stats["outgoing_retry_exhausted"] += 1
                            self.stats["outgoing_dropped"] += 1
                            logger.error(
                                "MQTT publish dropped after retries topic=%s retries=%s error=%s",
                                full_topic,
                                retry_attempt,
                                exc,
                            )
                            raise

                        retry_attempt += 1
                        self.stats["outgoing_retried"] += 1
                        retry_delay = self._retry_delay(retry_attempt)
                        msg["__publish_retry_attempt"] = retry_attempt
                        msg["__next_retry_not_before"] = time.time() + retry_delay
                        logger.warning(
                            "MQTT publish failed topic=%s retry=%s/%s in %.2fs (%s)",
                            full_topic,
                            retry_attempt,
                            self.publish_max_retries,
                            retry_delay,
                            exc,
                        )

                        if self._is_connection_error(exc):
                            requeued = await self._enqueue_outgoing(msg, source="publish_connection_retry")
                            if requeued:
                                self.stats["outgoing_requeued"] += 1
                            raise

                        await asyncio.sleep(retry_delay)

                self.recent_outgoing_topics.append(
                    {
                        "topic": full_topic,
                        "timestamp": time.time(),
                        "qos": qos,
                        "retain": retain,
                        "retries": retry_attempt,
                    }
                )
                self.stats["outgoing_published"] += 1
                wait_ms = max(0.0, (time.time() - first_queued_at) * 1000.0)
                self.queue_metrics["out_last_wait_ms"] = wait_ms
                self.queue_metrics["out_max_wait_ms"] = max(self.queue_metrics["out_max_wait_ms"], wait_ms)
                logger.debug("MQTT published topic=%s qos=%s retain=%s", full_topic, qos, retain)
            except Exception as exc:
                if not failure_counted:
                    self.stats["outgoing_failed"] += 1
                self._track_error("outgoing", exc)
                if self._is_connection_error(exc):
                    logger.error("MQTT connection-related publish error (%s): %s", type(exc).__name__, exc)
                    raise
                logger.error("MQTT publish error (%s): %s", type(exc).__name__, exc)
            finally:
                if got_item:
                    try:
                        self.mqtt_out_queue.task_done()
                    except ValueError:
                        pass

    async def handle_command_message(self, topic: str, payload: Any, eui: str) -> None:
        """Handle command messages from MQTT <base>/ep/<EUI>/cmd."""
        try:
            if isinstance(payload, str):
                command = payload.strip().lower()
                source = "cmd_string"
                command_payload = {}
            elif isinstance(payload, dict):
                command = str(payload.get("command", payload.get("action", ""))).strip().lower()
                source = "cmd_json"
                command_payload = payload
            else:
                logger.warning("Invalid command payload type on %s: %s", topic, type(payload).__name__)
                return

            valid_commands = {"detach", "attach", "status"}
            if command not in valid_commands:
                logger.warning("Invalid command '%s' for %s (topic=%s)", command, eui, topic)
                await self._enqueue_ack(
                    eui,
                    {
                        "action": "error_response",
                        "sensor_eui": eui,
                        "error": f"Unknown command: {command}",
                        "timestamp": time.time(),
                    },
                )
                return

            command_msg: dict[str, Any] = {
                "message_type": "command",
                "eui": eui,
                "action": command,
                "source": source,
                "timestamp": command_payload.get("timestamp", time.time()),
            }
            selected_bs = self._extract_base_station_targets(command_payload)
            if selected_bs:
                command_msg["base_stations"] = selected_bs

            queued = await self._enqueue_incoming(command_msg, source="command")
            if not queued:
                await self._enqueue_ack(
                    eui,
                    {
                        "command": command,
                        "status": "rejected",
                        "sensor_eui": eui,
                        "error": "incoming queue is full",
                        "base_stations": selected_bs,
                        "timestamp": time.time(),
                    },
                )
                return

            await self._enqueue_ack(
                eui,
                {
                    "command": command,
                    "status": "received",
                    "sensor_eui": eui,
                    "base_stations": selected_bs,
                    "timestamp": time.time(),
                },
            )
        except Exception as exc:
            self._track_error("command_handler", exc)
            logger.error("Error handling command message (topic=%s eui=%s): %s", topic, eui, exc)

    async def handle_register_message(self, topic: str, payload: Any, eui: str) -> None:
        """Handle legacy register messages from MQTT <base>/ep/<EUI>/register."""
        try:
            if not isinstance(payload, dict):
                logger.error("Legacy register for %s ignored: expected JSON object on %s", eui, topic)
                await self._enqueue_ack(
                    eui,
                    {
                        "action": "legacy_register",
                        "status": "rejected",
                        "sensor_eui": eui,
                        "error": "payload must be JSON object",
                        "timestamp": time.time(),
                    },
                )
                return

            config = dict(payload)
            config["eui"] = eui.upper()
            config["message_type"] = "config"
            config["source"] = "legacy_register"

            required_fields = ("nwKey", "shortAddr")
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                logger.error("Legacy register missing required fields for %s: %s", eui, ", ".join(missing_fields))
                await self._enqueue_ack(
                    eui,
                    {
                        "action": "legacy_register",
                        "status": "rejected",
                        "sensor_eui": eui,
                        "error": f"missing fields: {','.join(missing_fields)}",
                        "timestamp": time.time(),
                    },
                )
                return

            if "bidi" not in config:
                config["bidi"] = False

            queued = await self._enqueue_incoming(config, source="legacy_register")
            if not queued:
                await self._enqueue_ack(
                    eui,
                    {
                        "action": "legacy_register",
                        "status": "rejected",
                        "sensor_eui": eui,
                        "error": "incoming queue is full",
                        "timestamp": time.time(),
                    },
                )
                return
            await self._enqueue_ack(
                eui,
                {
                    "action": "legacy_register",
                    "status": "received",
                    "sensor_eui": eui,
                    "timestamp": time.time(),
                },
            )
        except Exception as exc:
            self._track_error("register_handler", exc)
            logger.error("Error handling legacy register (topic=%s eui=%s): %s", topic, eui, exc)
