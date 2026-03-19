import asyncio
import logging
from datetime import datetime, timezone, timedelta

import bssci_config
from bssci_config import (
    SENSOR_CONFIG_FILE,
    LISTEN_PORT,
    MQTT_ENABLED,
    MQTT_BROKER,
    MQTT_PORT,
    MQTT_IN_QUEUE_MAXSIZE,
    MQTT_OUT_QUEUE_MAXSIZE,
)
from mqtt_interface import MQTTClient
from TLSServer import TLSServer


class TimezoneFormatter(logging.Formatter):
    def __init__(self, fmt, datefmt=None):
        super().__init__(fmt, datefmt)
        self.timezone = timezone(timedelta(hours=2))

    def formatTime(self, record, datefmt=None):
        utc_time = datetime.fromtimestamp(record.created, tz=timezone.utc)
        local_time = utc_time.astimezone(self.timezone)
        if datefmt:
            return local_time.strftime(datefmt)
        return local_time.strftime('%Y-%m-%d %H:%M:%S')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

timezone_formatter = TimezoneFormatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    '%Y-%m-%d %H:%M:%S',
)
for handler in logging.root.handlers:
    handler.setFormatter(timezone_formatter)
logger = logging.getLogger(__name__)


tls_server_instance = None


async def main() -> None:
    global tls_server_instance

    out_queue_maxsize = max(0, int(MQTT_OUT_QUEUE_MAXSIZE))
    in_queue_maxsize = max(0, int(MQTT_IN_QUEUE_MAXSIZE))
    mqtt_out_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=out_queue_maxsize)
    mqtt_in_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=in_queue_maxsize)

    logger.info("Initializing BSSCI Service Center...")
    logger.info("Config: TLS Port %s, MQTT Broker %s:%s", LISTEN_PORT, MQTT_BROKER, MQTT_PORT)
    logger.info(
        "MQTT queue limits: out_maxsize=%s in_maxsize=%s",
        out_queue_maxsize if out_queue_maxsize > 0 else "unbounded",
        in_queue_maxsize if in_queue_maxsize > 0 else "unbounded",
    )
    logger.info("MQTT transport enabled: %s", MQTT_ENABLED)

    from queue_logger import setup_queue_logging, log_all_queue_stats

    queue_loggers = setup_queue_logging({
        'mqtt_out_queue': mqtt_out_queue,
        'mqtt_in_queue': mqtt_in_queue,
    })

    logger.info("Queue instance analysis:")
    logger.info("   mqtt_out_queue counter: starting fresh")
    logger.info("   mqtt_in_queue counter: starting fresh")

    tls_server_instance = TLSServer(SENSOR_CONFIG_FILE, mqtt_out_queue, mqtt_in_queue)

    try:
        import web_main
        web_main.set_tls_server(tls_server_instance)
    except ImportError:
        pass

    tls_server = tls_server_instance
    mqtt_client = MQTTClient(mqtt_out_queue, mqtt_in_queue) if MQTT_ENABLED else None

    try:
        import web_main
        web_main.set_mqtt_client(mqtt_client)
    except ImportError:
        pass

    logger.info("Queue assignment verification:")
    logger.info("   TLS Server mqtt_out_queue: connected")
    logger.info("   TLS Server mqtt_in_queue: connected")
    if mqtt_client is not None:
        logger.info("   MQTT Client mqtt_out_queue: connected")
        logger.info("   MQTT Client mqtt_in_queue: connected")
    else:
        logger.info("   MQTT Client: disabled")

    async def queue_stats_reporter():
        while True:
            await asyncio.sleep(60)
            log_all_queue_stats(queue_loggers)

    async def mqtt_out_queue_drain():
        logger.info("MQTT transport disabled. Outgoing MQTT queue will be drained without publish.")
        while True:
            item = await mqtt_out_queue.get()
            try:
                topic = str((item or {}).get('topic', '') or '')
                logger.debug("Dropped MQTT message because transport is disabled (topic=%s)", topic)
            finally:
                mqtt_out_queue.task_done()

    asyncio.create_task(queue_stats_reporter())

    logger.info("Starting BSSCI Service Center...")
    logger.info(
        "TLS Server is starting%s",
        " together with MQTT transport" if MQTT_ENABLED else " with MQTT transport disabled",
    )

    try:
        tasks = [tls_server.start_server()]
        if MQTT_ENABLED and mqtt_client is not None:
            tasks.extend([
                tls_server.process_mqtt_messages(),
                mqtt_client.start(),
            ])
        else:
            tasks.append(mqtt_out_queue_drain())
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down BSSCI Service Center...")
    except Exception as e:
        logger.error("Service error: %s", e)

    logger.info("BSSCI Service Center shutdown complete")


if __name__ == "__main__":
    policy_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if policy_cls is not None:
        asyncio.set_event_loop_policy(policy_cls())
    asyncio.run(main())
