import asyncio
import logging
import threading
import time
import os
import web_ui
from web_ui import app
from main import main as bssci_main

# Configure logging with timezone
from datetime import datetime, timezone, timedelta

# Check for Synology Docker mode
if os.getenv('SYNOLOGY_DOCKER') == '1':
    print("=" * 50)
    print("SYNOLOGY DOCKER MODE DETECTED")
    print("=" * 50)
    print("Configuration files have been copied to writable container locations.")
    print("Changes made via web UI will be container-local only.")
    print("To persist changes, backup config files after making changes:")
    print("  docker cp bssci-service-center:/app/bssci_config.py ./bssci_config.py")
    print("  docker cp bssci-service-center:/app/endpoints.json ./endpoints.json")
    print("=" * 50)

class TimezoneFormatter(logging.Formatter):
    def __init__(self, fmt, datefmt=None):
        super().__init__(fmt, datefmt)
        # Set timezone to UTC+2 (Central European Time)
        self.timezone = timezone(timedelta(hours=2))

    def formatTime(self, record, datefmt=None):
        # Convert UTC timestamp to local timezone
        utc_time = datetime.fromtimestamp(record.created, tz=timezone.utc)
        local_time = utc_time.astimezone(self.timezone)
        if datefmt:
            return local_time.strftime(datefmt)
        else:
            return local_time.strftime('%Y-%m-%d %H:%M:%S')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)

# Apply timezone formatter to all handlers
timezone_formatter = TimezoneFormatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    '%Y-%m-%d %H:%M:%S'
)
for handler in logging.root.handlers:
    handler.setFormatter(timezone_formatter)

# Re-attach in-memory web log handler after force-reset of logging handlers.
try:
    web_ui.ensure_web_log_handler()
except Exception as e:
    logging.getLogger(__name__).warning(f"Could not attach web log handler: {e}")

logger = logging.getLogger(__name__)

# Global references for runtime service instances
tls_server_instance = None
mqtt_client_instance = None

def fix_env_file_permissions():
    """Ensure .env file has proper permissions for configuration updates"""
    env_file = '.env'
    try:
        if os.path.exists(env_file):
            # Check if file is writable
            if not os.access(env_file, os.W_OK):
                logger.info("đź”§ Fixing .env file permissions...")
                # Try to make it writable
                current_mode = os.stat(env_file).st_mode
                os.chmod(env_file, current_mode | 0o666)
                logger.info("âś… .env file permissions fixed")
            else:
                logger.info("âś… .env file permissions OK")
        else:
            # Create .env file if it doesn't exist
            logger.info("đź“ Creating .env file...")
            with open(env_file, 'a'):
                pass
            os.chmod(env_file, 0o666)
            logger.info("âś… .env file created with proper permissions")
    except Exception as e:
        logger.warning(f"âš ď¸Ź  Warning: Could not fix .env permissions: {e}")
        logger.warning("   Configuration updates may fail in some environments")

def run_web_ui():
    """Run the Flask web UI in a separate thread"""
    logger.info("Starting Web UI on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

def run_bssci_service():
    """Run the BSSCI service"""
    logger.info("Starting BSSCI Service")
    import main
    asyncio.run(main.main())

def get_tls_server():
    """Get the TLS server instance"""
    global tls_server_instance
    return tls_server_instance

def set_tls_server(server):
    """Set the TLS server instance"""
    global tls_server_instance
    tls_server_instance = server

    # Make it available to web_ui - this is critical for web interface functionality
    try:
        import web_ui
        web_ui.set_tls_server(tls_server_instance)
        logger.info(f"âś… TLS server instance passed to web UI successfully")
        logger.info(f"   TLS server ID: {id(tls_server_instance)}")
        if hasattr(tls_server_instance, 'connected_base_stations'):
            logger.info(f"   Connected base stations count: {len(tls_server_instance.connected_base_stations)}")
    except ImportError as e:
        logger.error(f"âťŚ Failed to import web_ui: {e}")
    except Exception as e:
        logger.error(f"âťŚ Failed to set TLS server in web_ui: {e}")


def set_mqtt_client(client):
    """Set the MQTT client instance and expose it to web_ui."""
    global mqtt_client_instance
    mqtt_client_instance = client

    try:
        import web_ui
        web_ui.set_mqtt_client(mqtt_client_instance)
        logger.info("MQTT client instance passed to web UI successfully")
    except ImportError as e:
        logger.error(f"Failed to import web_ui: {e}")
    except Exception as e:
        logger.error(f"Failed to set MQTT client in web_ui: {e}")


if __name__ == "__main__":
    logger.info("Starting BSSCI Service Center with Web UI")

    # Fix .env file permissions at startup
    fix_env_file_permissions()

    # Start web UI in a separate thread
    web_thread = threading.Thread(target=run_web_ui, daemon=True)
    web_thread.start()

    # Give web UI time to start
    time.sleep(2)
    logger.info("Web UI available at http://localhost:5000")

    # Run BSSCI service in main thread
    try:
        run_bssci_service()
    except KeyboardInterrupt:
        logger.info("Shutting down...")

