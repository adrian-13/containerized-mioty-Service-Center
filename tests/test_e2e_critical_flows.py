import asyncio
import json
import os
import tempfile
import unittest

import web_ui
from flask import session


class _FakeMqttClient:
    def __init__(self, base_topic="mioty"):
        self.connected = True
        self.base_topic = base_topic
        self.mqtt_out_queue = asyncio.Queue(maxsize=32)


class CriticalFlowsE2ETest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(lambda: os.chdir(self._old_cwd))

        os.makedirs("logs", exist_ok=True)

        self.sensor_file = os.path.join(self._tmp.name, "endpoints.json")
        self.bs_file = os.path.join(self._tmp.name, "base_stations.json")
        self.positions_file = os.path.join(self._tmp.name, "coverage_positions.json")
        self.users_file = os.path.join(self._tmp.name, "users.json")
        self.audit_file = os.path.join(self._tmp.name, "logs", "admin_audit.jsonl")

        self._write_json(
            self.users_file,
            {
                "users": {
                    "admin": {
                        "name": "Administrator",
                        "role": "admin",
                        "tenant_id": "",
                    },
                    "tenant_user": {
                        "name": "Tenant User",
                        "role": "user",
                        "tenant_id": "tenant-a",
                    },
                },
                "role_permissions": {
                    "admin": {
                        "can_edit_sensors": True,
                        "can_edit_config": True,
                        "can_manage_certificates": True,
                    },
                    "user": {
                        "can_edit_sensors": True,
                        "can_edit_config": False,
                        "can_manage_certificates": False,
                    },
                },
            },
        )

        self._write_json(
            self.sensor_file,
            [
                {
                    "eui": "00124B001CBCE171",
                    "nwKey": "00112233445566778899AABBCCDDEEFF",
                    "shortAddr": "A171",
                    "bidi": False,
                    "name": "Default tenant sensor",
                    "tenant_id": "default",
                },
                {
                    "eui": "00124B001CBCE172",
                    "nwKey": "00112233445566778899AABBCCDDEE00",
                    "shortAddr": "A172",
                    "bidi": False,
                    "name": "Tenant A sensor",
                    "tenant_id": "tenant-a",
                },
            ],
        )

        self._write_json(
            self.bs_file,
            {
                "base_stations": {
                    "129AF3FFFE01F124": {
                        "name": "Default tenant BS",
                        "ip": "192.168.1.10",
                        "tenant_id": "default",
                    },
                    "129AF3FFFE01F125": {
                        "name": "Tenant A BS",
                        "ip": "192.168.1.11",
                        "tenant_id": "tenant-a",
                    },
                }
            },
        )

        self._write_json(self.positions_file, {"positions": {}})

        self._old_sensor_cfg = getattr(web_ui.bssci_config, "SENSOR_CONFIG_FILE", "endpoints.json")
        self._old_bs_cfg = getattr(web_ui.bssci_config, "BASE_STATION_CONFIG_FILE", "base_stations.json")
        self._old_default_tenant = getattr(web_ui.bssci_config, "TIMESCALE_DEFAULT_TENANT", "default")
        web_ui.bssci_config.SENSOR_CONFIG_FILE = self.sensor_file
        web_ui.bssci_config.BASE_STATION_CONFIG_FILE = self.bs_file
        web_ui.bssci_config.TIMESCALE_DEFAULT_TENANT = "default"

        self._old_tls = getattr(web_ui, "tls_server_instance", None)
        self._old_mqtt = getattr(web_ui, "mqtt_client_instance", None)
        self._old_audit_file = web_ui.admin_audit_log_file
        self._old_audit_entries = list(web_ui.admin_audit_entries)
        web_ui.tls_server_instance = None
        web_ui.mqtt_client_instance = None
        web_ui.admin_audit_log_file = self.audit_file
        web_ui.admin_audit_entries = []

        self.addCleanup(self._restore_globals)

        web_ui.app.config["TESTING"] = True
        self.client = web_ui.app.test_client()

    def _restore_globals(self):
        web_ui.bssci_config.SENSOR_CONFIG_FILE = self._old_sensor_cfg
        web_ui.bssci_config.BASE_STATION_CONFIG_FILE = self._old_bs_cfg
        web_ui.bssci_config.TIMESCALE_DEFAULT_TENANT = self._old_default_tenant
        web_ui.tls_server_instance = self._old_tls
        web_ui.mqtt_client_instance = self._old_mqtt
        web_ui.admin_audit_log_file = self._old_audit_file
        web_ui.admin_audit_entries = self._old_audit_entries

    def _write_json(self, path, payload):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def _read_json(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _login(self, username, role, tenant_id):
        with self.client.session_transaction() as sess:
            sess["username"] = username
            sess["role"] = role
            sess["tenant_id"] = tenant_id

    def test_attach_and_detach_sensor_mapping(self):
        self._login("admin", "admin", "default")
        sensor_eui = "00124B001CBCE171"
        bs_eui = "129AF3FFFE01F124"

        resp = self.client.post(
            f"/api/sensors/{sensor_eui}/attach",
            json={"base_stations": [bs_eui]},
        )
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body.get("success"))

        sensors_after_attach = self._read_json(self.sensor_file)
        sensor_row = next(s for s in sensors_after_attach if s["eui"] == sensor_eui)
        self.assertEqual(sensor_row.get("attached_base_stations"), [bs_eui])

        resp = self.client.post(f"/api/sensors/{sensor_eui}/detach", json={})
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body.get("success"))

        sensors_after_detach = self._read_json(self.sensor_file)
        sensor_row = next(s for s in sensors_after_detach if s["eui"] == sensor_eui)
        self.assertNotIn("attached_base_stations", sensor_row)

    def test_coverage_positions_syncs_gps_to_sensor_and_base_station(self):
        self._login("admin", "admin", "default")
        sensor_eui = "00124B001CBCE171"
        bs_eui = "129AF3FFFE01F124"
        payload = {
            "positions": {
                f"sensor_{sensor_eui}": {
                    "type": "osm",
                    "deviceType": "sensor",
                    "eui": sensor_eui,
                    "lat": 48.123456,
                    "lng": 17.654321,
                },
                f"bs_{bs_eui}": {
                    "type": "osm",
                    "deviceType": "bs",
                    "eui": bs_eui,
                    "lat": 48.765432,
                    "lng": 17.111111,
                },
            }
        }

        resp = self.client.post("/api/coverage/positions", json=payload)
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body.get("success"))

        sensors = self._read_json(self.sensor_file)
        sensor_row = next(s for s in sensors if s["eui"] == sensor_eui)
        self.assertEqual(sensor_row.get("gps_lat"), 48.123456)
        self.assertEqual(sensor_row.get("gps_lng"), 17.654321)

        base_cfg = self._read_json(self.bs_file)
        bs_row = base_cfg["base_stations"][bs_eui]
        self.assertEqual(bs_row.get("gps_lat"), 48.765432)
        self.assertEqual(bs_row.get("gps_lng"), 17.111111)

        resp = self.client.get("/api/coverage/positions")
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        state = resp.get_json()
        self.assertIn(f"sensor_{sensor_eui}", state.get("positions", {}))
        self.assertIn(f"bs_{bs_eui}", state.get("positions", {}))

    def test_tenant_filtering_for_sensors_and_base_stations(self):
        self._login("tenant_user", "user", "tenant-a")

        resp = self.client.get("/api/sensors")
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        sensor_map = resp.get_json() or {}
        self.assertEqual(set(sensor_map.keys()), {"00124B001CBCE172"})
        self.assertEqual(sensor_map["00124B001CBCE172"].get("tenant_id"), "tenant-a")

        resp = self.client.get("/api/base-stations")
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        bs_payload = resp.get_json() or {}
        self.assertTrue(bs_payload.get("success"))
        euis = {row.get("eui") for row in bs_payload.get("base_stations", [])}
        self.assertEqual(euis, {"129af3fffe01f125"})

    def test_sensor_marker_color_persists_and_is_returned_by_api(self):
        self._write_json(
            self.users_file,
            {
                "users": {
                    "admin": {
                        "name": "Administrator",
                        "role": "admin",
                        "tenant_id": "",
                        "password": "rotated-admin-pass",
                        "require_password_change": False,
                        "bootstrap_password_state": "rotated",
                    },
                },
                "role_permissions": {
                    "admin": {
                        "can_edit_sensors": True,
                        "can_edit_config": True,
                        "can_manage_certificates": True,
                    }
                },
            },
        )
        self._write_json(
            self.sensor_file,
            [
                {
                    "eui": "00124B001CBCE171",
                    "nwKey": "00112233445566778899AABBCCDDEEFF",
                    "shortAddr": "A171",
                    "bidi": False,
                    "name": "Default tenant sensor",
                    "tenant_id": "default",
                }
            ],
        )

        self._login("admin", "admin", "default")
        sensor_eui = "00124B001CBCE171"

        resp = self.client.post(
            f"/api/sensors/{sensor_eui}/marker-color",
            json={"color": "#123456"},
        )
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body.get("success"), body)
        self.assertEqual(body.get("marker_color"), "#123456")

        sensors_after = self._read_json(self.sensor_file)
        sensor_row = next(s for s in sensors_after if s["eui"] == sensor_eui)
        self.assertEqual(sensor_row.get("marker_color"), "#123456")

        resp = self.client.get("/api/sensors")
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        sensor_map = resp.get_json() or {}
        self.assertEqual(sensor_map[sensor_eui].get("marker_color"), "#123456")

        resp = self.client.post(
            f"/api/sensors/{sensor_eui}/marker-color",
            json={"color": ""},
        )
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body.get("success"), body)
        self.assertIsNone(body.get("marker_color"))

    def test_mini_marker_color_popup_is_deferred(self):
        template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates", "index.html")
        with open(template_path, "r", encoding="utf-8") as fh:
            template = fh.read()

        self.assertRegex(
            template,
            r"setTimeout\(\(\) => \{\s*showMiniColorPop\(clientX, clientY, currentColor \|\| null, \(color\) => \{",
        )

    def test_shared_color_helpers_are_available_before_branch_split(self):
        template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates", "index.html")
        with open(template_path, "r", encoding="utf-8") as fh:
            template = fh.read()

        scripts_start = template.index("{% block scripts %}")
        branch_split = template.index("{% if is_customer_user %}", scripts_start)
        self.assertLess(template.index("function showColorPop"), branch_split)
        self.assertLess(template.index("function persistSensorMarkerColor"), branch_split)

    def test_global_admin_dashboard_cache_key_is_distinct_from_default_scope(self):
        with web_ui.app.test_request_context("/api/customer/dashboard/runtime"):
            session["username"] = "admin"
            session["role"] = "admin"
            session["tenant_id"] = ""
            global_key = web_ui._customer_dashboard_cache_key("")

        with web_ui.app.test_request_context("/api/customer/dashboard/runtime"):
            session["username"] = "tenant_user"
            session["role"] = "user"
            session["tenant_id"] = "default"
            default_key = web_ui._customer_dashboard_cache_key("default")

        self.assertNotEqual(global_key, default_key)
        self.assertIn("__global__", global_key)

    def test_demo_tenant_detected_from_inventory_without_registry_entry(self):
        self._write_json(
            self.sensor_file,
            [
                {
                    "eui": "00124B001CBCE171",
                    "nwKey": "00112233445566778899AABBCCDDEEFF",
                    "shortAddr": "A171",
                    "bidi": False,
                    "name": "Default tenant sensor",
                    "tenant_id": "default",
                    "gps_lat": 48.1,
                    "gps_lng": 17.1,
                },
                {
                    "eui": "00124B001CBCE199",
                    "nwKey": "00112233445566778899AABBCCDDEE99",
                    "shortAddr": "A199",
                    "bidi": False,
                    "name": "Implicit demo sensor",
                    "tenant_id": "test",
                    "gps_lat": 48.2,
                    "gps_lng": 17.2,
                },
            ],
        )

        # Non-demo user (admin) must never see demo-tenant sensors, regardless
        # of any URL flag — demo data is now bound exclusively to the dedicated
        # demo account.
        with web_ui.app.test_request_context("/api/customer/dashboard/runtime"):
            session["username"] = "admin"
            session["role"] = "admin"
            session["tenant_id"] = ""
            hidden_payload = web_ui._build_customer_dashboard_runtime_payload()
            hidden_nodes = hidden_payload["topologyData"]["nodes"]

        with web_ui.app.test_request_context("/api/customer/dashboard/runtime?include_demo=1"):
            session["username"] = "admin"
            session["role"] = "admin"
            session["tenant_id"] = ""
            still_hidden_payload = web_ui._build_customer_dashboard_runtime_payload()
            still_hidden_nodes = still_hidden_payload["topologyData"]["nodes"]

        # The dedicated demo account is the only context in which demo
        # inventory becomes visible.
        with web_ui.app.test_request_context("/api/customer/dashboard/runtime"):
            session["username"] = "test"
            session["role"] = "viewer"
            session["tenant_id"] = "test"
            visible_payload = web_ui._build_customer_dashboard_runtime_payload()
            visible_nodes = visible_payload["topologyData"]["nodes"]

        hidden_sensor_euis = {node.get("eui") for node in hidden_nodes if node.get("type") == "sensor"}
        still_hidden_sensor_euis = {node.get("eui") for node in still_hidden_nodes if node.get("type") == "sensor"}
        visible_sensor_euis = {node.get("eui") for node in visible_nodes if node.get("type") == "sensor"}
        self.assertNotIn("00124B001CBCE199", hidden_sensor_euis)
        self.assertNotIn("00124B001CBCE199", still_hidden_sensor_euis)
        self.assertIn("00124B001CBCE199", visible_sensor_euis)

    def test_payload_decoders_route_is_separate_from_administration(self):
        self._login("admin", "admin", "default")

        admin_resp = self.client.get("/administration")
        self.assertEqual(admin_resp.status_code, 200, admin_resp.get_data(as_text=True))
        admin_html = admin_resp.get_data(as_text=True)
        self.assertNotIn('data-admin-tab-btn="decoders"', admin_html)

        decoder_resp = self.client.get("/payload-decoders")
        self.assertEqual(decoder_resp.status_code, 200, decoder_resp.get_data(as_text=True))
        decoder_html = decoder_resp.get_data(as_text=True)
        self.assertIn('id="decoderProfileList"', decoder_html)

    def test_payload_decoders_route_requires_manage_system_scope(self):
        self._login("tenant_user", "user", "tenant-a")

        resp = self.client.get("/payload-decoders")
        self.assertEqual(resp.status_code, 302, resp.get_data(as_text=True))
        self.assertTrue((resp.headers.get("Location") or "").endswith("/"))

    def test_mqtt_test_publish_queues_message(self):
        self._login("admin", "admin", "default")
        fake_mqtt = _FakeMqttClient(base_topic="mioty")
        web_ui.mqtt_client_instance = fake_mqtt

        resp = self.client.post(
            "/api/mqtt/publish-test",
            json={
                "topic": "mioty/e2e/test",
                "payload_mode": "text",
                "payload": "hello-e2e",
                "qos": 1,
                "retain": True,
            },
        )
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body.get("success"))
        self.assertEqual(body.get("topic"), "mioty/e2e/test")

        queued = fake_mqtt.mqtt_out_queue.get_nowait()
        self.assertEqual(queued.get("topic"), "e2e/test")
        self.assertEqual(queued.get("payload"), "hello-e2e")
        self.assertEqual(queued.get("qos"), 1)
        self.assertTrue(queued.get("retain"))

    def test_admin_audit_log_contains_mqtt_publish_action(self):
        self._login("admin", "admin", "default")
        web_ui.mqtt_client_instance = _FakeMqttClient(base_topic="mioty")

        publish_resp = self.client.post(
            "/api/mqtt/publish-test",
            json={
                "topic": "mioty/audit/check",
                "payload_mode": "text",
                "payload": "audit-entry",
            },
        )
        self.assertEqual(publish_resp.status_code, 200, publish_resp.get_data(as_text=True))
        self.assertTrue(publish_resp.get_json().get("success"))

        resp = self.client.get("/api/audit/logs?action=mqtt.publish_test&entity=mqtt&limit=50")
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json() or {}
        self.assertTrue(body.get("success"))
        entries = body.get("entries", [])
        self.assertTrue(entries, "Expected at least one audit entry")
        self.assertEqual(entries[0].get("action"), "mqtt.publish_test")
        self.assertEqual(entries[0].get("entity"), "mqtt")


if __name__ == "__main__":
    unittest.main(verbosity=2)
