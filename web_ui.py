import csv
import copy
import secrets
import io
import json
import logging
import math
import os
import base64
import queue
import re
import subprocess
import shutil
import threading
import time
import tempfile
import zipfile
from collections import deque
import urllib.parse
import urllib.error
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, Response, session, send_file, has_request_context, make_response, g
from functools import wraps
from typing import List, Dict, Any, Optional
import bssci_config
try:
    import psycopg
except Exception:
    psycopg = None
try:
    from werkzeug.security import check_password_hash, generate_password_hash
except Exception:
    check_password_hash = None
    generate_password_hash = None

# Global service instance references
tls_server_instance = None
mqtt_client_instance = None

# Uptime tracking
bs_uptime_events = {}
_last_known_bs_status = {}
_influx_snapshot_thread = None
_influx_snapshot_stop = threading.Event()
_timescale_schema_lock = threading.Lock()
_timescale_schema_ready = False
_TIMESCALE_UPLINK_QUEUE_MAXSIZE = max(100, int(getattr(bssci_config, "TIMESCALE_UPLINK_QUEUE_MAXSIZE", 10000) or 10000))
_TIMESCALE_UPLINK_BATCH_SIZE = max(1, int(getattr(bssci_config, "TIMESCALE_UPLINK_BATCH_SIZE", 200) or 200))
_TIMESCALE_UPLINK_MAX_RETRIES = max(0, int(getattr(bssci_config, "TIMESCALE_UPLINK_WRITE_MAX_RETRIES", 5) or 5))
_TIMESCALE_UPLINK_RETRY_BASE_SECONDS = max(
    0.1, float(getattr(bssci_config, "TIMESCALE_UPLINK_RETRY_BASE_SECONDS", 0.5) or 0.5)
)
_TIMESCALE_UPLINK_RETRY_MAX_SECONDS = max(
    _TIMESCALE_UPLINK_RETRY_BASE_SECONDS,
    float(getattr(bssci_config, "TIMESCALE_UPLINK_RETRY_MAX_SECONDS", 10.0) or 10.0),
)
_timescale_uplink_queue = queue.Queue(maxsize=_TIMESCALE_UPLINK_QUEUE_MAXSIZE)
_timescale_uplink_thread = None
_timescale_uplink_stop = threading.Event()
_timescale_uplink_lock = threading.Lock()
_viewer_demo_seed_lock = threading.Lock()
_viewer_demo_seed_state = {
    "seeded": False,
    "last_attempt_ts": 0.0,
    "last_error": "",
}
_viewer_sensor_list_cache_lock = threading.Lock()
_viewer_sensor_list_cache = {}
_VIEWER_SENSOR_LIST_CACHE_TTL_SECONDS = 15.0
_customer_dashboard_cache_lock = threading.Lock()
_customer_dashboard_cache = {}
_CUSTOMER_DASHBOARD_CACHE_TTL_SECONDS = 5.0
_customer_dashboard_summary_cache_lock = threading.Lock()
_customer_dashboard_summary_cache = {}
_CUSTOMER_DASHBOARD_SUMMARY_CACHE_TTL_SECONDS = 5.0
_customer_dashboard_runtime_cache_lock = threading.Lock()
_customer_dashboard_runtime_cache = {}
_CUSTOMER_DASHBOARD_RUNTIME_CACHE_TTL_SECONDS = 10.0
_incident_feed_cache_lock = threading.Lock()
_incident_feed_cache = {}
_INCIDENT_FEED_CACHE_TTL_SECONDS = 5.0
_timescale_uplink_stats = {
    "queued": 0,
    "written": 0,
    "failed_batches": 0,
    "dropped": 0,
    "dropped_write_failures": 0,
    "dropped_invalid": 0,
    "retry_attempts": 0,
    "retried_batches": 0,
    "backpressure_events": 0,
    "queue_high_water": 0,
    "last_retry_ts": 0.0,
    "last_retry_delay_sec": 0.0,
    "last_error": "",
    "last_write_ts": 0.0,
    "write_latency_last_ms": 0.0,
    "write_latency_avg_ms": 0.0,
    "write_latency_max_ms": 0.0,
    "write_latency_samples": 0,
    "write_latency_total_ms": 0.0,
    "max_retries": _TIMESCALE_UPLINK_MAX_RETRIES,
    "batch_size": _TIMESCALE_UPLINK_BATCH_SIZE,
}
_timescale_uplink_stats_lock = threading.Lock()

_APP_LANGUAGE_OPTIONS = {
    "en": {
        "label": "English",
        "native_label": "English",
        "locale": "en-US",
    },
    "sk": {
        "label": "Slovak",
        "native_label": "Slovenčina",
        "locale": "sk-SK",
    },
}

_UI_TRANSLATIONS = {
    "en": {},
    "sk": {
        "page.dashboard": "Dashboard",
        "page.sensors": "Senzory",
        "page.base_stations": "Základňové stanice",
        "page.system_health": "Stav systému",
        "page.network_topology": "Topológia siete",
        "page.coverage_map": "Mapa pokrytia",
        "page.mqtt": "MQTT",
        "page.sensor_telemetry": "Telemetria senzorov",
        "page.logs": "Logy",
        "page.access_tenants": "Prístupy a tenancy",
        "page.configuration": "Konfigurácia",
        "page.certificates": "Certifikáty",
        "page.documentation": "Dokumentácia",
        "page.administration": "Administrácia",
        "page.login": "Prihlásenie",
        "page.sensor_detail": "Detail senzora",
        "page.base_station_detail": "Detail základňovej stanice",
        "nav.overview": "Prehľad",
        "nav.devices": "Zariadenia",
        "nav.monitoring": "Monitoring",
        "nav.administration": "Administrácia",
        "nav.dashboard": "Dashboard",
        "nav.sensors": "Senzory",
        "nav.base_stations": "Základňové stanice",
        "nav.oms": "OMS",
        "nav.system_health": "Stav systému",
        "nav.network_topology": "Topológia siete",
        "nav.coverage_map": "Mapa pokrytia",
        "nav.mqtt": "MQTT",
        "nav.sensor_telemetry": "Telemetria senzorov",
        "nav.logs": "Logy",
        "nav.access_tenants": "Prístupy a tenancy",
        "nav.configuration": "Konfigurácia",
        "nav.certificates": "Certifikáty",
        "nav.documentation": "Dokumentácia",
        "auth.log_out": "Odhlásiť sa",
        "auth.role_admin": "Admin",
        "auth.role_customer": "Zákazník",
        "auth.role_user": "Používateľ",
        "brand.subtitle": "MIOTY operačné centrum",
        "topbar.workspace_subtitle": "Prevádzkové rozhranie MIOTY",
        "topbar.toggle_navigation": "Prepnúť navigáciu",
        "common.current_application_time": "Aktuálny čas aplikácie",
        "common.close": "Zavrieť",
        "common.cancel": "Zrušiť",
        "common.confirm": "Potvrdiť",
        "toast.success": "Úspech",
        "toast.info": "Informácia",
        "toast.warning": "Upozornenie",
        "toast.error": "Chyba",
        "common.done": "Hotovo",
        "common.reset": "Obnoviť",
        "common.loading": "Načítava sa...",
        "common.refresh": "Obnoviť",
        "common.edit": "Upraviť",
        "common.save": "Uložiť",
        "common.delete": "Vymazať",
        "common.back": "Späť",
        "common.details": "Detail",
        "common.expanded": "Rozšírený",
        "common.overview": "Prehľad",
        "common.telemetry": "Telemetria",
        "common.debug": "Debug",
        "common.audit": "Audit",
        "common.certificates": "Certifikáty",
        "common.linked_sensors": "Pripojené senzory",
        "common.yes": "Áno",
        "common.no": "Nie",
        "common.connected": "Pripojené",
        "common.connecting": "Pripája sa",
        "common.offline": "Offline",
        "common.unknown": "Neznáme",
        "common.not_set": "Nenastavené",
        "common.disabled": "Vypnuté",
        "common.not_configured": "Nenakonfigurované",
        "common.not_available": "Nedostupné",
        "common.no_data_yet": "Zatiaľ bez dát",
        "common.no_tags": "Bez tagov",
        "common.present": "Prítomné",
        "common.missing": "Chýba",
        "common.primary": "Primárna",
        "common.time": "Čas",
        "common.status": "Stav",
        "common.runtime": "Runtime",
        "common.configuration": "Konfigurácia",
        "common.language": "Jazyk",
        "common.navigation": "Navigácia",
        "common.super_admin": "Super admin",
        "display.settings": "Nastavenia zobrazenia",
        "display.ui_zoom": "Priblíženie rozhrania",
        "display.zoom_note": "Prispôsobí text a rozostupy pre menšie obrazovky alebo vysoké DPI.",
        "display.layout_density": "Hustota rozloženia",
        "display.density_note": "Určuje, ako kompaktne budú panely a navigácia rozložené.",
        "display.color_scheme": "Farebná schéma",
        "display.theme_note": "Použije svetlý farebný variant v celom rozhraní.",
        "display.language_note": "Prepína texty rozhrania a formátovanie času. Po zmene sa stránka obnoví.",
        "display.auto_recommended": "Auto (odporúčané)",
        "display.comfortable": "Pohodlné",
        "display.compact": "Kompaktné",
        "display.condensed": "Zhustené",
        "display.kinet_light": "Kinet light",
        "display.azure_light": "Azure light",
        "display.sage_light": "Sage light",
        "confirm.please_confirm": "Potvrďte akciu",
        "confirm.are_you_sure": "Naozaj chcete pokračovať?",
        "guard.unsaved_changes_title": "Neuložené zmeny",
        "guard.unsaved_changes_message": "Máte neuložené zmeny. Opustiť stránku bez uloženia?",
        "guard.leave_page": "Opustiť stránku",
        "guard.stay": "Zostať",
        "config.workspace_title": "Pracovisko konfigurácie",
        "config.workspace_subtitle": "Upravte runtime, MQTT, úložiská telemetrie a servisné voľby.",
        "config.no_changes": "Bez zmien",
        "config.unsaved_changes": "Neuložené zmeny",
        "config.unsaved_changes_message": "Máte neuložené zmeny konfigurácie. Opustiť stránku bez uloženia?",
        "config.saving": "Ukladá sa...",
        "config.fix_validation_errors": "Opravte validačné chyby",
        "config.save_configuration": "Uložiť konfiguráciu",
        "config.tab.general": "Všeobecné",
        "config.tab.storage": "Úložisko",
        "config.tab.maintenance": "Údržba",
        "config.advanced_on": "Pokročilé: Zapnuté",
        "config.advanced_off": "Pokročilé: Vypnuté",
        "config.server_configuration": "Konfigurácia servera",
        "config.listen_host": "Host pre počúvanie",
        "config.listen_port": "Port pre počúvanie",
        "config.status_interval": "Interval stavu (sekundy)",
        "config.deduplication_delay": "Oneskorenie deduplikácie správ (sekundy)",
        "config.deduplication_delay_note": "Čas na zachytenie duplicít pred odoslaním najlepšej správy ďalej (0,5-10 sekúnd).",
        "config.timezone": "Časové pásmo",
        "config.timezone_note": "Používa sa pre časové značky v logoch a v rozhraní.",
        "config.language": "Jazyk aplikácie",
        "config.language_note": "Prepína texty rozhrania a formátovanie času. Po uložení sa stránka obnoví.",
        "config.mqtt_configuration": "Konfigurácia MQTT",
        "config.enable_mqtt_transport": "Povoliť MQTT transport",
        "config.enable_mqtt_transport_note": "Spustí MQTT bridge klienta, príjem príkazov z brokera a publikovanie runtime správ z platformy.",
        "config.mqtt_broker": "MQTT broker",
        "config.mqtt_port": "MQTT port",
        "config.mqtt_username": "MQTT používateľ",
        "config.mqtt_password": "MQTT heslo",
        "config.base_topic": "Základný topic",
        "config.auto_detach_configuration": "Konfigurácia auto-detach",
        "config.enable_auto_detach": "Povoliť auto-detach",
        "config.enable_auto_detach_note": "Automaticky odpojí senzory po dlhšej neaktivite.",
        "config.auto_detach_timeout": "Timeout auto-detach (hodiny)",
        "config.auto_detach_timeout_note": "Počet hodín neaktivity pred automatickým odpojením (predvolene 72).",
        "config.warning_timeout": "Timeout varovania (hodiny)",
        "config.warning_timeout_note": "Počet hodín neaktivity pred zobrazením varovania (predvolene 36).",
        "config.check_interval": "Interval kontroly (hodiny)",
        "config.check_interval_note": "Ako často sa majú kontrolovať neaktívne senzory (predvolene 1 hodina).",
        "config.optional_modules": "Voliteľné moduly",
        "config.platform": "Platforma",
        "config.integrations": "Integrácie",
        "config.server_configuration_note": "Runtime host, časovanie a lokalizačné nastavenia platformy a rozhrania.",
        "config.mqtt_configuration_note": "Správa MQTT bridge, pripojenia na broker a routovania topicov pre toto nasadenie.",
        "config.sensor_rules": "Pravidlá senzorov",
        "config.auto_detach_configuration_note": "Nastavenia varovania a automatického odpojenia pre senzory, ktoré prestanú odosielať dáta.",
        "config.modules": "Moduly",
        "config.optional_modules_note": "Skryte stránky a funkcie, ktoré v tomto nasadení nechcete mať viditeľné predvolene.",
        "config.storage": "Úložisko",
        "config.storage_and_telemetry_note": "Vyberte hlavný zdroj telemetrie, spravujte InfluxDB alebo TimescaleDB a nastavte monitorovacie prahy.",
        "config.storage.influx_kicker": "Zdroj runtime telemetrie",
        "config.storage.influx_title": "InfluxDB a zdroj telemetrie",
        "config.storage.influx_note": "Určite, odkiaľ UI číta telemetriu, a spravujte InfluxDB zápisy a snapshoty inventára.",
        "config.storage.timescale_kicker": "Operačné úložisko",
        "config.storage.timescale_note": "Konfigurácia trvalého telemetry store, retencie, kompresie a tenant-aware operačnej databázy.",
        "config.storage.monitoring_kicker": "Prahy monitoringu",
        "config.storage.monitoring_note": "Definujte prahy fronty, retry, latencie a reconnectov a spúšťajte prevádzkové kontroly priamo z tejto stránky.",
        "config.operations": "Operácie",
        "config.maintenance_note": "Uloženie konfigurácie a servisné reštarty majte oddelené od bežných nastavení.",
        "config.requires_attention": "Vyžaduje pozornosť",
        "config.live_scope": "Jadro runtime",
        "config.advanced": "Pokročilé",
        "config.optional": "Voliteľné",
        "config.show_bs_uptime_panel_short": "Uptime panel",
        "config.summary.title": "Súhrn konfigurácie",
        "config.summary.guidance": "Odporúčania pre úpravy",
        "config.summary.language_timezone": "Jazyk a časové pásmo",
        "config.summary.locale_note": "Hlavné prezentačné nastavenia rozhrania",
        "config.summary.transport": "Runtime transport",
        "config.summary.transport_note": "MQTT bridge a primárny zdroj telemetrie",
        "config.summary.modules": "Voliteľné moduly",
        "config.summary.modules_note": "Viditeľné doplnkové funkcie tohto nasadenia",
        "config.summary.change_state": "Stav zmien",
        "config.summary.change_note": "Validácia, uloženie a dopad na reštart",
        "config.summary.locale": "Lokalita",
        "config.summary.mqtt": "MQTT transport",
        "config.summary.telemetry": "Zdroj telemetrie",
        "config.summary.modules_enabled": "Aktívne moduly",
        "config.summary.restart_impact": "Dopad na reštart",
        "config.summary.modules_none": "Žiadne voliteľné moduly nie sú zapnuté",
        "config.summary.mqtt_enabled_unconfigured": "Povolené, broker ešte nie je vyplnený",
        "config.summary.restart_pending": "Konfigurácia sa zmenila. Pred servisnou akciou skontrolujte dopad na reštart.",
        "config.summary.restart_clear": "Momentálne nie je potrebná žiadna reštart akcia.",
        "config.summary.guidance_status": "Všeobecné",
        "config.summary.guidance_status_text": "Všeobecné použite pre identitu platformy, MQTT bridge a voliteľné moduly. Pokročilé sekcie zapnite len vtedy, keď potrebujete pravidlá alebo politiky.",
        "config.summary.guidance_storage": "Úložisko",
        "config.summary.guidance_storage_text": "InfluxDB a TimescaleDB berte ako prevádzkové integrácie. Ako primárnu čítaciu cestu pre UI nechajte len jeden zdroj telemetrie.",
        "config.summary.guidance_maintenance": "Údržba",
        "config.summary.guidance_maintenance_text": "Najprv uložte konfiguráciu, až potom reštartujte službu alebo kontajner, ak sa zmenili runtime alebo env hodnoty.",
        "config.show_oms_module": "Zobraziť OMS modul v navigácii",
        "config.show_oms_module_note": "Skryje OMS stránku a API endpointy, ak OMS merače vo vašom nasadení nepoužívate.",
        "config.show_mqtt_module": "Zobraziť MQTT konzolu v navigácii",
        "config.show_mqtt_module_note": "Skryje MQTT monitorovaciu stránku a jej API endpointy, ak MQTT konzolu vo vašom nasadení nepoužívate.",
        "config.show_bs_uptime_panel": "Zobraziť panel trendu uptime pre base stations",
        "config.show_bs_uptime_panel_note": "Ponechá graf uptime flotily viditeľný na stránke Base Stations. Ak chcete čistejšiu predvolenú stránku, nechajte vypnuté.",
        "config.tip": "Tip",
        "config.tip_keep_oms": "Nechávajte OMS zapnuté len vtedy, ak sú OMS/W-MBus merače súčasťou vášho nasadenia.",
        "config.tip_mqtt_console": "MQTT konzolu nechajte zapnutú len vtedy, ak potrebujete živý monitoring brokera alebo test publish priamo z rozhrania.",
        "config.storage_and_telemetry": "Úložisko a telemetria",
        "config.telemetry_source": "Zdroj telemetrie",
        "config.telemetry_source.auto": "Auto",
        "config.telemetry_source.runtime": "Runtime",
        "config.telemetry_source.influx": "InfluxDB",
        "config.influx_url": "Influx URL",
        "config.org": "Organizácia",
        "config.bucket": "Bucket",
        "config.tls_verify": "Overenie TLS",
        "config.token": "Token",
        "config.sync_inventory_influx": "Synchronizovať inventory do Influx",
        "config.inventory_events_measurement": "Measurement udalostí inventory",
        "config.crud_events": "CRUD udalosti",
        "config.snapshot_measurement": "Measurement snapshotu",
        "config.snapshot": "Snapshot",
        "config.enabled": "povolené",
        "config.disabled": "zakázané",
        "config.snapshot_interval_sec": "Interval snapshotu (sek)",
        "config.uptime_measurement": "Measurement uptime",
        "config.uptime_field": "Pole uptime",
        "config.uptime_eui_tag": "EUI tag uptime",
        "config.custom_uptime_flux_query": "Vlastný Flux query pre uptime (voliteľné)",
        "config.custom_uptime_flux_query_note": "Ak je prázdne, predvolený query pre uptime sa vygeneruje automaticky.",
        "config.timescale_section": "TimescaleDB / PostgreSQL (multi-tenant operačné úložisko)",
        "config.timescale": "Timescale",
        "config.host": "Host",
        "config.port": "Port",
        "config.database": "Databáza",
        "config.user": "Používateľ",
        "config.password": "Heslo",
        "config.ssl_mode": "SSL režim",
        "config.default_tenant": "Predvolený tenant",
        "config.event_writes": "Zápisy udalostí",
        "config.telemetry_writes": "Zápisy telemetrie",
        "config.snapshots": "Snapshoty",
        "config.retention": "Retencia",
        "config.telemetry_retention_days": "Retencia telemetrie (dni)",
        "config.inventory_retention_days": "Retencia inventory (dni)",
        "config.compression": "Kompresia",
        "config.compress_after_days": "Komprimovať po (dňoch)",
        "config.grafana_url": "Grafana URL",
        "config.grafana_dashboard_uid": "Grafana Dashboard UID",
        "config.monitoring_alert_thresholds": "Prahy monitorovacích alertov",
        "config.queue_warn_pct": "Varovanie fronty (%)",
        "config.queue_critical_pct": "Kritická fronta (%)",
        "config.retry_fail_warn_pct": "Varovanie zlyhania retry (%)",
        "config.retry_fail_critical_pct": "Kritické zlyhanie retry (%)",
        "config.db_latency_warn_ms": "Varovanie latencie DB (ms)",
        "config.db_latency_critical_ms": "Kritická latencia DB (ms)",
        "config.reconnect_warn_per_hour": "Varovanie reconnectov (/h)",
        "config.reconnect_critical_per_hour": "Kritické reconnecty (/h)",
        "config.operational_tools": "Prevádzkové nástroje",
        "config.check_timescale_status": "Skontrolovať stav Timescale",
        "config.sync_inventory_timescale": "Synchronizovať inventory do Timescale",
        "config.apply_timescale_policies": "Aplikovať Timescale politiky",
        "config.maintenance": "Údržba",
        "config.restart_service": "Reštartovať službu",
        "config.restart_container": "Reštartovať kontajner",
        "config.note": "Poznámka",
        "config.auto_update_disabled_note": "Automatické aktualizácie sú v tomto upravenom nasadení vypnuté. Aktualizácie aplikujte cez Git workflow a rebuild kontajnerov.",
        "config.restart_required_note": "Zmeny konfigurácie vyžadujú reštart. Použite „Reštartovať službu“ pre rýchly reštart procesu alebo „Reštartovať kontajner“ pre plné znovunačítanie prostredia.",
        "config.validation.listen_host_required": "Host pre počúvanie je povinný.",
        "config.validation.listen_port_range": "Port pre počúvanie musí byť v rozsahu 1 až 65535.",
        "config.validation.mqtt_broker_required": "MQTT broker je povinný.",
        "config.validation.mqtt_port_range": "MQTT port musí byť v rozsahu 1 až 65535.",
        "config.validation.base_topic_required": "Základný topic je povinný.",
        "config.validation.status_interval_min": "Interval stavu musí byť aspoň 1 sekunda.",
        "config.validation.dedup_range": "Oneskorenie deduplikácie musí byť v rozsahu 0,5 až 10 sekúnd.",
        "config.validation.auto_detach_timeout_min": "Timeout auto-detach musí byť aspoň 1 hodina.",
        "config.validation.warning_timeout_min": "Timeout varovania musí byť aspoň 1 hodina.",
        "config.validation.warning_timeout_order": "Timeout varovania musí byť menší alebo rovný timeoutu auto-detach.",
        "config.validation.check_interval_min": "Interval kontroly musí byť aspoň 1 hodina.",
        "config.validation.influx_url_required": "Influx URL je povinné, keď je zdroj telemetrie InfluxDB.",
        "config.validation.influx_org_required": "Influx organizácia je povinná, keď je zdroj telemetrie InfluxDB.",
        "config.validation.influx_bucket_required": "Influx bucket je povinný, keď je zdroj telemetrie InfluxDB.",
        "config.validation.influx_snapshot_interval_min": "Interval snapshotu pre Influx musí byť aspoň 15 sekúnd.",
        "config.validation.timescale_host_required": "Host pre Timescale je povinný, keď je Timescale zapnutý.",
        "config.validation.timescale_port_range": "Port pre Timescale musí byť v rozsahu 1 až 65535.",
        "config.validation.timescale_db_required": "Databáza Timescale je povinná, keď je Timescale zapnutý.",
        "config.validation.timescale_user_required": "Používateľ Timescale je povinný, keď je Timescale zapnutý.",
        "config.validation.default_tenant_required": "Predvolený tenant je povinný, keď je Timescale zapnutý.",
        "config.validation.timescale_snapshot_interval_min": "Interval snapshotu pre Timescale musí byť aspoň 15 sekúnd.",
        "config.validation.telemetry_retention_min": "Retencia telemetrie musí byť aspoň 1 deň.",
        "config.validation.inventory_retention_min": "Retencia inventory musí byť aspoň 1 deň.",
        "config.validation.compression_window_min": "Okno kompresie musí byť aspoň 1 deň.",
        "config.validation.threshold_warn_non_negative": "{label} varovanie musí byť nezáporné číslo.",
        "config.validation.threshold_crit_non_negative": "{label} kritická hodnota musí byť nezáporné číslo.",
        "config.validation.threshold_order": "{label} kritická hodnota musí byť väčšia alebo rovná varovaniu.",
        "config.validation.label.queue_threshold": "Prah fronty",
        "config.validation.label.retry_threshold": "Prah zlyhania retry",
        "config.validation.label.db_latency_threshold": "Prah latencie databázy",
        "config.validation.label.reconnect_threshold": "Prah reconnectov",
        "config.saved_restart_notice": "Konfigurácia uložená. Ak sa zmenili runtime hodnoty, reštartujte službu.",
        "config.save_failed": "Uloženie zlyhalo: {error}",
        "config.influx_sync_complete": "Synchronizácia Influx dokončená: {line_count} bodov ({sensor_points} senzorov, {base_station_points} základňových staníc).",
        "config.influx_sync_failed": "Synchronizácia Influx zlyhala: {error}",
        "config.timescale_status_online": "Timescale online ({host}/{database}) | fronta {queue_size}, zapísané {written}, zahodené {dropped}.",
        "config.timescale_status_failed": "Kontrola stavu Timescale zlyhala: {error}",
        "config.timescale_sync_complete": "Synchronizácia Timescale dokončená: {line_count} záznamov ({sensor_points} senzorov, {base_station_points} základňových staníc).",
        "config.timescale_sync_failed": "Synchronizácia Timescale zlyhala: {error}",
        "config.apply_policies_success": "Politiky retencie a kompresie pre Timescale boli aplikované.",
        "config.apply_policies_failed": "Aplikovanie politík zlyhalo: {error}",
        "config.restart_service_title": "Reštart služby",
        "config.restart_service_message": "Reštartovať BSSCI službu teraz? Spojenia budú krátko prerušené.",
        "config.restart_service_confirm": "Reštartovať službu",
        "config.restarting": "Reštartuje sa...",
        "config.restart_service_success": "Reštart služby bol úspešne spustený.",
        "config.restart_service_failed": "Reštart služby zlyhal: {error}",
        "config.restart_container_title": "Reštart kontajnera",
        "config.restart_container_message": "Reštartovať celý kontajner? Úplné znovunačítanie trvá približne 10-15 sekúnd.",
        "config.restart_container_confirm": "Reštartovať kontajner",
        "config.restart_container_notice": "Reštart kontajnera bol spustený. Stránka sa pripojí znova automaticky.",
        "login.brand_badge": "",
        "login.brand_title": "Kinet MIOTY Center",
        "login.brand_subtitle": "Prehľad senzorov, meraní a upozornení na jednom mieste.",
        "login.brand_footer": "",
        "login.heading": "Prihlásenie",
        "login.subtitle": "Pokračujte pomocou prihlasovacích údajov svojho účtu.",
        "login.username": "Používateľské meno",
        "login.password": "Heslo",
        "login.submit": "Prihlásiť sa",
        "login.eyebrow": "Monitorovacie centrum",
        "login.feature_bs": "Monitoring základňových staníc v reálnom čase",
        "login.feature_sensors": "Správa senzorov a telemetrie",
        "login.feature_network": "Vizualizácia topológie siete naživo",
        "login.username_placeholder": "Zadajte používateľské meno",
        "login.password_placeholder": "Zadajte heslo",
        "login.brand_console": "MIOTY monitorovacie centrum",
        "login.error.timeout": "Relácia vypršala z dôvodu neaktivity. Prihláste sa znova.",
        "login.error.invalid_credentials": "Neplatné používateľské meno alebo heslo",
        "login.language_switch": "Jazyk rozhrania",
        "login.mode_demo": "Demo režim",
        "login.mode_development": "Vývojový režim",
        "login.mode_production": "Produkčný režim",
        "login.bootstrap_accounts_title": "Predvolené účty pre rýchly štart",
        "login.bootstrap_accounts_copy": "Tieto bootstrap účty sú určené pre demo a lokálne testovanie.",
        "login.production_notice": "Produkčný režim má mať demo účty vypnuté a admin účet so zmeneným heslom.",
        "login.account_admin": "Platform admin",
        "login.account_customer": "Zákaznícky portál",
        "login.account_test": "Demo tenant",
        "auth.force_password_change_title": "Nastavte nové heslo admin účtu",
        "auth.force_password_change_subtitle": "Pred pokračovaním zmeňte predvolené bootstrap heslo pre admin účet.",
        "auth.new_password": "Nové heslo",
        "auth.confirm_password": "Potvrdenie hesla",
        "auth.password_change_submit": "Uložiť nové heslo",
        "auth.password_change_required": "Pred pokračovaním nastavte nové heslo pre bootstrap admin účet.",
        "auth.password_change_required_api": "Password change required before continuing.",
        "auth.password_change_too_short": "Nové heslo musí mať aspoň 8 znakov.",
        "auth.password_change_mismatch": "Zadané heslá sa nezhodujú.",
        "auth.password_change_same_as_default": "Nové heslo sa musí líšiť od predvoleného bootstrap hesla.",
        "auth.password_change_success": "Heslo admin účtu bolo aktualizované.",
        "auth.password_change_save_failed": "Nové heslo sa nepodarilo uložiť. Skúste to znova.",
        "runtime.loading_sensor_detail": "Načítava sa detail senzora...",
        "runtime.loading_base_station_detail": "Načítava sa detail základňovej stanice...",
        "runtime.unable_load_sensor_detail": "Nepodarilo sa načítať detail senzora: {error}",
        "runtime.unable_load_base_station_detail": "Nepodarilo sa načítať detail základňovej stanice: {error}",
        "runtime.unable_load_sensor_audit": "Nepodarilo sa načítať audit senzora",
        "runtime.unable_load_base_station_audit": "Nepodarilo sa načítať audit základňovej stanice",
        "runtime.failed_load_sensors": "Nepodarilo sa načítať senzory: {error}",
        "runtime.failed_load_base_stations": "Nepodarilo sa načítať základňové stanice: {error}",
        "runtime.failed_load_base_station_data": "Nepodarilo sa načítať dáta základňovej stanice.",
        "runtime.failed_load_logs": "Nepodarilo sa načítať logy.",
        "runtime.failed_load_audit_logs": "Nepodarilo sa načítať audit logy.",
        "runtime.failed_load_service_status": "Nepodarilo sa načítať stav služby",
        "runtime.no_sensors_configured": "Zatiaľ nie sú nakonfigurované žiadne senzory. Pridajte prvý senzor.",
        "runtime.view_prefs_reset": "Nastavenia zobrazenia boli obnovené.",
        "runtime.auto_refresh_enabled": "Automatické obnovovanie je zapnuté.",
        "runtime.auto_refresh_paused": "Automatické obnovovanie je pozastavené.",
        "runtime.select_sensor_first": "Najprv vyberte aspoň jeden senzor.",
        "runtime.no_sensor_selected_for_attach": "Nie je vybraný žiadny senzor na prepojenie.",
        "runtime.loading_base_stations": "Načítavajú sa základňové stanice...",
        "runtime.failed_load_base_stations_inline": "Nepodarilo sa načítať základňové stanice: {error}",
        "runtime.importing_sensors": "Importujú sa senzory...",
        "runtime.clipboard_unavailable": "Schránka nie je v tomto prehliadači dostupná.",
        "runtime.clipboard_copy_failed": "SN sa nepodarilo skopírovať do schránky.",
        "runtime.sn_copied": "SN skopírované: {eui}",
        "runtime.gps_both_required": "Zadajte obe GPS hodnoty alebo nechajte obe polia prázdne.",
        "runtime.gps_invalid_numbers": "GPS súradnice musia byť platné čísla.",
        "runtime.gps_out_of_range": "GPS sú mimo rozsahu. Latitude -90..90, longitude -180..180.",
        "runtime.attach_mapping_saved": "Prepojenie bolo uložené pre {success}/{total} senzorov.",
        "runtime.save_finished_with_issues": "Uloženie skončilo s problémami.",
        "runtime.attachment_mapping_updated": "Prepojenie bolo upravené pre senzor {eui}.",
        "runtime.sensor_detached": "Senzor {eui} bol odpojený.",
        "runtime.sensor_deleted": "Senzor {eui} bol vymazaný.",
        "runtime.sensor_reloaded": "Konfigurácia senzorov bola znovu načítaná.",
        "runtime.detach_all_requested": "Odpojenie všetkých senzorov bolo odoslané.",
        "runtime.all_sensors_cleared": "Všetky senzory boli vymazané.",
        "runtime.import_complete": "Import bol dokončený.",
        "runtime.bulk_action_completed": "{action} bolo dokončené pre {count} senzor(y).",
        "runtime.bulk_action_completed_with_issues": "{action} skončilo s problémami ({success}/{total} úspešne).",
        "runtime.confirm_action": "Potvrdiť akciu",
        "runtime.are_you_sure": "Naozaj chcete pokračovať?",
        "runtime.discard_sensor_changes_title": "Zahodiť zmeny senzora",
        "runtime.discard_sensor_changes_message": "Máte neuložené zmeny senzora. Zavrieť formulár bez uloženia?",
        "runtime.leave_page_sensor_changes": "Máte neuložené zmeny formulára senzora. Opustiť stránku bez uloženia?",
        "runtime.discard": "Zahodiť",
        "runtime.save_as_detached": "Uložiť ako odpojené",
        "runtime.detach_sensor_title": "Odpojiť senzor",
        "runtime.delete_sensor_title": "Vymazať senzor",
        "runtime.detach_all_sensors_title": "Odpojiť všetky senzory",
        "runtime.clear_all_sensors_title": "Vymazať všetky senzory",
        "runtime.clear_all_sensors_message": "Vymazať všetky senzory, odpojiť ich a odstrániť všetky konfigurácie? Túto akciu nemožno vrátiť späť.",
        "runtime.loading_service_status": "Načítava sa stav služby...",
        "runtime.loading_logs": "Načítavajú sa logy...",
        "runtime.loading_audit_trail": "Načítava sa audit stopa...",
        "runtime.logs_cleared": "Logy boli vymazané.",
        "runtime.unable_clear_logs": "Nepodarilo sa vymazať logy.",
        "runtime.admin_audit_cleared": "Admin audit log bol vymazaný.",
        "runtime.unable_clear_admin_audit": "Nepodarilo sa vymazať admin audit log.",
        "runtime.audit_export_failed": "Export auditu zlyhal: {error}",
        "runtime.telemetry_workspace_unavailable": "Prehľad telemetrie nie je dostupný: {error}",
        "runtime.unable_refresh_system_health": "Nepodarilo sa obnoviť stav systému: {error}",
        "runtime.telemetry_refresh_failed": "Obnovenie pracoviska telemetrie zlyhalo: {error}",
        "runtime.layout_saved": "Rozloženie bolo uložené.",
        "runtime.layout_engine_failed": "Engine rozloženia sa nepodarilo načítať. Obnovte stránku a overte prístup na internet.",
        "runtime.gps_sync_failed": "GPS synchronizácia zlyhala pre {eui}: {error}",
        "runtime.positions_locked": "Pozície sú uzamknuté. Pre presun alebo umiestnenie bodov vypnite uzamknutie.",
        "runtime.base_station_list_error": "Chyba zoznamu z?klad?ov?ch stan?c: {error}",
        "runtime.select_device_first": "Najprv vyberte zariadenie.",
        "runtime.selected_device_has_no_position": "Vybrané zariadenie nemá uloženú pozíciu.",
        "runtime.unsupported_position_source": "Nepodporovaný zdroj pozície.",
        "runtime.invalid_device_selected": "Vybrané zariadenie je neplatné.",
        "runtime.sensor_saved": "Senzor {eui} bol uložený.",
        "runtime.sensor_save_failed": "Uloženie senzora zlyhalo: {error}",
        "runtime.attachment_mapping_update_failed": "Aktualizácia prepojenia zlyhala: {error}",
        "runtime.sensor_detach_failed": "Odpojenie senzora zlyhalo: {error}",
        "runtime.sensor_delete_failed": "Vymazanie senzora zlyhalo: {error}",
        "runtime.sensor_reload_failed": "Znovunačítanie senzorov zlyhalo: {error}",
        "runtime.detach_sensors_failed": "Odpojenie senzorov zlyhalo: {error}",
        "runtime.clear_sensors_failed": "Vymazanie senzorov zlyhalo: {error}",
        "runtime.import_sensors_failed": "Import senzorov zlyhal: {error}",
        "runtime.sensor_not_found_reload": "Senzor {eui} sa v aktuálnej cache nenašiel. Zoznam sa obnovuje.",
        "runtime.no_base_stations_found_tenant": "V aktívnom tenante sa nenašli žiadne základňové stanice.",
        "runtime.invalid_mapping_payload": "Neplatný payload mapovania.",
        "runtime.saving": "Ukladá sa...",
        "runtime.base_station_gps_updated": "GPS pre základňovú stanicu bolo aktualizované.",
        "runtime.sensor_gps_updated": "GPS pre senzor bolo aktualizované.",
        "runtime.base_station_placed_floorplan": "Základňová stanica bola umiestnená na pôdorys.",
        "runtime.sensor_placed_floorplan": "Senzor bol umiestnený na pôdorys.",
        "runtime.place_base_station_first": "Najprv umiestnite aspoň jednu základňovú stanicu na pôdorys.",
        "runtime.place_devices_first_heatmap": "Pre generovanie heatmapy najprv umiestnite zariadenia na mapu.",
        "runtime.heatmap_generated": "Heatmapa bola vygenerovaná.",
        "runtime.positions_zoom_saved": "Pozície a úroveň priblíženia boli uložené.",
        "runtime.positions_cleared": "Všetky pozície boli vymazané.",
        "runtime.clear_positions_title": "Vymazať všetky pozície",
        "runtime.clear_positions_message": "Vymazať všetky uložené pozície zariadení?",
        "runtime.type_eui_or_name_first": "Najprv zadajte EUI alebo názov uzla.",
        "runtime.no_topology_match": "Pre \"{query}\" sa nenašiel žiadny uzol topológie.",
        "runtime.topology_graph_not_ready": "Graf topológie ešte nie je pripravený.",
        "runtime.topology_positions_saved": "Pozície a pohľad topológie boli úspešne uložené.",
        "runtime.failed_save_positions": "Uloženie pozícií zlyhalo: {error}",
        "runtime.coverage_positions_locked": "Pozície v mape pokrytia sú uzamknuté.",
        "runtime.coverage_positions_unlocked": "Pozície v mape pokrytia sú odomknuté.",
        "runtime.saved_positions_active": "Aktívne sú uložené pozície. Ak chcete použiť automatické rozloženie, zvoľte Reset layout.",
        "runtime.cannot_save_layout_browser": "V tomto prehliadači sa rozloženie nedá uložiť.",
        "runtime.layout_reverted_saved": "Rozloženie bolo vrátené do posledného uloženého stavu.",
        "runtime.no_saved_layout_found": "Nenašlo sa žiadne uložené rozloženie. Bola načítaná predvolená verzia.",
        "runtime.layout_reset_default": "Rozloženie bolo obnovené na predvolené.",
        "runtime.auto_refresh_failed": "Automatické obnovovanie zlyhalo: {error}",
        "runtime.runtime_refresh_failed": "Obnovenie runtime dát zlyhalo: {error}",
        "runtime.audit_export_failed_simple": "Export auditu zlyhal: {error}",
        "runtime.base_station_added": "Base station bola úspešne pridaná.",
        "runtime.base_station_updated": "Base station bola aktualizovaná.",
        "runtime.base_station_deleted": "Základňová stanica bola vymazaná.",
        "runtime.base_station_eui_copied": "EUI základňovej stanice bolo skopírované: {eui}",
        "runtime.no_base_stations_match_filters": "Žiadna základňová stanica nezodpovedá aktuálnym filtrom.",
        "runtime.no_uptime_events_available": "Zatiaľ nie sú dostupné žiadne uptime udalosti.",
        "runtime.uptime_data_unavailable": "Dáta uptime momentálne nie sú dostupné.",
        "runtime.no_uptime_telemetry_window": "V zvolenom okne nie sú žiadne uptime telemetry udalosti.",
        "runtime.no_status_events_yet": "Zatiaľ bez status udalostí.",
        "runtime.base_station_detail_loading_failed": "Načítanie detailu základňovej stanice zlyhalo.",
        "runtime.failed_load_base_station_cert_inventory": "Nepodarilo sa načítať certifikátový inventár základňovej stanice.",
        "runtime.failed_load_global_cert_status": "Nepodarilo sa načítať globálny stav certifikátov.",
        "runtime.periodic_status_query_disabled": "Periodické dotazovanie stavu je vypnuté.",
        "runtime.please_select_file_upload": "Najprv vyberte súbor na nahratie.",
        "runtime.please_select_backup_zip": "Najprv vyberte záložný ZIP súbor.",
        "runtime.error_activating_vm": "Aktivácia VM zlyhala.",
        "runtime.error_querying_vm_status": "Dotaz na stav VM zlyhal.",
        "runtime.clear_logs_title": "Vymazať logy služby",
        "runtime.clear_logs_message": "Vymazať aktuálne logy služby?",
        "runtime.clear_admin_audit_title": "Vymazať admin audit log",
        "runtime.clear_admin_audit_message": "Vymazať celý admin audit log?",
        "runtime.export_failed": "Export zlyhal",
        "health.failed_load_data": "Nepodarilo sa načítať health dáta",
        "mqtt.queue_util_note": "Využitie fronty voči kapacite zápisu.",
        "mqtt.retry_note": "Podiel zlyhaných retry pokusov v sledovanom okne.",
        "mqtt.latency_note": "Priemerná latencia zápisu do úložiska telemetrie.",
        "mqtt.reconnect_note": "Počet MQTT reconnectov za poslednú hodinu.",
        "base_station.validation.eui_length": "EUI musí mať presne 16 hex znakov.",
        "base_station.save_failed": "Uloženie zlyhalo",
        "base_station.delete_failed": "Vymazanie zlyhalo",
        "base_station.unsaved_changes_message": "Máte neuložené zmeny základňovej stanice. Zavrieť formulár bez uloženia?",
        "sensor_detail.decimal": "Desiatkové bajty",
        "sensor_detail.subtitle": "Prehľad aktuálneho stavu, meraní a histórie senzora.",
        "sensor_detail.subtitle_runtime": "Použite Prehľad pre aktuálny stav, Telemetriu pre namerané hodnoty a Históriu pre predchádzajúce dáta.",
        "sensor_detail.back_to_sensors": "Späť na senzory",
        "sensor_detail.edit_sensor": "Upraviť senzor",
        "sensor_detail.manage_attach": "Prepojenie so základňovými stanicami",
        "sensor_detail.telemetry_history": "História telemetrie",
        "sensor_detail.sensor": "Senzor",
        "sensor_detail.event_driven_sensor": "Udalosťami riadený senzor",
        "sensor_detail.periodic_sensor": "Periodický senzor",
        "sensor_detail.no_events": "Bez udalostí",
        "sensor_detail.no_events_note": "Pre tento senzor zatiaľ nebola zaznamenaná žiadna udalosť.",
        "sensor_detail.no_telemetry": "Bez telemetrie",
        "sensor_detail.unknown_telemetry_note": "Zatiaľ nebola zachytená žiadna nedávna telemetria.",
        "sensor_detail.recent_event": "Nedávna udalosť",
        "sensor_detail.quiet": "Pokojový stav",
        "sensor_detail.stale": "Bez udalosti dlhšie",
        "sensor_detail.online": "Online",
        "sensor_detail.delayed": "Oneskorené",
        "sensor_detail.last_event_arrived": "Posledná udalosť prišla {time}.",
        "sensor_detail.quiet_note": "Nedávno nebola zaznamenaná žiadna udalosť. Pri udalostných senzoroch je to v poriadku.",
        "sensor_detail.stale_note": "V rámci nastaveného servisného okna neprišla žiadna udalosť.",
        "sensor_detail.online_note": "Senzor posiela dáta v očakávanom intervale.",
        "sensor_detail.delayed_note": "Posledný uplink mešká ({time}).",
        "sensor_detail.offline_note": "V rámci offline prahu neprišla žiadna telemetria.",
        "sensor_detail.audit_all": "Všetko",
        "sensor_detail.audit_updates": "Úpravy",
        "sensor_detail.audit_attach": "Priradenie",
        "sensor_detail.audit_detach": "Odpojenie",
        "sensor_detail.audit_update": "Úprava",
        "sensor_detail.audit_other": "Iné",
        "sensor_detail.audit_action": "Akcia",
        "sensor_detail.audit_summary": "Zhrnutie",
        "sensor_detail.audit_actor": "Vykonal",
        "sensor_detail.rows_shown": "{count} riadkov",
        "sensor_detail.no_structured_summary": "Bez štruktúrovaného zhrnutia.",
        "sensor_detail.repeated_times": "Opakované {count}x",
        "sensor_detail.oldest_in_group": "Najstaršie v skupine: {time}",
        "sensor_detail.no_audit_match": "Žiadny audit záznam nevyhovuje zvolenému filtru.",
        "sensor_detail.no_audit_yet": "Pre tento senzor zatiaľ nie sú audit záznamy.",
        "sensor_detail.audit_load_on_open": "Audit stopa sa načíta po otvorení tejto karty.",
        "sensor_detail.generic_no_decoded": "Bez dekódovaných hodnôt",
        "sensor_detail.generic_telemetry_note": "Tento payload profil zatiaľ nemá vlastný UI blok. Dekódované hodnoty sú stále dostupné nižšie.",
        "sensor_detail.decoded_summary": "Zhrnutie dekódovania",
        "sensor_detail.no_short_history": "Krátka história zatiaľ nie je dostupná.",
        "sensor_detail.no_decoded_payload": "Zatiaľ neprišiel žiadny dekódovaný payload. Po prijatí telemetrie sa tu zobrazia dekódované hodnoty a krátka história.",
        "sensor_detail.no_raw_payload": "Raw payload zatiaľ nie je dostupný.",
        "sensor_detail.profile": "Profil",
        "sensor_detail.decoder_profile_note": "Profil dekódera použitý pre posledný uplink",
        "sensor_detail.model_hint": "Odhad modelu",
        "sensor_detail.model_hint_note": "Odhad typu senzora podľa prijatých dát",
        "sensor_detail.received_at": "Prijaté",
        "sensor_detail.received_at_note": "Čas po deduplikácii a uložení",
        "sensor_detail.raw_payload": "Raw payload",
        "sensor_detail.hex_and_decimal": "Hex a desiatkové bajty",
        "sensor_detail.decoded_json": "Dekódovaný JSON",
        "sensor_detail.debug_view_note": "Debug pohľad pre technickú analýzu",
        "sensor_detail.indoor": "Interiér",
        "sensor_detail.outdoor": "Exteriér",
        "sensor_detail.indoor_context_note": "Pre CO2 a komfortné metriky sa používajú vnútorné prahy.",
        "sensor_detail.outdoor_context_note": "Pre CO2 a environmentálne metriky sa používajú vonkajšie prahy.",
        "sensor_detail.auto_context_note": "Kontext prostredia sa odvodí zo zvoleného profilu senzora.",
        "sensor_detail.co2_no_data": "Bez dát",
        "sensor_detail.co2_no_data_note": "Dekódovaná CO2 vzorka zatiaľ nie je dostupná.",
        "sensor_detail.co2_comfortable": "Komfortné",
        "sensor_detail.co2_comfortable_note": "Vnútorná úroveň CO2 je v bežnom komfortnom pásme.",
        "sensor_detail.co2_elevated": "Zvýšené",
        "sensor_detail.co2_elevated_note": "Vnútorné CO2 je zvýšené. Odporúča sa skontrolovať vetranie.",
        "sensor_detail.co2_critical": "Kritické",
        "sensor_detail.co2_critical_note": "Vnútorné CO2 je vysoké a treba ho preveriť.",
        "sensor_detail.co2_ambient": "Ambientné",
        "sensor_detail.co2_ambient_note": "Vonkajšia úroveň CO2 je blízko bežného ambientného stavu.",
        "sensor_detail.co2_outdoor_elevated_note": "Vonkajšie CO2 je zvýšené oproti typickému ambientnému pozadiu.",
        "sensor_detail.co2_outdoor_critical_note": "Vonkajšie CO2 je nezvyčajne vysoké.",
        "sensor_detail.battery_healthy": "V poriadku",
        "sensor_detail.battery_monitor": "Sledovať",
        "sensor_detail.battery_low": "Nízka",
        "sensor_detail.signal_excellent": "Výborný",
        "sensor_detail.signal_good": "Dobrý",
        "sensor_detail.signal_fair": "Priemerný",
        "sensor_detail.signal_weak": "Slabý",
        "sensor_detail.internal_alarm": "Interný alarm",
        "sensor_detail.external_alarm": "Externý alarm",
        "sensor_detail.no_event_type": "Bez typu udalosti",
        "sensor_detail.first_seen": "Prvé videnie",
        "sensor_detail.last_seen": "Posledné videnie",
        "sensor_detail.gateway_coverage": "Pokrytie základňovými stanicami",
        "sensor_detail.counter_gap_loss": "Strata podľa packet countera",
        "sensor_detail.health": "Zdravie",
        "sensor_detail.radio_and_coverage": "Rádio a pokrytie",
        "sensor_detail.base_station": "Základňová stanica",
        "sensor_detail.messages": "Správy",
        "sensor_detail.latest_payload_snapshot": "Posledný prehľad payloadu",
        "sensor_detail.environment_context": "Kontext prostredia",
        "sensor_detail.expected_interval_source": "Zdroj očakávaného intervalu",
        "sensor_detail.telegrams_received": "Prijaté telegramy",
        "sensor_detail.signal_score": "Skóre signálu",
        "sensor_detail.last_event_type": "Typ poslednej udalosti",
        "sensor_detail.co2_current": "CO2 (aktuálne)",
        "sensor_detail.temperature": "Teplota",
        "sensor_detail.humidity": "Vlhkosť",
        "sensor_detail.battery_est": "Batéria (odhad)",
        "sensor_detail.last_alarm": "Posledný alarm",
        "sensor_detail.alarm_duration": "Trvanie alarmu",
        "sensor_detail.total_openings": "Celkový počet otvorení",
        "sensor_detail.seconds_ago": "pred {value}s",
        "sensor_detail.minutes_ago": "pred {value}m",
        "sensor_detail.hours_ago": "pred {value}h",
        "sensor_detail.days_ago": "pred {value}d",
        "base_station_detail.subtitle": "Prevádzkový detail pre runtime health, pripojené senzory, certifikáty a audit históriu.",
        "base_station_detail.back_to_base_stations": "Späť na základňové stanice",
        "base_station_detail.subtitle_runtime": "Použite Prehľad pre runtime stav, Pripojené senzory pre routing kontext, Certifikáty pre PKI stav a Audit pre admin zmeny.",
        "base_station_detail.no_recent_event": "Bez nedávnej udalosti",
        "base_station_detail.connected_note": "TLS relácia je aktívna a základňová stanica je online.",
        "base_station_detail.connecting_note": "TLS handshake práve prebieha.",
        "base_station_detail.offline_note": "Aktuálne nie je registrovaná žiadna aktívna TLS relácia.",
        "base_station_detail.valid_certificate": "Platný certifikát",
        "base_station_detail.expiring_soon": "Čoskoro expirovaný",
        "base_station_detail.expired": "Expirovaný",
        "base_station_detail.missing_certificate": "Chýbajúci certifikát",
        "base_station_detail.expires_at": "Expiruje {date}",
        "base_station_detail.expired_at": "Expiroval {date}",
        "base_station_detail.certificate_present": "Metadáta certifikátu sú prítomné.",
        "base_station_detail.certificate_review": "Certifikát treba čoskoro preveriť.",
        "base_station_detail.certificate_expired": "Certifikát expiroval.",
        "base_station_detail.missing_certificate_note": "Pre túto základňovú stanicu neboli nájdené metadáta certifikátu.",
        "base_station_detail.no_linked_sensors": "K tejto základňovej stanici momentálne nie sú priradené ani naviazané žiadne senzory.",
        "base_station_detail.live_route": "Aktívna trasa",
        "base_station_detail.configured_only": "Len nakonfigurované",
        "base_station_detail.observed_elsewhere": "Pozorované inde",
        "base_station_detail.unnamed_sensor": "Nepomenovaný senzor",
        "base_station_detail.open_sensor": "Otvoriť senzor",
        "base_station_detail.certificate_status": "Stav certifikátu",
        "base_station_detail.certificate_status_note": "Aktuálne metadáta certifikátu pre túto základňovú stanicu",
        "base_station_detail.generated": "Vygenerované",
        "base_station_detail.registry_timestamp": "Čas z registru",
        "base_station_detail.expires": "Expiruje",
        "base_station_detail.certificate_expiration_metadata": "Metadáta expirácie certifikátu",
        "base_station_detail.certificate_file": "Súbor certifikátu",
        "base_station_detail.filesystem_presence_check": "Kontrola prítomnosti na disku",
        "base_station_detail.open_certificate_workspace": "Otvoriť pracovisko certifikátov",
        "base_station_detail.no_audit_yet": "Pre túto base station zatiaľ nie sú audit záznamy.",
        "base_station_detail.audit_load_on_open": "Audit stopa sa načíta po otvorení tejto karty.",
        "base_station_detail.load_failed": "Načítanie detailu základňovej stanice zlyhalo",
        "base_station_detail.runtime_health": "Prevádzkový stav",
        "base_station_detail.configured_ip": "Nakonfigurovaná IP",
        "base_station_detail.status_age": "Vek stavu",
        "base_station_detail.cpu": "CPU",
        "base_station_detail.memory": "Pamäť",
        "base_station_detail.vm_capable": "Podpora VM",
        "base_station_detail.last_status_change": "Posledná zmena stavu",
        "base_station_detail.last_event": "Posledná udalosť",
        "base_station_detail.runtime_uptime": "Runtime uptime",
        "base_station_detail.name": "Názov",
        "base_station_detail.tenant": "Tenant",
        "base_station_detail.tags": "Tagy",
        "base_station_detail.gps": "GPS",
        "base_station_detail.configuration_note": "Statické nastavenia uložené v registri",
        "base_station_detail.runtime_health_note": "CPU, pamäť, teplota a VM schopnosti",
        "base_station_detail.declared_management_endpoint": "Deklarovaný manažment endpoint",
        "base_station_detail.coverage_map_source": "Zdroj umiestnenia v mape pokrytia",
        "base_station_detail.linked_sensor_note": "Senzory preferujúce túto base station",
        "base_station_detail.last_event_connected": "Posledná zaznamenaná pripojená udalosť",
        "base_station_detail.no_runtime_status_events": "Zatiaľ bez runtime status udalostí",
        "base_station_detail.latest_runtime_health": "Posledný runtime health snapshot",
        "base_station_detail.used_memory_estimate": "Odhad využitej pamäte",
        "base_station_detail.latest_reported_temperature": "Posledná nahlásená teplota zariadenia",
        "base_station_detail.supports_vm_routing": "Podporuje virtual meter routing",
        "base_station_detail.unnamed_base_station": "Nepomenovaná základňová stanica",
        "base_station_detail.yes": "Áno",
        "base_station_detail.no": "Nie",
        "language.english": "English",
        "language.slovak": "Slovenčina",
        "viewer.alerts": "Upozornenia",
        "viewer.active_alerts": "Aktívne upozornenia",
        "viewer.alert_rules": "Pravidlá upozornení",
        "viewer.add_alert": "Pridať upozornenie",
        "viewer.edit_alert": "Upraviť upozornenie",
        "viewer.delete_alert": "Vymazať upozornenie",
        "viewer.enable_alert": "Aktivovať",
        "viewer.disable_alert": "Deaktivovať",
        "viewer.sensor": "Senzor",
        "viewer.metric": "Metrika",
        "viewer.condition": "Podmienka",
        "viewer.threshold": "Prah",
        "viewer.severity": "Závažnosť",
        "viewer.enabled": "Aktivovaná",
        "viewer.alert_title": "Upozornenie",
        "viewer.triggered_alerts": "Spustené upozornenia",
        "viewer.acknowledged": "Potvrdené",
        "viewer.snooze_4h": "Odložiť 4h",
        "viewer.snooze_24h": "Odložiť 24h",
        "viewer.open_sensor": "Otvoriť senzor",
        "viewer.sensor_detail_alerts": "Upozornenia senzora",
        "viewer.no_alerts": "Žiadne upozornenia",
        "viewer.map_locked": "Zamknuté",
        "viewer.map_unlocked": "Odomknuté",
        "viewer.unlock_dragging": "Odomknúť presúvanie",
        "viewer.lock_position": "Zamknúť polohu",
        "viewer.move_marker": "Presuňte marker na novú polohu",
        "viewer.marker_moved": "✓ Premiestnený — uložiť?",
        "viewer.save_position": "Uložiť polohu",
        "viewer.cancel_move": "Zrušiť",
        "viewer.position_saved": "✓ Poloha uložená",
        "viewer.position_save_failed": "✗ Chyba uloženia",
        "viewer.move_cancelled": "Presunutie zrušené",
        "viewer.move_timeout": "Presunutie zrušené (čas vypršal)",
        "viewer.change_color": "Zmeniť farbu",
        "viewer.marker_color": "Farba markera",
        "viewer.reset_color": "Obnoviť predvolenú",
        "viewer.color_changed": "✓ Farba zmenená",
        "viewer.color_reset": "✓ Farba obnovená",
        "viewer.set_default_view": "Nastaviť ako východzí pohľad",
        "viewer.view_saved": "✓ Pohľad uložený",
        "viewer.unlock_dragging_hint": "Presúvanie odomknuté — presuňte marker",
        "viewer.dragging_locked": "Zamknutie",
        "viewer.sensor_details": "Detail senzora",
        "viewer.add_sensor": "Pridať senzor",
        "viewer.sensor_list_empty": "Žiadne senzory",
        "viewer.last_data": "Posledné dáta",
        "viewer.last_activity": "Posledná aktivita",
        "viewer.ago": "pred",
        "viewer.no_data": "Bez dát",
        "viewer.sensor_options": "Možnosti",
        "viewer.guide_title": "Sprievodca portálom",
        "viewer.guide_settings_note": "Rýchly prehľad, kde sledovať senzory, pracovať s upozorneniami a meniť základné nastavenia portálu.",
        "viewer.guide_open": "Otvoriť sprievodcu",
        "viewer.guide_kicker": "Rýchly sprievodca",
        "viewer.guide_intro_copy": "Tento portál slúži na každodenný prehľad senzorov, meraní, upozornení a základných nastavení.",
        "sensor_detail.delayed": "Oneskorené",
        "sensor_detail.quiet": "Pokojový stav",
    },
}

_PAGE_TITLE_KEYS = {
    "Dashboard": "page.dashboard",
    "Sensors": "page.sensors",
    "Base Stations": "page.base_stations",
    "System Health": "page.system_health",
    "Network": "page.network_topology",
    "Network Topology": "page.network_topology",
    "Coverage": "page.coverage_map",
    "Coverage Map": "page.coverage_map",
    "MQTT": "page.mqtt",
    "Sensor Telemetry": "page.sensor_telemetry",
    "Logs": "page.logs",
    "Administration": "page.administration",
    "Access & Tenants": "page.access_tenants",
    "Configuration": "page.configuration",
    "Certificates": "page.certificates",
    "Documentation": "page.documentation",
    "Login": "page.login",
    "Sensor Detail": "page.sensor_detail",
    "Base Station Detail": "page.base_station_detail",
}

_UI_TRANSLATIONS["sk"].update({
    "common.auto_refresh": "Automatické obnovovanie",
    "common.refresh_now": "Obnoviť teraz",
    "common.updated": "Aktualizované",
    "common.never": "nikdy",
    "common.never_capitalized": "Nikdy",
    "common.cache": "Cache",
    "common.focus": "Zamerať",
    "common.refresh": "Obnoviť",
    "common.save": "Uložiť",
    "common.reset": "Reset",
    "common.clear": "Vymazať",
    "common.filters": "Filtre",
    "common.search": "Hľadať",
    "common.limit": "Limit",
    "common.identity": "Identita",
    "common.telemetry": "Telemetria",
    "common.placement": "Umiestnenie",
    "common.name": "Názov",
    "common.name_label_optional": "Názov / štítok (voliteľné)",
    "common.tags_optional": "Tagy (voliteľné)",
    "common.tags_comma_separated": "Tagy oddelené čiarkou.",
    "common.auto_unknown": "Auto / neznáme",
    "common.indoor": "Interiér",
    "common.outdoor": "Exteriér",
    "common.gps_latitude_optional": "GPS zemepisná šírka (voliteľné)",
    "common.gps_longitude_optional": "GPS zemepisná dĺžka (voliteľné)",
    "common.latitude": "Latitude",
    "common.longitude": "Longitude",
    "common.live_summary": "Aktuálny súhrn",
    "common.sensor": "Senzor",
    "common.close": "Zavrieť",
    "common.network": "Sieť",
    "common.ip_address": "IP adresa",
    "common.user": "Používateľ",
    "common.username": "Používateľské meno",
    "common.password": "Heslo",
    "common.tenant": "Tenant",
    "common.description_optional": "Popis (voliteľné)",
    "common.note": "Poznámka:",
    "common.last_refresh": "Posledné obnovenie:",
    "common.none": "Žiadne",
    "common.not_available_short": "N/A",
    "common.seconds_ago": "{count}s dozadu",
    "common.minutes_ago": "{count}m dozadu",
    "common.hours_ago": "{count}h dozadu",
    "common.days_ago": "{count}d dozadu",
    "common.json": "JSON",
    "common.latest": "Posledná",
    "common.minimum": "Minimum",
    "common.maximum": "Maximum",
    "common.configuration": "6. Konfigurácia",
    "common.dashboard": "Dashboard",
    "common.network_topology": "Network Topology",
    "common.system_health": "System Health",
    "common.mqtt_logs": "MQTT a logy",
    "common.administration": "Administrácia",
    "common.configuration_short": "Configuration",
    "common.authenticated": "Autentifikovaný prístup",
    "common.selected_count": "0 vybraných",
    "health.refresh_interval": "Interval obnovovania",
    "health.tenant_scoped_view": "Tenant-scoped pohľad na telemetriu",
    "health.system_uptime": "Uptime systému",
    "health.service_center_runtime": "Runtime Service Center",
    "health.active_registered": "Aktívne / Registrované",
    "health.connected_total": "Pripojené / Celkom",
    "health.uplinks_24h": "Uplinky (24h)",
    "health.timescale_telemetry_volume": "Objem telemetrie v Timescale",
    "health.average_snr": "Priemerné SNR",
    "health.signal_quality_baseline": "Základná úroveň kvality signálu",
    "health.across_tracked_sensors": "Naprieč sledovanými senzormi",
    "health.telemetry_analytics_workspace": "Pracovisko analýzy telemetrie",
    "health.native_charts_subtitle": "Natívne grafy z telemetrického API platformy (tenant-aware)",
    "health.layout_edit_mode_active": "Režim úpravy rozloženia je aktívny. Panely môžete presúvať alebo meniť ich veľkosť z pravého dolného rohu. Zmeny sa po dokončení uložia automaticky.",
    "network.coverage_routing": "Pokrytie a smerovanie",
    "network.workspace_title": "Pracovisko topológie siete",
    "network.workspace_subtitle": "Umiestňujte zariadenia presne, overujte kvalitu spojení a spravujte topológiu z jedného operatívneho pohľadu.",
    "network.gps_synced": "GPS synchronizované",
    "network.live_topology": "Živá topológia",
    "network.tenant_aware": "Tenant-aware",
    "network.workspace_mode": "Režim pracoviska",
    "network.coverage_mode": "Režim pokrytia",
    "network.topology_mode": "Režim topológie",
    "network.coverage_mode_hint": "Režim pokrytia: mapa + umiestnenie v pôdoryse.",
    "network.coverage_controls": "Ovládanie pokrytia",
    "network.refresh_data": "Obnoviť dáta",
    "network.show_base_stations": "Zobraziť base stations",
    "network.show_sensors": "Zobraziť senzory",
    "network.heatmap": "Heatmapa",
    "network.signal_metric": "Signálová metrika",
    "network.gps_changes_sync": "Zmeny GPS sa synchronizujú automaticky.",
    "network.topology_controls": "Ovládanie topológie",
    "network.fit_graph": "Prispôsobiť graf",
    "network.layout_organic": "Organické (COSE)",
    "network.layout_layered": "Vrstvené (BS -> senzory)",
    "network.layout_grid": "Mriežka",
    "network.layout_concentric": "Sústredné",
    "logs.service_logs": "Servisné logy",
    "logs.admin_audit": "Admin audit",
    "logs.service_status": "Stav služby platformy",
    "logs.search_placeholder": "Hľadať správu, logger, zdroj...",
    "logs.level": "Úroveň",
    "logs.all_levels": "Všetky úrovne",
    "logs.logger": "Logger",
    "logs.all_loggers": "Všetky loggery",
    "logs.follow_tail": "Sledovať koniec",
    "logs.log_stream": "Tok logov",
    "sensors.add_new_sensor": "Pridať nový senzor",
    "sensors.configuration_sections": "Sekcie konfigurácie senzora",
    "sensors.identity_registration": "Identifikačné údaje",
    "sensors.identity_registration_note": "Základné údaje potrebné na rozpoznanie a komunikáciu senzora.",
    "sensors.eui_hex16": "EUI (16 hex znakov)",
    "sensors.network_key_hex32": "Sieťový kľúč (32 hex znakov)",
    "sensors.short_address_hex4": "Krátka adresa (4 hex znaky)",
    "sensors.name_placeholder": "napr. Vodomer - Hala A",
    "sensors.tags_placeholder": "napr. voda, hala-a, kritické",
    "sensors.bidirectional": "Obojsmerné",
    "sensors.telemetry_behavior": "Merania a hlásenie",
    "sensors.telemetry_behavior_note": "Profil senzora, spôsob hlásenia a časovanie ovplyvňujú zobrazenie hodnôt a stav senzora.",
    "sensors.sensor_profile": "Profil senzora",
    "sensors.sensor_profile_help": "Vyberte profil, ktorý nastaví dekóder, režim hlásenia a predvolené časovanie.",
    "sensors.payload_decoder": "Dekóder payloadu",
    "sensors.decoder_auto": "Automaticky (podľa názvu a tagov)",
    "sensors.raw_only": "Len raw dáta (bez dekódovania)",
    "sensors.payload_decoder_help": "Vyberte spôsob spracovania dát pre tento typ senzora.",
    "sensors.deployment_context": "Prostredie nasadenia",
    "sensors.deployment_context_help": "Používa sa pri kontextovom vyhodnocovaní CO2 a komfortných metrík.",
    "sensors.reporting_mode": "Režim hlásenia",
    "sensors.reporting_auto": "Automaticky (podľa typu senzora)",
    "sensors.reporting_periodic": "Periodické hlásenie",
    "sensors.reporting_event": "Udalostné hlásenie / len alarm",
    "sensors.reporting_mode_help": "Pri senzoroch dverí, okien alebo alarmov, ktoré neposielajú v pevných intervaloch, použite udalostný režim.",
    "sensors.expected_uplink_interval": "Očakávaný interval hlásenia (sekundy)",
    "sensors.expected_interval_placeholder": "Auto podľa typu zariadenia",
    "sensors.expected_interval_help": "Len pre periodické senzory. Nechajte prázdne, ak sa má odvodiť z typu zariadenia/profilu a prispôsobiť podľa prevádzky.",
    "sensors.event_sensor_service_window": "Servisné okno udalostného senzora (hodiny)",
    "sensors.service_window_placeholder": "Automaticky podľa typu udalostného senzora",
    "sensors.service_window_help": "Len pre udalostné senzory. Ak sa v tomto okne neobjaví žiadna udalosť, senzor sa označí ako neaktuálny.",
    "sensors.placement_mapping": "Umiestnenie na mape",
    "sensors.placement_mapping_note": "Voliteľné GPS údaje pre zobrazenie senzora na mape.",
    "sensors.gps_set_both_note": "Ak chcete senzor umiestniť na mape automaticky, vyplňte obe GPS polia.",
    "sensors.live_summary_note": "Panel vpravo priebežne zobrazuje vybraný profil, spôsob hlásenia a umiestnenie.",
    "sensors.manage_attachments": "Prepojenie senzora so základňovými stanicami",
    "sensors.loading_attachment_state": "Načítava sa stav prepojenia...",
    "sensors.all_base_stations": "Všetky základňové stanice",
    "sensors.clear_selection_detach": "Zrušiť výber a odpojiť",
    "sensors.save_mapping": "Uložiť prepojenie",
    "base_stations.uptime_tracking_starts": "Sledovanie uptime začne, keď sa base stations pripoja",
    "base_stations.configuration_sections": "Sekcie konfigurácie base station",
    "base_stations.identity_inventory": "Identita a inventár",
    "base_stations.identity_inventory_note": "Základná identita, názov a metadata používané na vyhľadávanie tejto base station.",
    "base_stations.eui_hex16": "EUI (16 hex znakov)",
    "base_stations.name_placeholder": "napr. Sklad Sever",
    "base_stations.tags_comma": "Tagy (oddelené čiarkou)",
    "base_stations.tags_placeholder": "napr. indoor, floor1, production",
    "base_stations.generate_certificates_automatically": "Generovať certifikáty automaticky",
    "base_stations.network_reachability": "Sieť a dostupnosť",
    "base_stations.network_reachability_note": "Voliteľné sieťové údaje používané na operatívnu viditeľnosť a diagnostiku.",
    "base_stations.ip_placeholder": "napr. 192.168.1.100",
    "base_stations.ip_help": "Voliteľné inventory pole pre LAN dostupnosť a rýchly operátorský kontext.",
    "base_stations.placement_mapping": "Umiestnenie a mapovanie",
    "base_stations.placement_mapping_note": "Voliteľné GPS súradnice pre umiestnenie base station do Coverage a Topology máp.",
    "base_stations.gps_set_both_note": "Pre automatické umiestnenie base station v mape Coverage nastavte obe polia.",
    "mqtt.topic_suffix_example": "ep/00124B001CBCE171/cmd",
    "mqtt.guide_step_1_title": "1) Príkaz senzoru - textový payload",
    "mqtt.guide_step_1_note": "Rýchly príkaz pre jeden senzorový endpoint.",
    "mqtt.guide_step_2_title": "2) Príkaz senzoru - JSON payload",
    "mqtt.guide_step_2_note": "Pripojte senzor k vybranej základňovej stanici alebo k viacerým základňovým staniciam.",
    "mqtt.guide_step_3_title": "3) Legacy register payload",
    "mqtt.guide_step_3_note": "Registrácia/konfigurácia endpointu cez legacy register topic.",
    "mqtt.guide_step_4_title": "4) Čo overiť po publishi",
    "mqtt.guide_step_4_note": "Skontrolujte, že sa správa objaví v outgoing topics a že sa ACK/odpoveď vráti na response topic.",
    "mqtt.runtime_counters": "Runtime počítadlá",
    "mqtt.runtime_counters_subtitle": "Priepustnosť príjmu, front a publishu",
    "mqtt.incoming_total": "Prijaté celkom",
    "mqtt.incoming_queued": "Zaradené do fronty",
    "mqtt.incoming_failed": "Príjem zlyhal",
    "mqtt.published": "Publikované",
    "mqtt.publish_failed": "Publish zlyhal",
    "mqtt.recent_errors": "Nedávne chyby",
    "mqtt.recent_incoming_topics": "Nedávne prichádzajúce topics",
    "mqtt.recent_outgoing_topics": "Nedávne odchádzajúce topics",
    "telemetry.tenant_scoped_history": "História obmedzená na tenant",
    "telemetry.no_decoded_values": "Bez dekódovaných hodnôt",
    "telemetry.raw_payload_hex": "Raw payload (hex)",
    "telemetry.raw_payload_dec": "Raw payload (dec)",
    "telemetry.no_numeric_decoded_values": "Žiadne číselné dekódované hodnoty",
    "telemetry.no_trend_metric_yet": "Pre trend zatiaľ nie je dostupná dekódovaná metrika",
    "telemetry.choose_one_sensor": "Vyberte jeden senzor",
    "telemetry.window_drift": "Posun v okne",
    "oms.note_wmbus_devices": "OMS merače sú WMBUS zariadenia (voda, plyn, elektrina). ID sa extrahujú z hlavičiek payloadov.",
    "oms.auto_refresh_every_30s": "Automatické obnovovanie každých 30 s",
    "oms.error_loading_vm_capable": "Nepodarilo sa načítať základňové stanice s podporou VM.",
    "oms.no_base_stations_connected": "Nie sú pripojené žiadne základňové stanice.",
    "oms.vm_not_confirmed_prefix": "Je pripojených <strong>{count}</strong> základňových staníc, ale podpora VM ešte nebola potvrdená.",
    "oms.run_vm_status_query": "Spustite VM status query pre detekciu podpory.",
    "oms.vm_capable_count": "VM-capable ({count})",
    "oms.not_confirmed_count": "Nepotvrdené ({count})",
    "oms.total_connected": "Pripojené celkom: {count}",
    "oms.error_loading_data": "Nepodarilo sa načítať dáta.",
    "oms.no_meters_available": "Nie sú dostupné žiadne merače",
    "oms.no_meters_match_search": "Aktuálnemu vyhľadávaniu nezodpovedá žiadny merač.",
    "cert.generated": "Vygenerované",
    "cert.expires": "Platí do",
    "cert.backup_restore": "Záloha a obnova",
    "cert.recovery_workflow": "Recovery workflow",
    "cert.backup_all_certificates": "Zálohovať všetky certifikáty",
    "cert.backup_all_certificates_note": "Vytvorí jeden ZIP archív s CA, servisným certifikátom a servisným kľúčom.",
    "cert.restore_from_zip": "Obnoviť zo ZIP",
    "cert.restore_from_zip_note": "Nahrajte predchádzajúci záložný balík a nahraďte aktuálne súbory.",
    "cert.restore_backup": "Obnoviť zálohu",
    "cert.no_certificate_records": "Nenašli sa žiadne certifikátové záznamy.",
    "admin.create_mode": "Režim vytvorenia",
    "admin.username_placeholder": "acme_user",
    "admin.display_name": "Zobrazované meno",
    "admin.display_name_placeholder": "ACME Operátor",
    "admin.password_required_create": "povinné pri vytvorení",
    "admin.access_scope": "Rozsah prístupu",
    "admin.tenant_id": "Tenant ID",
    "admin.user_restricted_to_tenant": "Používateľ je obmedzený na tento tenant.",
    "admin.admin_permissions": "Admin oprávnenia",
    "admin.granular_admin_rights": "Jemnozrnné admin práva pre tento účet.",
    "admin.save_user": "Uložiť používateľa",
    "admin.tenant_profile": "Profil tenantu",
    "admin.tenant_id_placeholder": "acme",
    "admin.tenant_name_placeholder": "ACME s.r.o.",
    "admin.tenant_description_placeholder": "Zákaznícky tenant pre ACME operácie",
        "admin.save_tenant": "Uložiť tenant",
        "admin.unsaved_changes_title": "Neuložené zmeny administrácie",
        "admin.unsaved_user_changes_message": "Máte neuložené zmeny používateľa. Zavrieť formulár bez uloženia?",
        "admin.unsaved_tenant_changes_message": "Máte neuložené zmeny tenanta. Zavrieť formulár bez uloženia?",
        "admin.unsaved_forms_leave_message": "Máte neuložené zmeny vo formulári používateľa alebo tenanta. Opustiť stránku bez uloženia?",
    "docs.core_workflows": "4. Kľúčové workflowy",
    "docs.api_summary": "5. Súhrn API",
    "docs.rbac": "7. Prístup, tenancy, RBAC",
    "docs.monitoring_reliability": "8. Monitoring a spoľahlivosť",
    "docs.troubleshooting": "9. Riešenie problémov",
    "docs.backup_restore_runbook": "10. Runbook zálohy a obnovy",
    "docs.objective_secure_transport": "Udržiavať bezpečný prenos medzi base stations a backend službami.",
    "docs.objective_sensor_lifecycle": "Spravovať celý životný cyklus senzora: vytvoriť, registrovať, attach, detach, monitorovať.",
    "docs.objective_observability": "Poskytnúť takmer real-time observabilitu cez dashboard a health stránky.",
    "docs.objective_tenant_separation": "Podporovať oddelenie tenantov s globálnym dohľadom na úrovni admina.",
    "docs.data_path_uplinks": "Uplinky prichádzajú cez TLSServer/BSSCI handlery.",
    "docs.data_path_queues": "Interné fronty oddeľujú ingest od perzistencie a publikovania.",
    "docs.data_path_storage": "Dáta môžu tiecť do MQTT a voliteľných storage backendov.",
    "docs.data_path_frontend": "Frontend číta API projekcie navrhnuté pre operatívnu zrozumiteľnosť.",
    "docs.quick_step_1": "Krok 1: Overte, že kontajnery a služby sú zdravé (`bssci-service-center`, DB, broker, voliteľne Grafana).",
    "docs.quick_step_2": "Krok 2: Otvorte Configuration a overte server/MQTT/storage endpointy.",
    "docs.quick_step_3": "Krok 3: Pridajte základňové stanice a potom nastavte GPS buď v edit formulári, alebo cez mapový workflow.",
    "docs.quick_step_4": "Krok 4: Pridajte senzory a spustite priradenie k vybraným základňovým staniciam.",
    "docs.quick_step_5": "Krok 5: Overte telemetriu v MQTT + Logs + System Health.",
    "docs.page_dashboard_desc": "Denný operatívny pohľad s mapou topológie, kvalitou signálu, prevádzkou, upozorneniami a kontextom základňových staníc.",
    "docs.page_sensors_desc": "Inventory a lifecycle operácie: create/edit, attach/detach, tenant labeling a diagnostika stavu.",
    "docs.page_base_stations_desc": "Inventár základňových staníc, runtime stav, identita, certifikáty a lokačné metaúdaje.",
    "docs.page_network_desc": "Coverage mapa + topology graf, kontrola liniek, filtre problémov a workflowy GPS synchronizácie.",
    "docs.page_health_desc": "Runtime počítadlá, queue/retry reliability metriky a analytika operatívnych incidentov.",
    "docs.page_mqtt_logs_desc": "Zdravie brokera, publish správanie, nástroje test publishu a viditeľnosť logov/auditu.",
    "docs.page_admin_desc": "Správa používateľov, tenant governance a administratívne akcie riadené rolami a scope.",
    "docs.page_configuration_desc": "Centralizované runtime nastavenia servera, storage, MQTT a údržby.",
    "docs.core_workflows_sub": "Produkčne bezpečný spôsob vykonania bežných akcií.",
    "docs.add_base_station": "Pridať base station",
    "docs.add_base_station_step_1": "Vytvorte záznam v Base Stations a overte identifikačné polia.",
    "docs.add_base_station_step_2": "Priraďte GPS, aby ste predišli nekonzistenciám na mape.",
    "docs.add_base_station_step_3": "Skontrolujte konektivitu a propagáciu stavu na Dashboarde.",
    "docs.add_sensor_attach": "Pridať senzor + attach",
    "docs.add_sensor_attach_step_1": "Vytvorte senzor s EUI a voliteľným popisným názvom/tagom.",
    "docs.add_sensor_attach_step_2": "Použite akciu priradenia na mapovanie senzora na jednu alebo viac základňových staníc.",
    "docs.add_sensor_attach_step_3": "Potvrďte, že riadok senzora zobrazuje pripojenú BS namiesto",
    "docs.gps_consistency": "Konzistencia GPS",
    "docs.gps_consistency_step_1": "Použite umiestnenie na mape a Save positions.",
    "docs.gps_consistency_step_2": "Znovu otvorte edit formuláre senzora/base station a overte rovnaké súradnice.",
    "docs.gps_consistency_step_3": "Vyčistite položky uvedené v pomocníkovi Missing GPS.",
    "docs.incident_triage": "Triáž incidentu",
    "docs.incident_triage_step_1": "Začnite v Dashboard alerts a health indikátoroch.",
    "docs.incident_triage_step_2": "Skontrolujte MQTT publish/reconnect správanie.",
    "docs.incident_triage_step_3": "Korelujte queue depth, retry failures a logy.",
    "docs.api_summary_sub": "Hlavné API skupiny používané UI a operations skriptami.",
    "docs.api_family": "Skupina",
    "docs.api_example_endpoints": "Príklady endpointov",
    "docs.api_purpose": "Účel",
    "docs.api_access_scope": "Rozsah prístupu",
    "docs.api_sensors_purpose": "Inventory + lifecycle priradenia",
    "docs.api_sensors_scope": "Oprávnenia na správu senzorov",
    "docs.api_bs_purpose": "Invent?r z?klad?ov?ch stan?c a meta?daje",
    "docs.api_bs_scope": "Oprávnenia na správu base stations",
    "docs.api_topology_coverage": "Topológia/Pokrytie",
    "docs.api_topology_purpose": "GPS stav mapy a dáta grafu",
    "docs.api_topology_scope": "Autentifikovaný, s obmedzeným zápisom",
    "docs.api_health_mqtt": "Health/MQTT",
    "docs.api_health_purpose": "Runtime spoľahlivosť a stav brokera",
    "docs.api_admin_tenants": "Admin/Tenanti",
    "docs.api_admin_purpose": "Governance a tenant operácie",
    "docs.api_admin_scope": "Admin scope",
    "docs.api_audit_purpose": "Čítanie/export admin aktivít",
    "docs.api_audit_scope": "Admin audit scope",
    "docs.configuration_sub": "Prevádzková politika pre bezpečné zmeny konfigurácie.",
    "docs.configuration_general_desc": "Základné správanie servera a runtime intervaly.",
    "docs.configuration_storage_desc": "Pripojenie Influx/Timescale, retencia, kompresia a sync akcie.",
    "docs.configuration_auto_detach_modules": "Auto-detach + voliteľné moduly",
    "docs.configuration_auto_detach_modules_desc": "Pokročilé moduly ponechajte skryté, ak ich vaše nasadenie nepoužíva.",
    "docs.configuration_maintenance_desc": "Akcie reštartu služby/kontajnera. Používajte len v kontrolovaných oknách.",
    "docs.configuration_warning_unsaved": "Guard neuložených zmien treba rešpektovať. Ak má zmena pretrvať, pred odchodom ju uložte.",
    "docs.rbac_sub": "Ako interpretovať oprávnenia a tenant izoláciu.",
    "docs.admin_visibility": "Viditeľnosť admina",
    "docs.admin_visibility_desc": "Admin môže byť globálny (cross-tenant) s limitmi podľa typu akcie a scope.",
    "docs.tenant_users": "Tenant používatelia",
    "docs.tenant_users_desc": "Ne-admin používatelia by mali pracovať striktne v dátových hraniciach prideleného tenantu.",
    "docs.auditability": "Auditovateľnosť",
    "docs.auditability_desc": "Kritické akcie sa majú objaviť v admin audit logu s aktérom, cieľom a stavom.",
    "docs.monitoring_reliability_sub": "Metriky, na ktoré sa oplatí alertovať v produkcii.",
    "docs.metric": "Metrika",
    "docs.why": "Prečo",
    "docs.healthy_behavior": "Zdravé správanie",
    "docs.example_alert": "Príklad alertu",
    "docs.metric_queue_depth": "Hĺbka fronty",
    "docs.metric_queue_depth_why": "Detekcia backpressure",
    "docs.metric_queue_depth_healthy": "Zväčša nízka, len s krátkymi špičkami",
    "docs.metric_queue_depth_alert": "Varovanie >70 %, kritické >90 %",
    "docs.metric_retry_fail_rate": "Miera zlyhania retry",
    "docs.metric_retry_fail_rate_why": "Detekcia pretrvávajúcich zlyhaní publish/write",
    "docs.metric_retry_fail_rate_healthy": "Takmer nulový rolling priemer",
    "docs.metric_retry_fail_rate_alert": "Alert >2 % na 5-10 min",
    "docs.metric_db_latency": "Latencia zápisu DB",
    "docs.metric_db_latency_why": "Zdravie storage/siete",
    "docs.metric_db_latency_healthy": "Stabilné p95",
    "docs.metric_db_latency_alert": "Alert pri prekročení SLO",
    "docs.metric_mqtt_reconnect": "Počet MQTT reconnectov",
    "docs.metric_mqtt_reconnect_why": "Indikátor stability brokera",
    "docs.metric_mqtt_reconnect_healthy": "Reconnecty sú zriedkavé",
    "docs.metric_mqtt_reconnect_alert": "Alert pri náhlych dávkach",
    "docs.troubleshooting_sub": "Časté problémy a prvé kontroly.",
    "docs.sensor_shows_bs_none": "Senzor zobrazuje BS: none",
    "docs.sensor_shows_bs_none_desc": "Znova skontrolujte odpoveď pri ukladaní attach mapovania, tenant filtre a obnovenú projekciu stavu senzora.",
    "docs.map_vs_device_gps_mismatch": "Nesúlad GPS medzi mapou a zariadením",
    "docs.map_vs_device_gps_mismatch_desc": "Spustite save/sync cyklus a overte, že v uložených map positions nie je duplicitný záznam EUI.",
    "docs.no_sensor_traffic_mqtt": "Žiadna senzorová prevádzka v MQTT",
    "docs.no_sensor_traffic_mqtt_desc": "Potvrďte registráciu + attach stav, potom skontrolujte počítadlá na MQTT stránke a runtime queue metriky.",
    "docs.empty_analytics_panels": "Prázdne analytické panely",
    "docs.empty_analytics_panels_desc": "Overte datasource, dashboard UID a správanie tenant variable query skôr než budete predpokladať chybu v UI.",
    "docs.backup_restore_runbook_sub": "Základy disaster recovery.",
    "docs.runbook_backup_scope": "Rozsah zálohy: DB dáta + konfiguračné súbory + users/tenants + certifikáty + runtime JSON.",
    "docs.runbook_backup_action": "Akcia zálohy: spustite backup skript a overte úplnosť archívu.",
    "docs.runbook_restore_action": "Akcia obnovy: najprv obnovte dátovú vrstvu, potom runtime súbory a napokon reštartujte služby.",
    "docs.runbook_validation": "Validácia: login, počty inventory, attach vzťahy, GPS stav, MQTT publish, health metriky.",
    "docs.no_section_matches_search": "Aktuálnemu vyhľadávaniu nezodpovedá žiadna sekcia dokumentácie.",
    "common.all": "Všetko",
    "common.more": "Viac",
    "common.import": "Import",
    "common.view_options": "Možnosti zobrazenia",
    "common.columns": "Stĺpce",
    "common.density": "Hustota",
    "common.detailed": "Detailné",
    "common.compact": "Kompaktné",
    "common.warning": "Upozornenie",
    "common.seconds": "sekundy",
        "dashboard.auto_refresh": "Automatické obnovovanie",
    "dashboard.waiting_first_update": "Čaká sa na prvú aktualizáciu...",
    "dashboard.cache_label": "Cache",
        "dashboard.operational_alerts": "Aktuálne upozornenia",
    "dashboard.layout_options": "Možnosti rozloženia",
    "dashboard.customize_layout": "Upraviť rozloženie",
    "dashboard.revert_saved": "Vrátiť uložené",
    "dashboard.reset_default": "Obnoviť predvolené",
    "dashboard.layout_edit_active": "Režim úpravy rozloženia je aktívny. Panely presúvajte za ikonu úchopu alebo meniť veľkosť z pravého dolného rohu. Zmeny sa po kliknutí na Dokončiť uložia automaticky.",
    "dashboard.finish": "Dokončiť",
    "dashboard.revert": "Vrátiť",
    "dashboard.tls_server": "TLS server",
    "dashboard.port_label": "Port",
        "dashboard.baseline_establishing": "Zhromažďujú sa úvodné dáta...",
    "dashboard.mqtt_broker": "MQTT broker",
    "dashboard.connected_gateways": "Pripojené základňové stanice",
    "dashboard.configured": "nakonfigurované",
        "dashboard.live_topology_canvas": "Živá mapa topológie",
    "dashboard.map": "Mapa",
    "dashboard.graph": "Graf",
    "dashboard.map_position_placeholder": "Lat --, Lng --, Z --",
    "dashboard.save_view": "Uložiť pohľad",
        "dashboard.click_full_network_editor": "Otvoriť editor topológie",
    "dashboard.no_positioned_devices_yet": "Zatiaľ nie sú umiestnené žiadne zariadenia",
    "dashboard.add_gps_map_note": "Doplňte GPS v základňových staniciach alebo senzoroch a značky sa zobrazia na mape.",
    "dashboard.alert_feed": "Prehľad upozornení",
    "dashboard.issues_requiring_attention": "Udalosti vyžadujúce pozornosť",
    "dashboard.no_critical_incidents": "Neboli zistené žiadne kritické incidenty",
    "dashboard.healthy": "v poriadku",
        "dashboard.gateway_actions": "Prehľad základňových staníc",
        "dashboard.quick_gateway_context": "Rýchly stav základňových staníc",
    "dashboard.no_gateways_yet": "Zatiaľ žiadne základňové stanice",
    "dashboard.waiting_for_data": "Čaká sa na dáta...",
    "telemetry.telemetry_store": "Úložisko telemetrie",
    "telemetry.subtitle": "TimescaleDB sa používa ako primárne úložisko telemetrie pre produkčnú históriu. Tabuľka zobrazuje najprv dekódované hodnoty; raw payload je dostupný iba v rozbaľovacom debug zobrazení.",
    "telemetry.waiting": "čaká sa",
    "telemetry.queue": "Fronta",
    "telemetry.records": "Záznamy",
    "telemetry.window": "Okno",
    "telemetry.current_page": "Strana",
    "telemetry.write_latency": "Latencia zápisu",
    "telemetry.all_sensors": "Všetky senzory",
    "telemetry.all_base_stations": "Všetky základňové stanice",
    "telemetry.payload_profile": "Profil payloadu",
    "telemetry.all_payloads": "Všetky payloady",
    "telemetry.time_window": "Časové okno",
    "telemetry.last_hour": "Posledná hodina",
    "telemetry.last_6_hours": "Posledných 6 hodín",
    "telemetry.last_24_hours": "Posledných 24 hodín",
    "telemetry.last_7_days": "Posledných 7 dní",
    "telemetry.last_30_days": "Posledných 30 dní",
    "telemetry.rows_per_page": "Riadkov na stránku",
    "telemetry.reset_filters": "Obnoviť filtre",
    "telemetry.export_csv": "Export CSV",
    "telemetry.refresh_history": "Obnoviť históriu",
    "telemetry.selected_sensor_trend": "Trend vybraného senzora",
    "telemetry.trend_note": "Mini trend používa aktuálne filtrované riadky senzora z TimescaleDB. Vyberte jeden senzor a skontrolujte posledný vývoj dekódovaných hodnôt bez opustenia stránky.",
    "telemetry.trend_metric": "Trendová metrika",
    "telemetry.auto_metric": "Automatická metrika",
    "telemetry.select_one_sensor_for_trend": "Vyberte jeden senzor, aby sa vykreslil kompaktný trend primárnej dekódovanej metriky podľa profilu payloadu.",
    "telemetry.decoded_records": "Dekódované telemetrické záznamy",
    "telemetry.table_note": "V tabuľke majú prednosť dekódované hodnoty. Riadok rozbaľte len vtedy, keď potrebujete skontrolovať raw payload.",
    "telemetry.no_rows_match_filters": "Aktuálnym filtrom nezodpovedajú žiadne telemetrické riadky.",
    "mqtt.title": "Monitoring MQTT brokeru",
    "mqtt.subtitle": "Živá runtime telemetria brokeru a diagnostika publikovania",
    "mqtt.refresh_interval": "Interval obnovovania",
    "mqtt.refresh_now": "Obnoviť teraz",
    "mqtt.updated": "Aktualizované",
    "mqtt.connection": "Pripojenie",
    "mqtt.broker": "Broker",
    "mqtt.topic_label": "Topic",
    "mqtt.input_queue": "Vstupná fronta",
    "mqtt.input_queue_note": "Prichádzajúce príkazy a konfigurácia",
    "mqtt.output_queue": "Výstupná fronta",
    "mqtt.output_queue_note": "Odchádzajúce publish/ack správy",
    "mqtt.monitoring_alerting": "Monitoring a alerting",
    "mqtt.monitoring_subtitle": "Hĺbka fronty, zlyhania retry, DB latencia a MQTT reconnect správanie",
    "mqtt.queue_depth": "Hĺbka fronty",
    "mqtt.retry_fail_rate": "Miera zlyhania retry",
    "mqtt.db_write_latency": "Latencia DB zápisu",
    "mqtt.reconnect_count": "Počet MQTT reconnectov",
    "mqtt.no_critical_runtime_signals": "Neboli zistené žiadne kritické runtime signály.",
    "mqtt.publish_test": "MQTT publish test",
    "mqtt.base_topic_label": "Základný topic",
    "mqtt.admin_tool": "Admin nástroj",
    "mqtt.topic_suffix": "Prípona topicu",
    "mqtt.payload": "Payload",
    "mqtt.payload_text": "Text",
    "mqtt.retain": "Retain",
    "mqtt.payload_example_json": "Príklad JSON: {\"command\":\"status\"}",
    "mqtt.queue_publish": "Zaradiť publish",
    "mqtt.test_guide": "MQTT testovací sprievodca",
    "mqtt.test_guide_subtitle": "Ako bezpečne testovať topicy a payloady",
    "mqtt.guide_intro_prefix": "Použite",
    "mqtt.guide_intro_suffix": "(bez základného topicu), vyberte režim payloadu a potom správu zaraďte do fronty. V tomto nasadení môžu testovacie správy publikovať iba používatelia s admin/config oprávnením.",
    "admin.users": "Používatelia",
    "admin.tenants": "Tenants",
    "admin.administrators": "Administrátori",
    "admin.default_tenant": "Super admin",
    "admin.super_admin_scope": "Super admin",
    "admin.reserved_super_admin_scope": "Rezervovaný globálny scope pre super admina",
    "admin.user_management": "Správa používateľov",
    "admin.tenant_management": "Správa tenantov",
    "admin.admin_only_operations": "Operácie iba pre administrátora. Zmeny sa aplikujú okamžite.",
    "admin.users_tenant_assignment": "Používatelia a priradenie tenantov",
    "admin.new_user": "Nový používateľ",
    "admin.load_users_tenants_note": "Načítajte používateľov a tenantov z backendu.",
    "admin.search_username_name_tenant": "Hľadať podľa používateľa, mena alebo tenantu",
    "admin.name": "Názov",
        "admin.role": "Rola",
        "admin.role_admin": "Admin",
        "admin.role_user": "Používateľ",
        "admin.role_customer": "Zákazník",
        "admin.scope": "Rozsah",
    "admin.actions": "Akcie",
    "admin.no_users_loaded": "Zatiaľ nie sú načítaní žiadni používatelia.",
    "admin.tenant_registry": "Register tenantov",
    "admin.new_tenant": "Nový tenant",
    "admin.manage_tenant_records": "Spravujte tenant záznamy a dátové operácie.",
    "admin.tenant": "Tenant",
    "admin.no_tenants_loaded": "Zatiaľ nie sú načítané žiadne tenanty.",
    "cert.backup_zip": "Zálohovací ZIP",
    "cert.generate_new": "Vygenerovať nové",
    "cert.never": "nikdy",
    "cert.ca_certificate": "CA certifikát",
    "cert.service_certificate": "Servisný certifikát",
    "cert.private_key": "Privátny kľúč",
    "cert.checking": "Kontroluje sa...",
    "cert.managed_bs_certs": "Spravované BS certifikáty",
    "cert.base_stations_inventory": "Základňové stanice v registri certifikátov",
    "cert.needs_attention": "Vyžaduje pozornosť",
    "cert.needs_attention_note": "Chýbajúce, expirované alebo čoskoro expirované",
    "cert.download_certificates": "Stiahnuť certifikáty",
    "cert.export_active_trust_assets": "Exportovať aktívne dôveryhodné súbory",
    "oms.total_meters": "Spolu OMS meračov",
    "oms.unique_devices_detected": "Zistené unikátne zariadenia",
    "oms.total_messages": "Spolu správ",
    "oms.vm_uplink_payloads_processed": "Spracované VM uplink payloady",
    "oms.average_per_meter": "Priemer / merač",
    "oms.messages_per_meter": "Správy na známy merač",
    "oms.active_last_hour": "Aktívne za poslednú hodinu",
    "oms.meters_seen_60m": "Merače videné za 60 minút",
    "oms.vm_controls": "VM ovládanie",
    "oms.control_virtual_channel_discovery": "Ovládanie objavovania virtuálnych kanálov",
    "oms.mac_type": "MAC typ",
    "oms.mac_type_note": "Predvolená hodnota je 0. Používajte len hodnoty podporované vašimi základňovými stanicami.",
    "oms.activate_vm": "Aktivovať VM",
    "oms.query_vm_status": "Zistiť stav VM",
    "oms.periodic_status_query": "Periodické zisťovanie stavu",
    "oms.auto_query_interval_range": "Rozsah intervalu automatického dotazu: 10 až 3600 sekúnd.",
    "oms.vm_event_log": "Log VM udalostí",
    "oms.recent_control_state_transitions": "Nedávne riadiace a stavové prechody",
    "oms.no_vm_events": "Zatiaľ neboli zaznamenané žiadne VM udalosti.",
    "oms.vm_capable_base_stations": "Základňové stanice s podporou VM",
    "oms.capability_discovery_status": "Stav zisťovania schopností",
    "oms.loading_vm_capable_base_stations": "Načítavajú sa základňové stanice s podporou VM...",
    "oms.detected_meters": "Detegované OMS merače (WMBUS)",
    "oms.live_telemetry_inventory": "Živý telemetrický inventár",
    "oms.search_placeholder": "Hľadať podľa sériového čísla, výrobcu, základňovej stanice...",
    "oms.no_meters_detected": "Zatiaľ neboli zistené žiadne OMS merače. Merače sa tu objavia po prijatí VM uplink dát.",
    "oms.meter": "Merač",
    "oms.manufacturer": "Výrobca",
    "oms.type": "Typ",
    "oms.version": "Verzia",
    "oms.last_payload": "Posledný payload",
    "docs.index_label": "Obsah dokumentácie",
    "docs.search_placeholder": "Hľadať v dokumentácii...",
    "docs.sidebar_note": "Tento manuál je usporiadaný podľa reálnych funkcií UI a produkčných operácií.",
    "docs.hero_title": "Funkčná dokumentácia Kinet MIOTY Center",
    "docs.hero_subtitle": "Komplexný sprievodca pre prevádzku senzorov, základňových staníc, topológie siete, toku telemetrie, administrácie a runtime údržby na jednom mieste.",
    "docs.introduction": "1. Úvod",
    "docs.introduction_sub": "Kinet MIOTY Center je operačná riadiaca rovina pre MIOTY infraštruktúru: TLS komunikáciu, správu inventára, vizualizáciu topológie a spoľahlivosť telemetrie.",
    "docs.main_objectives": "Hlavné ciele",
    "docs.data_path": "Dátová cesta",
    "docs.data_path_code": "Základňová stanica -> TLSServer -> Runtime fronty -> MQTT + Timescale/Influx -> UI API",
    "docs.quick_start": "2. Rýchly štart",
    "docs.quick_start_sub": "Minimálna sekvencia uvedenia do prevádzky pre čisté nasadenie.",
    "docs.warning_attach_mapping": "Ak sú senzory viditeľné, ale dáta neprichádzajú, najprv overte attach mapovanie, stav registrácie a cestu topicu.",
    "docs.page_reference": "3. Prehľad stránok",
    "docs.page_reference_sub": "Na čo slúži každá sekcia menu a kedy ju použiť.",
    "sensors.reload_config": "Znovu načítať konfiguráciu",
    "sensors.detach_all": "Odpojiť všetko",
    "sensors.clear_all": "Vymazať všetko",
    "sensors.list_density": "Hustota zoznamu senzorov",
    "sensors.show_summary_cards": "Zobraziť sumárne karty",
    "sensors.show_quick_filters": "Zobraziť rýchle filtre",
    "sensors.reset_view": "Obnoviť zobrazenie",
    "sensors.total_sensors": "Senzory spolu",
    "sensors.configured_in_registry": "Nakonfigurované v registri",
    "sensors.registered": "Registrované",
    "sensors.active_registration": "Má aktívnu registráciu",
    "sensors.active": "Aktívne",
    "sensors.recent_traffic_observed": "Pozorovaná nedávna prevádzka",
    "sensors.warnings": "Upozornenia",
    "sensors.warning_or_detached": "Upozornenie alebo odpojený stav",
    "sensors.search_placeholder": "Hľadať podľa EUI, názvu, tagu, krátkej adresy, GPS, základňovej stanice alebo stavu...",
    "sensors.live_configuration_overview": "Živý prehľad konfigurácie",
    "sensors.show_last_seen": "Zobraziť naposledy videné",
    "sensors.show_path_coverage": "Zobraziť trasu a pokrytie",
    "sensors.bulk_mode": "Hromadný výber",
    "sensors.bulk_mode_on": "Hromadný výber zapnutý",
    "sensors.waiting_first_refresh": "Čaká sa na prvé obnovenie...",
    "sensors.detached": "Odpojené",
    "sensors.no_data": "Bez dát",
    "sensors.unregistered": "Neregistrované",
    "sensors.status_unregistered": "Neregistrovaný",
    "sensors.status_recent_event": "Nedávna udalosť",
    "sensors.status_quiet": "Bez nových udalostí",
    "sensors.status_active": "Aktívny",
    "sensors.status_stale": "Neaktuálny",
    "sensors.status_auto_detached": "Automaticky odpojený",
    "sensors.status_no_events": "Bez udalostí",
    "sensors.status_no_data": "Bez dát",
    "sensors.status_registered": "Registrovaný",
    "sensors.status_not_registered": "Neregistrovaný",
    "sensors.event_seen": "Udalosť zachytená",
    "sensors.no_events_yet": "Zatiaľ bez udalostí",
    "sensors.traffic_seen": "Prevádzka zachytená",
    "sensors.no_recent_traffic": "Zatiaľ bez prevádzky",
    "sensors.no_path": "Bez trasy",
    "sensors.full_coverage": "Plné pokrytie",
    "sensors.no_coverage": "Bez pokrytia",
    "sensors.coverage_label": "Pokrytie",
    "sensors.active_paths_count": "Aktívne spojenia: {count}",
    "sensors.configured_paths_count": "Nastavené stanice: {count}",
    "sensors.missing_coverage_count": "Chýba: {count}",
    "sensors.open_detail": "Otvoriť detail",
    "sensors.showing_range": "Zobrazené {start}-{end} z {total} senzorov",
    "sensors.showing_zero": "Zobrazené 0 z {total} senzorov",
    "sensors.uplink_only": "Len uplink",
    "sensors.loading_attachment_state": "Načítava sa stav prepojenia...",
    "sensors.profile_hint_co2_name": "napr. CO2 - zasadacia miestnosť",
    "sensors.profile_hint_co2_tags": "napr. co2, vnútorný-vzduch, kancelária",
    "sensors.profile_hint_co2_tags_help": "Použite tagy ako miestnosť, poschodie alebo komfortná zóna pre rýchle filtrovanie CO2 senzorov.",
    "sensors.profile_hint_co2_decoder": "Profil LANSEN E2 CO2 dekóduje CO2, teplotu, vlhkosť a stav batérie.",
    "sensors.profile_hint_co2_environment": "Nastavte interiér alebo exteriér explicitne, aby sa použili správne CO2 prahy.",
    "sensors.profile_hint_co2_reporting": "CO2 senzory bývajú periodické a mali by posielať pravidelné hlásenia.",
    "sensors.profile_hint_co2_expected": "Typický interval hlásenia CO2 senzora je približne 300 až 360 sekúnd.",
    "sensors.profile_hint_co2_stale": "Používa sa len vtedy, ak tento senzor zámerne prepnete na udalostný režim.",
    "sensors.profile_hint_m2_name": "napr. kontakt dverí - sklad sever",
    "sensors.profile_hint_m2_tags": "napr. dvere, kontakt, sklad",
    "sensors.profile_hint_m2_tags_help": "Použite tagy pre zónu dverí alebo okna, časť budovy alebo alarmovú skupinu.",
    "sensors.profile_hint_m2_decoder": "Profil LANSEN M2 dekóduje otvorenia, alarmové okná, batériu a tamper príznaky.",
    "sensors.profile_hint_m2_environment": "Kontext ovplyvňuje hlavne komfortnú interpretáciu, nie logiku kontaktového alarmu.",
    "sensors.profile_hint_m2_reporting": "Senzory dverí a okien sú zvyčajne udalostné a mali by používať servisné okno.",
    "sensors.profile_hint_m2_expected": "Pri udalostných senzoroch to zvyčajne netreba. Pole nechajte prázdne, ak zariadenie neposiela pravidelný heartbeat.",
    "sensors.profile_hint_m2_stale": "Určuje, ako dlho môže byť senzor ticho, kým ho systém označí ako neaktuálny.",
    "sensors.profile_hint_custom_name": "napr. vodomer - hala A",
    "sensors.profile_hint_custom_tags": "napr. vlastné, utility, hala-a",
        "sensors.profile_hint_custom_tags_help": "Voľné tagy pomáhajú pri filtrovaní, skupinovaní a vyhľadávaní.",
    "sensors.profile_hint_custom_decoder": "Vlastný profil ponecháva dekóder, časovanie aj kontext úplne ručné.",
    "sensors.profile_hint_custom_environment": "Kontext používajte len vtedy, ak payload závisí od interiéru alebo exteriéru.",
    "sensors.profile_hint_custom_reporting": "Pre pravidelne hlásiace senzory zvoľte periodický režim, pre alarmové a kontaktné zariadenia udalostný.",
    "sensors.profile_hint_custom_expected": "Konkrétny heartbeat nastavte len vtedy, keď zariadenie naozaj posiela pravidelne.",
    "sensors.profile_hint_custom_stale": "Servisné okno používajte pri udalostných zariadeniach, ktoré môžu byť dlho bez aktivity.",
    "sensors.profile_hint_auto_name": "napr. vodomer - hala A",
    "sensors.profile_hint_auto_tags": "napr. voda, hala-a, kritické",
    "sensors.profile_hint_auto_tags_help": "Tagy oddelené čiarkou pomáhajú pri vyhľadávaní, skupinovaní aj filtrovaní topológie.",
    "sensors.profile_hint_auto_decoder": "Automatický dekóder sa pokúsi odhadnúť formát payloadu podľa profilu, názvu alebo tagov.",
    "sensors.profile_hint_auto_environment": "Neznámy kontext ponechá interpretáciu neutrálnu, kým sa nezvolí profil alebo prostredie.",
    "sensors.profile_hint_auto_reporting": "Automatický režim určí periodické alebo udalostné správanie podľa zvoleného profilu.",
    "sensors.profile_hint_auto_expected": "Pole nechajte prázdne a očakávaný interval sa odvodí z profilu alebo pozorovanej prevádzky.",
    "sensors.profile_hint_auto_stale": "Servisné okno používajú len udalostné senzory pri detekcii neaktuálneho stavu.",
    "sensors.not_placed_yet": "Zatiaľ bez umiestnenia",
    "sensors.map_placement_available_note": "Umiestnenie na mape bude dostupné v režime Pokrytie.",
    "sensors.map_placement_add_coordinates": "Pridajte zemepisnú šírku a dĺžku, aby sa senzor zobrazil na mape.",
    "sensors.service_window_short": "servisné okno",
    "sensors.auto_service_window": "Automatické servisné okno",
    "sensors.expected_interval_short": "očakávaný interval",
    "sensors.auto_expected_interval": "Automatický očakávaný interval",
    "sensors.service_window_note_short": "Určuje, kedy sa tichý event senzor začne považovať za neaktuálny.",
    "sensors.expected_interval_note_short": "Určuje prechod medzi stavmi online, oneskorený a offline.",
    "sensors.profile_summary_note": "Prednastavenie riadi dekóder, režim hlásenia aj časovanie.",
    "sensors.payload_decoder_summary_note": "Formát dekódovania payloadu použitý pri interpretácii telemetrie.",
    "sensors.context_note_indoor": "Použijú sa interiérové komfortné prahy.",
    "sensors.context_note_outdoor": "Použijú sa prahy prispôsobené exteriéru.",
    "sensors.context_note_auto": "Zatiaľ nie je vynútený žiadny konkrétny kontext.",
    "sensors.timing": "Časovanie",
    "sensors.selected_count_suffix": "vybraných senzorov",
    "sensors.attach_selected": "Pripojiť vybrané",
    "sensors.detach_selected": "Odpojiť vybrané",
    "sensors.delete_selected": "Vymazať vybrané",
    "sensors.clear_selection": "Zrušiť výber",
    "base_stations.fleet_overview": "Prehľad flotily",
    "base_stations.list_density": "Hustota zoznamu základňových staníc",
    "base_stations.add_base_station": "Pridať základňovú stanicu",
    "base_stations.total_configured": "Spolu nakonfigurované",
    "base_stations.gateway_inventory": "Inventár základňových staníc",
    "base_stations.search_placeholder": "Hľadať podľa EUI, názvu, IP, GPS, tagu...",
    "base_stations.live_gateway_inventory": "Živý inventár základňových staníc",
    "base_stations.base_station_eui": "Základňová stanica (EUI)",
    "base_stations.network": "Sieť",
    "base_stations.last_update": "Posledná aktualizácia",
    "base_stations.score": "Skóre",
    "base_stations.fleet_uptime_trend": "Trend uptime flotily (posledných 24 hodín)",
    "health.runtime_health": "Stav systému",
    "common.settings": "nastavenia",
    "common.note": "Poznámka",
    "common.auto": "Auto",
    "common.previous": "Predchádzajúce",
    "common.next": "Ďalšie",
        "dashboard.signal_quality_distribution": "Prehľad kvality signálu",
        "dashboard.open_health_analytics": "Otvoriť analýzu stavu",
    "dashboard.excellent": "Výborné",
    "dashboard.good": "Dobré",
    "dashboard.fair": "Priemerné",
    "dashboard.poor": "Slabé",
    "dashboard.critical": "Kritické",
    "dashboard.avg_snr": "Priem. SNR",
    "dashboard.avg_rssi": "Priem. RSSI",
    "dashboard.avg_loss": "Priem. strata",
    "dashboard.top_problem_sensors": "Najproblematickejšie senzory",
    "dashboard.no_sensor_incidents": "Zatiaľ neboli zistené žiadne incidenty senzorov.",
    "dashboard.traffic_capacity": "Prevádzka a kapacita",
    "dashboard.open_system_health": "Otvoriť stav systému",
    "dashboard.trend_unavailable": "Trend nie je dostupný",
    "dashboard.peak_label": "Špička",
    "dashboard.packets_per_hour": "Pakety/h",
    "dashboard.packet_loss": "Strata paketov",
    "dashboard.vm_capable_bs": "BS s podporou VM",
    "dashboard.messages_today": "Správy dnes",
    "telemetry.timestamp": "Časová pečiatka",
    "telemetry.decoded_values": "Dekódované hodnoty",
    "telemetry.radio": "Rádio",
    "base_stations.connected_now": "Pripojené teraz",
    "base_stations.peak_connected": "Maximum pripojených",
    "base_stations.fleet_availability": "Dostupnosť flotily",
    "base_stations.calculating_uptime": "Počíta sa uptime flotily...",
    "base_stations.source": "Zdroj",
    "base_stations.runtime_memory_events": "runtime udalosti v pamäti",
    "base_stations.show_breakdown": "Zobraziť rozpis základňových staníc",
    "base_stations.availability": "Dostupnosť",
    "base_stations.online_hours": "Online hodiny",
    "base_stations.last_change": "Posledná zmena",
    "cert.ca_note": "Koreňová certifikačná autorita pre podpísané servisné a BS certifikáty.",
    "cert.download_ca": "Stiahnuť CA",
    "cert.service_cert_note": "Serverový certifikát pre TLS spojenia so service centrom.",
    "cert.download_service_cert": "Stiahnuť servisný certifikát",
    "cert.service_private_key": "Servisný privátny kľúč",
    "cert.private_key_note": "Privátny kľúč spárovaný so servisným certifikátom.",
    "cert.download_key": "Stiahnuť kľúč",
    "cert.upload_certificates": "Nahrať certifikáty",
    "cert.replace_selected_without_leaving_ui": "Nahradiť vybrané certifikáty bez opustenia UI",
    "cert.restart_after_upload": "Po nahraní reštartujte službu/kontajner, aby sa TLS zmeny plne aplikovali.",
    "cert.accepts_pem_crt_cer": "Akceptuje .pem/.crt/.cer",
    "cert.upload_ca": "Nahrať CA",
    "cert.upload_service_cert": "Nahrať servisný certifikát",
    "cert.accepts_pem_key": "Akceptuje .pem/.key",
    "cert.upload_key": "Nahrať kľúč",
    "cert.per_base_station_certificates": "Certifikáty po základňových staniciach",
    "cert.operational_certificate_registry": "Prevádzkový register certifikátov",
    "cert.search_placeholder": "Hľadať podľa EUI, názvu, stavu...",
    "sensors.sensor_sn": "Senzor (SN)",
    "sensors.path_coverage": "Trasa a pokrytie",
    "sensor_detail.internal_alarm": "Interný alarm",
    "sensor_detail.external_alarm": "Externý alarm",
    "sensor_detail.no_event_type": "Bez typu udalosti",
    "sensor_detail.service_window": "Servisné okno",
    "sensor_detail.signal_score": "Skóre signálu",
    "sensor_detail.health": "Zdravie",
    "sensor_detail.profile": "Profil",
    "sensor_detail.environment_context": "Kontext prostredia",
    "sensor_detail.radio_and_coverage": "Rádio a pokrytie",
    "sensor_detail.operational_state_and_thresholds": "Aktuálny stav a prahy",
    "sensor_detail.avg_snr_operator_score": "Priemerné SNR prepočítané na jednoduché skóre signálu.",
    "sensor_detail.messages": "Správy",
    "sensor_detail.co2_current": "CO2 (aktuálne)",
    "sensor_detail.temperature": "Teplota",
    "sensor_detail.humidity": "Vlhkosť",
    "sensor_detail.battery_est": "Batéria (odhad)",
    "sensor_detail.total_openings": "Počet otvorení",
    "sensor_detail.last_alarm": "Posledný alarm",
    "sensor_detail.alarm_duration": "Trvanie alarmu",
    "sensor_detail.no_structured_summary": "Bez štruktúrovaného zhrnutia.",
})

_UI_TRANSLATIONS["sk"].update({
    "common.source": "Zdroj",
    "common.total": "Spolu",
    "common.filtered": "Filtrované",
    "common.shown": "Zobrazené",
    "common.unknown": "neznáme",
    "common.unknown_error": "Neznáma chyba",
    "common.running": "Beží",
    "common.base_station_short": "BS",
    "common.snr": "SNR",
    "common.rssi": "RSSI",
    "common.cpu": "CPU",
    "common.memory_short": "Pamäť",
    "common.temperature_short": "Teplota",
    "common.base_stations": "Základňové stanice",
    "common.sensors": "senzory",
    "common.connected": "Pripojené",
    "common.hide": "Skryť",
    "common.fit": "Prispôsobiť",
    "common.excellent": "Výborné",
    "common.good": "Dobré",
    "common.fair": "Priemerné",
    "common.poor": "Slabé",
    "common.critical": "Kritické",
    "common.eui": "EUI",
    "common.address": "Adr",
    "common.name": "Názov",
    "common.issue": "Problém",
    "common.configured": "Nakonfigurované",
    "common.yes": "Áno",
    "common.no": "Nie",
    "common.disconnected": "Odpojené",
    "common.received_at": "Prijaté",
    "common.registered": "Registrované",
    "common.not_registered": "Neregistrované",
    "common.messages": "Správy",
    "common.no_tags": "Bez tagov",
    "common.hex": "Hex",
    "common.topic": "Topic",
    "common.full_screen": "Celá obrazovka",
    "common.focus": "Zamerať",
    "common.offline": "Offline",
    "common.online": "Online",
    "common.connected": "Pripojené",
    "common.page_x_of_y": "Strana {current}/{total}",
    "common.time_minutes_ago_compact": "pred {count} min",
    "common.time_hours_ago_compact": "pred {count} h",
    "common.time_days_ago_compact": "pred {count} d",
    "common.inactive": "Neaktívny",
    "common.not_connected": "Bez spojenia",
    "common.attention": "pozor",
    "common.all_statuses": "Všetky stavy",
    "common.edit": "Upraviť",
    "common.delete": "Vymazať",
    "common.no_summary": "Bez zhrnutia",
    "common.edit_mode": "Režim úpravy",
    "common.details": "Detaily",
    "common.healthy": "v poriadku",
    "dashboard.active_sensors": "Aktívne senzory",
    "dashboard.live_links": "Aktívne linky",
    "dashboard.finish_customizing": "Dokončiť úpravy",
    "dashboard.no_sensor_incidents_short": "Neboli zistené incidenty senzorov.",
    "dashboard.no_gateways_detected": "Neboli zistené žiadne základňové stanice",
    "dashboard.check_base_station_config": "Skontrolujte konfiguráciu základňových staníc.",
    "dashboard.all_clear_title": "Všetko je v poriadku",
    "dashboard.all_clear_note": "Momentálne nie sú zistené žiadne upozornenia.",
    "dashboard.baseline_established": "Základná línia pripravená",
    "dashboard.no_change_last_refresh": "Bez zmeny pri poslednom obnovení",
    "dashboard.diff_vs_previous_refresh": "{sign}{diff}{suffix} oproti predchádzajúcemu obnoveniu",
    "dashboard.last_updated_time": "Aktualizované {time}",
    "dashboard.cache_age_seconds": "Cache {seconds}s",
    "dashboard.live_badge": "Naživo",
    "dashboard.zoom_in": "Priblížiť",
    "dashboard.zoom_out": "Oddialiť",
    "dashboard.view_saved": "Pohľad uložený",
    "dashboard.save_failed_short": "Uloženie zlyhalo",
    "dashboard.save_failed": "Uloženie zlyhalo: {error}",
    "dashboard.map_position_value": "Lat {lat}, Lng {lng}, Z {zoom}",
    "dashboard.base_station_tooltip": "Základňová stanica: {label}",
    "dashboard.sensor_tooltip": "Senzor: {label}",
    "dashboard.topology_needs_more_live_nodes": "Topológia potrebuje viac aktívnych uzlov",
    "dashboard.topology_waiting_more_nodes": "Po pripojení ďalších základňových staníc alebo senzorov sa graf vykreslí automaticky.",
    "dashboard.status_active": "Aktívny",
    "dashboard.status_connected": "Pripojený",
    "dashboard.status_disconnected": "Odpojený",
    "dashboard.incident_tls_online_recovered": "Obnovené: TLS server je online",
    "dashboard.incident_tls_offline_warning": "Upozornenie: TLS server prešiel do offline stavu",
    "dashboard.incident_mqtt_recovered": "Obnovené: MQTT broker je pripojený",
    "dashboard.incident_mqtt_disconnected_warning": "Upozornenie: MQTT broker sa odpojil",
    "dashboard.stable_online": "Stabilne online",
    "dashboard.still_offline": "Stále offline",
        "dashboard.stable_connection": "Stabilné spojenie",
        "dashboard.still_disconnected": "Stále odpojený",
        "dashboard.transport_disabled": "Transport vypnutý",
        "dashboard.transport_disabled_note": "Vypnuté v konfigurácii",
    "dashboard.connecting_count": "{count} v pripájaní",
    "dashboard.configured_count": "{count} nakonfigurované",
    "dashboard.sensor_count_suffix": " senzorov",
    "dashboard.sensor_count_meta": "{count} senzorov",
    "dashboard.trend_collecting_baseline": "Trend: zhromažďujú sa úvodné dáta",
    "dashboard.trend_since_last_refresh": "Trend: {diff} správ od posledného obnovenia",
    "dashboard.peak_messages": "Špička: {count} správ",
    "dashboard.problem_sensor_row": "strata {loss}% | SNR {snr} dB",
    "dashboard.no_ip": "bez IP",
    "dashboard.incident_tls_offline": "TLS server je offline.",
    "dashboard.incident_mqtt_disconnected": "MQTT broker je odpojený.",
    "dashboard.incident_no_connected_bs": "Nie sú pripojené žiadne základňové stanice.",
    "dashboard.incident_only_sensors_active": "Aktívnych je len {percent}% senzorov.",
    "dashboard.incident_packet_loss_elevated": "Strata paketov je zvýšená: {loss}%.",
    "dashboard.incident_no_vm_capable_bs": "Nebola zistená žiadna základňová stanica s podporou VM.",
    "dashboard.incident_dedup_ratio_high": "Pomer deduplikácie je vysoký ({ratio}%).",
        "dashboard.alert_note_critical": "kritické",
        "dashboard.alert_note_attention": "pozor",
        "dashboard.platform_overview": "Preh\u013ead platformy",
        "dashboard.summary_focus": "Fokus",
        "dashboard.summary_fleet": "Flotila",
        "dashboard.summary_posture": "Stav prostredia",
        "dashboard.summary_next_step": "\u010eo \u010falej",
        "dashboard.platform_fleet_summary": "{baseStations} BS \u00b7 {sensors}/{total} senzorov na\u017eivo",
        "dashboard.focus_connect_base_station": "Pripojte aspo\u0148 jednu z\u00e1klad\u0148ov\u00fa stanicu.",
        "dashboard.focus_waiting_for_sensors": "Z\u00e1klad\u0148ov\u00e9 stanice s\u00fa pripraven\u00e9, \u010dak\u00e1 sa na prev\u00e1dzku zo senzorov.",
        "dashboard.focus_check_mqtt_transport": "Jadro routovania funguje, ale MQTT transport vy\u017eaduje pozornos\u0165.",
        "dashboard.focus_runtime_operational": "Runtime transport aj flotila s\u00fa pripraven\u00e9 na prev\u00e1dzku.",
        "dashboard.next_step_connect_and_position": "Prive\u010fte jednu z\u00e1klad\u0148ov\u00fa stanicu online a ponechajte jej GPS pre mapu.",
        "dashboard.next_step_wait_sensor_traffic": "Skontrolujte priradenie senzorov a po\u010dkajte na prv\u00e9 telegramy.",
        "dashboard.next_step_open_health": "Otvorte Stav syst\u00e9mu pre hlb\u0161iu anal\u00fdzu alebo skontrolujte jednotliv\u00e9 zariadenia.",
        "dashboard.posture_attention_required": "Vy\u017eaduje pozornos\u0165",
        "dashboard.posture_transport_degraded": "MQTT potrebuje pozornos\u0165",
        "dashboard.posture_partial_coverage": "\u010ciasto\u010dn\u00e9 \u017eiv\u00e9 pokrytie",
        "dashboard.posture_stable": "Stabiln\u00fd prev\u00e1dzkov\u00fd stav",
        "dashboard.base_station_route_live": "\u017div\u00e9 trasy s\u00fa dostupn\u00e9",
        "dashboard.sensor_routes_observed": "Bola zaznamenan\u00e1 ned\u00e1vna telemetria",
        "dashboard.connect_base_station_first": "Najprv pripojte z\u00e1klad\u0148ov\u00fa stanicu",
        "dashboard.connect_base_station_map_note": "\u017div\u00e1 topol\u00f3gia sa napln\u00ed a\u017e po pripojen\u00ed aspo\u0148 jednej z\u00e1klad\u0148ovej stanice.",
        "dashboard.topology_waiting_base_station": "Po pripojen\u00ed prvej z\u00e1klad\u0148ovej stanice sa v grafe zobrazia \u017eiv\u00e9 trasy.",
        "dashboard.ops_snapshot_title": "Prev\u00e1dzkov\u00fd preh\u013ead telemetrie",
        "dashboard.ops_connect_station_badge": "Pripojte z\u00e1klad\u0148ov\u00fa stanicu",
        "dashboard.ops_delivery_risk_badge": "Riziko doru\u010denia",
        "dashboard.ops_waiting_traffic_badge": "\u010cak\u00e1 sa na telemetriu",
        "dashboard.ops_traffic_healthy_badge": "Telemetria je v poriadku",
        "dashboard.ops_connect_station_note": "Ke\u010f sa prv\u00e1 z\u00e1klad\u0148ov\u00e1 stanica pripoj\u00ed, tieto karty sa automaticky naplnia routovan\u00edm a telemetriou.",
        "dashboard.ops_rate_note": "Aktu\u00e1lna priemern\u00e1 vstupn\u00e1 r\u00fdchlos\u0165",
        "dashboard.ops_rate_waiting": "Zatia\u013e nebola zaznamenan\u00e1 prev\u00e1dzka",
        "dashboard.ops_delivery_good": "Doru\u010dovanie vyzer\u00e1 zdravo naprie\u010d akt\u00edvnymi senzormi",
        "dashboard.ops_delivery_watch": "Odhad vych\u00e1dza z medzier v packet counteri",
        "dashboard.ops_sensors_reporting": "Senzory s\u00fa pozorovan\u00e9 v \u017eivom runtime",
        "dashboard.ops_sensors_idle": "Zatia\u013e nie je \u017eiadna \u017eiv\u00e1 telemetria senzorov",
        "dashboard.ops_links_note": "Akt\u00edvne trasy medzi senzormi a z\u00e1klad\u0148ov\u00fdmi stanicami",
        "dashboard.ops_links_waiting": "\u017div\u00e9 linky sa zobrazia po spusten\u00ed routovania",
    "sensors.decoder_lansen_e2_co2": "LANSEN E2 CO2",
    "sensors.decoder_lansen_m2": "LANSEN M2",
    "sensors.no_sensors_match_filters": "Žiadne senzory nezodpovedajú aktuálnym filtrom.",
    "sensors.copy_sensor_sn": "Kopírovať SN senzora",
    "sensors.no_path_yet": "Zatiaľ bez trasy",
    "sensors.manage_attach": "Spravovať prepojenie",
    "sensors.manage_attach_natural": "Spravovať prepojenie",
    "sensors.delete_sensor_title": "Vymazať senzor",
    "sensors.decoded_payload_available": "Dekódovaný payload je dostupný",
    "sensors.no_base_station_coverage_data_yet": "Zatiaľ nie sú dostupné údaje o pokrytí základňovými stanicami.",
    "sensors.packets": "Pakety",
    "sensors.quick_overview": "Rýchly prehľad",
    "sensors.latest_payload_snapshot": "Posledný snapshot payloadu",
    "sensors.no_payload_snapshot_yet": "Zatiaľ nie je dostupný dekódovaný snapshot payloadu.",
    "sensors.no_sensor_audit_entries": "Pre tento senzor ešte nie sú zaznamenané audit záznamy.",
    "sensors.decoder_debug": "Debug dekódera",
    "sensors.raw_payload_debug_after_first_uplink": "Debug raw payloadu sa zobrazí po prijatí prvého uplinku.",
    "sensors.runtime_storage_payload_note": "Runtime alebo storage payload použitý pri poslednom vykreslení detailu senzora.",
    "sensors.decoder_profile": "Profil dekódera",
    "sensors.model_hint": "Model hint",
    "sensors.packet_counter": "Počítadlo paketov",
    "sensors.decoded_values_json": "Dekódované hodnoty (JSON)",
    "sensors.indoor_thresholds": "Indoor prahy",
    "sensors.outdoor_thresholds": "Outdoor prahy",
    "sensors.generic_thresholds": "Všeobecné prahy",
    "sensors.auto_context": "Auto kontext",
    "sensors.co2_indoor_limits": "CO2 sa vyhodnocuje podľa indoor komfortných limitov.",
    "sensors.co2_outdoor_limits": "CO2 sa vyhodnocuje podľa outdoor ambient limitov.",
    "sensors.set_context_for_stricter_interpretation": "Pre prísnejšiu interpretáciu nastavte v nastaveniach senzora Indoor alebo Outdoor.",
    "sensors.ambient": "Ambientné",
    "sensors.typical_outdoor_baseline": "Typická outdoor základná línia.",
    "sensors.within_expected_outdoor_variation": "Stále v očakávanej outdoor variabilite.",
    "sensors.higher_than_usual_outside": "Vyššie než je vonku obvyklé.",
    "sensors.outdoor_exhaust_or_enclosure": "Pravdepodobne ovplyvnené výfukom alebo uzavretým priestorom v okolí.",
    "sensors.unusually_high_outdoor": "Na outdoor umiestnenie nezvyčajne vysoké.",
    "sensors.fresh_indoor_air": "Čerstvá indoor kvalita vzduchu.",
    "sensor_profiles.auto.label": "Automaticky / odvodené",
    "sensor_profiles.auto.description": "Ponechá ručné nastavenia dekódera alebo ich odvodí z názvu senzora a tagov.",
        "sensor_profiles.lansen_e2_co2.label": "LANSEN E2 CO2",
        "sensor_profiles.lansen_e2_co2.description": "Periodická CO2 telemetria s približne 6-minútovým intervalom. Indoor a outdoor interpretácia sa riadi samostatným kontextom prostredia.",
        "sensor_profiles.lansen_e2_co2_auto.label": "LANSEN E2 CO2",
        "sensor_profiles.lansen_e2_co2_auto.description": "Periodická CO2 telemetria s približne 6-minútovým intervalom. Indoor a outdoor interpretácia sa riadi samostatným kontextom prostredia.",
        "sensor_profiles.lansen_e2_co2_indoor.label": "LANSEN E2 CO2",
        "sensor_profiles.lansen_e2_co2_indoor.description": "Periodická CO2 telemetria s približne 6-minútovým intervalom. Indoor a outdoor interpretácia sa riadi samostatným kontextom prostredia.",
        "sensor_profiles.lansen_e2_co2_outdoor.label": "LANSEN E2 CO2",
        "sensor_profiles.lansen_e2_co2_outdoor.description": "Periodická CO2 telemetria s približne 6-minútovým intervalom. Indoor a outdoor interpretácia sa riadi samostatným kontextom prostredia.",
    "sensor_profiles.lansen_m2_contact.label": "LANSEN M2 kontakt",
    "sensor_profiles.lansen_m2_contact.description": "Udalostný snímač dverí alebo okna s týždenným servisným oknom.",
    "sensor_profiles.custom.label": "Vlastný profil",
    "sensor_profiles.custom.description": "Ručná kombinácia dekódera, kontextu, režimu hlásenia a časovania.",
    "sensors.comfortable_indoor_range": "Komfortné indoor rozmedzie.",
    "sensors.check_ventilation": "Skontrolujte ventiláciu.",
    "sensors.insufficient_air_exchange": "Výmena vzduchu je pravdepodobne nedostatočná.",
    "sensors.ventilate_immediately": "Okamžite vetrajte.",
    "sensors.fresh": "Čerstvé",
    "sensors.healthy_baseline": "Zdravá základná línia.",
    "sensors.generally_acceptable": "Všeobecne akceptovateľné.",
    "sensors.consider_setting_context": "Zvážte nastavenie Indoor/Outdoor kontextu.",
    "sensors.context_missing_value_high": "Kontext chýba, hodnota rastie vysoko.",
    "sensors.context_missing_clearly_excessive": "Kontext chýba, no hodnota je zjavne nadmerná.",
    "sensors.battery_estimate_not_available": "Odhad batérie nie je dostupný.",
    "sensors.battery_level_stable": "Úroveň batérie vyzerá stabilne.",
    "sensors.monitor": "Sledovať",
    "sensors.battery_should_be_watched": "Batéria je ešte použiteľná, ale treba ju sledovať.",
    "sensors.battery_replacement_plan": "Naplánujte výmenu batérie.",
    "sensors.latest_uplink_payload": "Posledný uplink payload",
    "sensors.no_decoded_payload_received_yet": "Zatiaľ nebol prijatý žiadny dekódovaný payload.",
    "sensors.no_structured_decoder_for_profile": "Pre tento profil payloadu nie je dostupný štruktúrovaný dekóder.",
    "sensors.rising": "Rastie",
    "sensors.falling": "Klesá",
    "sensors.stable": "Stabilné",
    "sensors.for_use": "pre",
    "sensors.use_suffix": "použitie",
    "sensors.current_sample": "Aktuálna vzorka",
    "sensors.relative_humidity": "Relatívna vlhkosť",
    "base_stations.vm_capable": "S podporou VM",
    "base_stations.no_cert_metadata": "Bez cert metadata",
    "base_stations.open_detail_for": "Otvoriť detail základňovej stanice pre",
    "base_stations.copy_eui": "Kopírovať EUI",
    "base_stations.linked_sensors": "pripojené senzory",
    "base_stations.manage_certificates": "Spravovať certifikáty",
    "base_stations.no_uptime_events_available": "Nie sú dostupné žiadne uptime udalosti.",
    "base_stations.edit_base_station": "Upraviť base station",
    "mqtt.no_incoming_messages": "Zatiaľ neboli zaznamenané žiadne prichádzajúce MQTT správy.",
    "mqtt.no_outgoing_messages": "Zatiaľ neboli zaznamenané žiadne odchádzajúce MQTT publish správy.",
    "mqtt.qos": "QoS",
    "mqtt.retain": "Retain",
    "mqtt.no_errors_captured": "Neboli zachytené žiadne MQTT chyby.",
    "mqtt.no_critical_runtime_signals": "Neboli zistené žiadne kritické runtime signály.",
    "network.metric_snr": "SNR (dB)",
    "network.metric_rssi": "RSSI (dBm)",
    "network.all_nodes": "Všetky uzly",
    "network.base_stations_only": "Len z?klad?ov? stanice",
    "network.sensors_only": "Len senzory",
    "network.problems_context": "Problémy + kontext",
    "network.routes": "Trasy",
    "network.live_assigned": "Živé + priradené",
    "network.live_only": "Len živé",
    "network.assigned_only": "Len priradené",
    "network.highlight_issues": "Zvýrazniť problémy",
    "network.show_secondary_routes": "Zobraziť sekundárne trasy",
    "network.only_problematic": "Len problematické",
    "network.topology_control_note": "Živé uplink trasy používajú farby kvality. Len registrované linky sú modro prerušované, aby zostalo viditeľné priradenie senzor-base station.",
    "network.coverage_map_workspace": "Pracovisko mapy pokrytia",
    "network.openstreetmap": "OpenStreetMap",
    "network.floor_plan": "Pôdorys",
    "network.save_positions": "Uložiť pozície",
    "network.wheel_zoom_off": "Koliesko zoom vypnuté",
    "network.zoom_out": "Oddialiť",
    "network.zoom_in": "Priblížiť",
    "network.fit_to_view": "Prispôsobiť zobrazeniu",
    "network.reset_to_100": "Obnoviť na 100 %",
    "network.upload_floor_plan_start": "Nahrajte obrázok pôdorysu, aby ste mohli začať",
    "network.selected_device": "Vybrané zariadenie",
    "network.no_device_selected": "Nie je vybrané zariadenie",
    "network.select_device_drag_marker": "Vyberte zariadenie a potom posuňte marker na mape pre aktualizáciu GPS.",
    "network.primary_path": "Primárna trasa",
    "network.coverage_links": "Linky pokrytia",
    "network.best_signal": "Najlepší signál",
    "network.position_source": "Zdroj pozície",
    "network.gps_editor": "Editor GPS",
    "network.gps_sync_on_move": "GPS aktualizácie sa synchronizujú automaticky pri pohybe markeru.",
    "network.unlocked": "Odomknuté",
    "network.marker_dragging_enabled": "Ťahanie markerov a umiestňovanie na mape je povolené.",
    "network.lock_positions": "Zamknúť pozície",
    "network.device_explorer": "Prieskumník zariadení",
    "network.showing_devices_initial": "Zobrazuje sa 0 z 0 zariadení",
    "network.missing_gps": "Chýbajúce GPS",
    "network.topology_workspace": "Pracovisko topológie",
    "network.route_connection_inspection": "Kontrola trás a spojení",
    "network.topology_graph": "Graf topológie",
    "network.primary_route_live": "Primárna trasa (live)",
    "network.secondary_route_live": "Sekundárna trasa (live)",
    "network.assigned_relation_registration": "Priradený vzťah (registrácia)",
    "network.assigned_relation_configured": "Priradený vzťah (konfigurácia)",
    "network.good_link_quality": "Dobrá kvalita linky",
    "network.fair_link_quality": "Priemerná kvalita linky",
    "network.poor_link_quality": "Slabá kvalita linky",
    "network.critical_link_quality": "Kritická kvalita linky",
    "network.topology_stats": "Štatistiky topológie",
    "network.connections": "Spojenia",
    "network.problem_nodes": "Problémové uzly",
    "network.relationships": "Vzťahy",
    "network.online_bs": "Online BS",
    "network.linked_sensors": "Pripojené senzory",
    "network.orphan_sensors": "Osirelé senzory",
    "network.multi_bs_sensors": "Senzory na viacerých BS",
    "network.primary_routes": "Primárne trasy",
    "network.live_routes": "Živé trasy",
    "network.assigned_routes": "Priradené trasy",
    "network.no_base_stations_match_filter": "Žiadne base stations nezodpovedajú aktuálnemu filtru.",
    "network.no_sensors_match_filter": "Žiadne senzory nezodpovedajú aktuálnemu filtru.",
    "network.no_missing_gps_entries": "Nie sú žiadne položky bez GPS.",
    "network.gps_not_configured": "GPS nie je nakonfigurované",
    "network.no_telegram_data_yet": "Zatiaľ nie sú dostupné žiadne telegramové dáta",
    "network.receiving_sensors": "Prijímané senzory",
    "network.showing_devices_count": "Zobrazuje sa {shown} z {total} zariadení",
    "network.devices_hidden_until_gps": "{count} zariadení je na OpenStreetMap skrytých, kým sa nepriradí GPS.",
    "network.positioned": "Umiestnené",
    "network.not_positioned": "Neumiestnené",
    "network.route": "trasa",
    "network.routes_count": "trasy",
    "network.connection_lost": "Spojenie stratené",
    "network.duty_cycle": "Duty cycle",
    "network.primary_bs": "Primárna BS",
    "network.receivers": "Prijímače",
    "network.live_receivers": "Live prijímače",
    "network.assigned_bs": "Priradené BS",
    "network.no_base_station_relation": "Bez vzťahu k základňovej stanici",
    "network.no_live_uplink_assignment_only": "Zatiaľ bez live uplinku (len priradenie)",
    "network.missing_primary_route": "Chýba primárna trasa",
    "logs.service_not_running": "Služba nebeží.",
    "logs.connected_bs": "pripojené BS",
    "logs.connecting_bs": "BS v pripájaní",
    "logs.pending": "čaká",
    "logs.no_logs_current_filter": "Pre aktuálny filter nie sú žiadne logy.",
    "logs.memory_source": "pamäť",
    "logs.no_audit_entries_current_filter": "Pre aktuálny filter nie sú žiadne audit záznamy.",
    "logs.refresh_audit": "Obnoviť audit",
    "logs.loading_audit_trail": "Načítava sa audit trail...",
    "health.station": "Stanica",
    "health.duty": "Duty",
    "health.uptime": "Uptime",
    "health.no_base_stations_runtime_snapshot": "V runtime snímke nie sú žiadne základňové stanice.",
    "sensors.current_mapping_summary": "Aktuálne prepojenie: <strong>{count}</strong> základňových staníc. Zrušte všetky výbery a uložte, ak chcete senzor odpojiť.",
    "sensors.currently_detached_prompt": "Senzor je momentálne odpojený. Vyberte jednu alebo viac základňových staníc a uložte prepojenie.",
    "sensors.bulk_mapping_summary": "Hromadná úprava pre <strong>{count}</strong> senzorov. Označené položky predstavujú spoločné prepojenie.",
    "sensors.base_station_offline_pending_assignment": "Offline (čaká na priradenie)",
    "sensors.not_assigned": "Bez prepojenia",
    "sensors.assigned": "Prepojené",
    "sensors.assigned_all_selected": "Prepojené (všetky vybrané)",
    "sensors.assigned_partial": "Prepojené ({count}/{total})",
    "sensors.assignment_selection_summary": "{selected} vybraných · {mapped} už prepojených",
    "sensors.save_detached_selected": "Uložiť ako odpojené pre vybrané senzory",
    "sensors.save_assignment_selected": "Uložiť prepojenie pre vybrané senzory",
    "sensors.confirm_save_detached_selected": "Uložiť <strong>{count}</strong> vybraných senzorov ako odpojené? Tým sa odstráni ich prepojenie so základňovými stanicami.",
    "sensors.confirm_save_detached_single": "Uložiť senzor <code>{eui}</code> ako odpojený bez prepojenia so základňovou stanicou?",
    "sensors.primary_base_station": "Primárna základňová stanica",
    "sensors.base_station_label": "Základňová stanica",
    "sensors.base_station_coverage": "Pokrytie základňovými stanicami",
    "sensors.receiving_base_stations": "Prijímajúce základňové stanice",
    "sensors.recorded_base_station_paths": "Počet zaznamenaných trás k základňovým staniciam: {count}",
    "sensors.no_base_station_path_yet": "Trasa k základňovej stanici zatiaľ nebola zaznamenaná",
    "health.sensor_reliability": "Spoľahlivosť senzorov",
    "health.worst_packet_loss_first": "Najhoršie linky podľa straty paketov ako prvé",
    "health.received": "Prijaté",
    "health.lost": "Stratené",
    "health.loss_percent": "Strata %",
    "health.avg_snr": "Priem. SNR",
    "health.avg_rssi": "Priem. RSSI",
    "health.no_sensor_packet_stats": "Nie sú dostupné štatistiky paketov senzorov.",
    "admin.no_scope_definitions": "Nie sú dostupné žiadne definície admin scope.",
    "admin.cannot_grant_scope": "Tento scope nemôžete prideliť alebo odobrať.",
    "admin.all_tenants": "Všetky tenanty",
    "admin.global_admins": "Globálni admini",
    "admin.all_roles": "Všetky roly",
    "admin.no_users_match_filters": "Žiadni používatelia nezodpovedajú aktuálnym filtrom.",
    "admin.no_tenants_loaded": "Nie sú načítané žiadne tenanty.",
    "admin.edit_user": "Upraviť používateľa",
    "admin.create_user": "Vytvoriť používateľa",
    "admin.update_user": "Aktualizovať používateľa",
    "admin.edit_tenant": "Upraviť tenant",
    "admin.create_tenant": "Vytvoriť tenant",
    "admin.update_tenant": "Aktualizovať tenant",
    "admin.all_tenants_global_admin": "Všetky tenanty (globálny admin)",
    "admin.admin_global_can_see_all": "Admin je globálny a vidí všetky tenanty.",
})

_UI_TRANSLATIONS["sk"].update({
    "login.error.account_disabled": "Tento účet je deaktivovaný.",
    "login.bootstrap_accounts_title": "Predvolené účty pre prvé prihlásenie",
    "login.bootstrap_accounts_copy": "Tieto bootstrap účty sú pripravené na prvé spustenie aplikácie. Admin bude po prvom prihlásení vyzvaný na zmenu hesla.",
    "login.account_test": "Test tenant",
    "admin.status": "Stav",
    "admin.account_active": "Aktívny účet",
    "admin.account_active_hint": "Deaktivovaný účet sa nebude môcť prihlásiť.",
    "admin.user_active": "Aktívny",
    "admin.user_inactive": "Deaktivovaný",
    "admin.activate_user": "Aktivovať",
    "admin.deactivate_user": "Deaktivovať",
    "admin.cannot_deactivate_self": "Vlastný účet nie je možné deaktivovať.",
    "admin.cannot_delete_self": "Vlastný účet nie je možné odstrániť.",
    "admin.account_status_updated": "Stav účtu '{username}' bol aktualizovaný.",
    "common.active": "Aktívny",
})


def _normalize_app_language(value):
    raw = str(value or "").strip().lower()
    return raw if raw in _APP_LANGUAGE_OPTIONS else "sk"


def _get_app_language():
    if has_request_context():
        override = session.get("ui_language_override")
        if override:
            return _normalize_app_language(override)
    return _normalize_app_language(getattr(bssci_config, "APP_LANGUAGE", "sk"))


def _get_app_locale():
    return str(_APP_LANGUAGE_OPTIONS.get(_get_app_language(), _APP_LANGUAGE_OPTIONS["sk"]).get("locale") or "sk-SK")


def _ui_text(key, default=None, language=None, **kwargs):
    lang = _normalize_app_language(language or _get_app_language())
    translations = _UI_TRANSLATIONS.get(lang, {})
    fallback_translations = _UI_TRANSLATIONS.get("en", {})
    template = translations.get(key)
    if template is None:
        template = fallback_translations.get(key, default if default is not None else key)
    try:
        return str(template).format(**kwargs) if kwargs else str(template)
    except Exception:
        return str(template)


_UI_TRANSLATIONS["sk"].update({
    "config.influx_enabled": "Povoli\u0165 integr\u00e1ciu InfluxDB",
    "config.influx_enabled_note": "Vypnite t\u00fato vo\u013ebu, ak chcete nasadenie bez InfluxDB a chcete pou\u017e\u00edva\u0165 len runtime a TimescaleDB.",
    "config.influx_disabled_notice": "Integr\u00e1cia InfluxDB je vypnut\u00e1. Aplik\u00e1cia bude pou\u017e\u00edva\u0165 iba runtime a TimescaleDB cesty.",
    "config.storage_and_telemetry_note": "Spravujte TimescaleDB ako hlavn\u00e9 \u00falo\u017eisko telemetrie a nastavte monitorovacie prahy.",
    "config.storage.influx_title": "Zdroj telemetrie",
    "config.storage.influx_note": "Ur\u010dite, odkia\u013e UI \u010d\u00edta telemetriu. Pri vypnutom InfluxDB sa pou\u017e\u00edva runtime a TimescaleDB fallback.",
    "config.summary.guidance_storage_text": "TimescaleDB berte ako hlavn\u00e9 opera\u010dn\u00e9 \u00falo\u017eisko. Runtime pou\u017eite pre \u017eiv\u00e9 stavy a InfluxDB nechajte mimo akt\u00edvneho nasadenia.",
    "config.storage.runtime_timescale_note": "Toto nasadenie pou\u017e\u00edva runtime telemetriu a TimescaleDB ako hlavn\u00e9 perzistentn\u00e9 \u00falo\u017eisko. InfluxDB u\u017e nie je s\u00fa\u010das\u0165ou akt\u00edvnej konfigur\u00e1cie.",
    "config.storage.runtime_timescale_source_note": "\u017div\u00e1 telemetria sa \u010d\u00edta z runtime pam\u00e4te. Historick\u00e9 a trvalo ulo\u017een\u00e9 d\u00e1ta s\u00fa v TimescaleDB.",
    "config.storage.runtime_timescale_notice": "Polia pre InfluxDB boli z tohto pracovn\u00e9ho priestoru odstr\u00e1nen\u00e9, preto\u017ee t\u00e1to platforma be\u017e\u00ed v re\u017eime runtime + TimescaleDB.",
    "config.storage.runtime_ready_title": "Runtime + TimescaleDB je akt\u00edvny model",
    "config.storage.section_connection": "Pripojenie a identita",
    "config.storage.section_runtime_behavior": "Z\u00e1pisy a snapshoty",
    "config.storage.section_retention": "Retencia a kompresia",
    "config.storage.section_observability": "Napojen\u00e1 observabilita",
    "config.summary.changed_fields": "Zmenen\u00e9 polia",
    "config.summary.changed_fields_empty": "Zatia\u013e neboli vykonan\u00e9 \u017eiadne zmeny. Upraven\u00e9 polia sa zobrazia tu.",
    "config.summary.changed_fields_more": "A e\u0161te {count} \u010fal\u0161\u00edch upraven\u00fdch pol\u00ed.",
})

def _translate_page_title(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    key = _PAGE_TITLE_KEYS.get(raw)
    if not key:
        return raw
    return _ui_text(key, raw)

def record_bs_event(eui, event_type):
    eui = eui.lower()
    if eui not in bs_uptime_events:
        bs_uptime_events[eui] = []
    bs_uptime_events[eui].append({
        "event": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    if len(bs_uptime_events[eui]) > 500:
        bs_uptime_events[eui] = bs_uptime_events[eui][-500:]

def _track_bs_status_changes(current_statuses):
    global _last_known_bs_status
    for eui, status in current_statuses.items():
        prev = _last_known_bs_status.get(eui)
        if prev != status:
            if status == "connected":
                record_bs_event(eui, "connected")
            elif prev == "connected" and status != "connected":
                record_bs_event(eui, "disconnected")
    _last_known_bs_status = dict(current_statuses)

def _normalize_uptime_event(value):
    """Normalize various event/status payloads to connected/disconnected."""
    raw = str(value or "").strip().lower()
    if raw in {"connected", "connect", "up", "online", "true", "1"}:
        return "connected"
    if raw in {"disconnected", "disconnect", "down", "offline", "false", "0"}:
        return "disconnected"
    return raw

def _extract_influx_uptime_events(csv_text):
    """
    Parse Influx CSV output into {eui: [{event, timestamp}, ...]}.
    Accepts common column names:
      - time: _time|time|timestamp
      - eui: eui|bs_eui|base_station|base_station_eui|gateway|host
      - event: event|status|_value|value
    """
    lines = []
    for line in csv_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        lines.append(line)
    if not lines:
        return {}

    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    result = {}

    for row in reader:
        time_value = (
            row.get("_time")
            or row.get("time")
            or row.get("timestamp")
            or ""
        ).strip()
        eui_value = (
            row.get("eui")
            or row.get("bs_eui")
            or row.get("base_station")
            or row.get("base_station_eui")
            or row.get("gateway")
            or row.get("host")
            or ""
        ).strip().lower()
        event_value = (
            row.get("event")
            or row.get("status")
            or row.get("_value")
            or row.get("value")
            or ""
        ).strip()

        if not eui_value or not time_value or not event_value:
            continue

        normalized = _normalize_uptime_event(event_value)
        if normalized not in {"connected", "disconnected"}:
            continue

        result.setdefault(eui_value, []).append({
            "event": normalized,
            "timestamp": time_value
        })

    for eui in list(result.keys()):
        result[eui].sort(key=lambda item: item.get("timestamp", ""))
    return result

def _build_default_influx_uptime_query():
    bucket = bssci_config.INFLUXDB_BUCKET
    measurement = bssci_config.INFLUX_UPTIME_MEASUREMENT
    field = bssci_config.INFLUX_UPTIME_FIELD
    eui_tag = bssci_config.INFLUX_UPTIME_EUI_TAG
    if not bucket:
        return ""

    # Query expects event/status value in _value and eui in tag column.
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -24h)\n'
        f'  |> filter(fn: (r) => r._measurement == "{measurement}")\n'
        f'  |> filter(fn: (r) => r._field == "{field}")\n'
        f'  |> keep(columns: ["_time", "_value", "{eui_tag}"])\n'
        f'  |> rename(columns: {{"{eui_tag}": "eui"}})\n'
        f'  |> sort(columns: ["_time"])'
    )

def _get_influx_uptime_events():
    """
    Returns dict:
      {
        success: bool,
        uptime_events: {...},
        source: str,
        error?: str
      }
    """
    if not bool(getattr(bssci_config, "INFLUX_ENABLED", True)):
        return {
            "success": False,
            "source": "influxdb",
            "error": "InfluxDB integration is disabled."
        }
    influx_url = bssci_config.INFLUXDB_URL.rstrip("/")
    influx_org = bssci_config.INFLUXDB_ORG
    influx_token = bssci_config.INFLUXDB_TOKEN

    if not influx_url or not influx_org or not influx_token:
        return {
            "success": False,
            "source": "influxdb",
            "error": "InfluxDB configuration is incomplete (URL/ORG/TOKEN)."
        }

    flux_query = bssci_config.INFLUX_UPTIME_QUERY or _build_default_influx_uptime_query()
    if not flux_query:
        return {
            "success": False,
            "source": "influxdb",
            "error": "No Influx uptime query configured (bucket/query missing)."
        }

    query_url = f"{influx_url}/api/v2/query?org={urllib.parse.quote(influx_org)}"
    req = urllib.request.Request(
        query_url,
        method="POST",
        data=flux_query.encode("utf-8"),
        headers={
            "Authorization": f"Token {influx_token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        }
    )

    ssl_ctx = None
    if not bssci_config.INFLUXDB_VERIFY_SSL:
        ssl_ctx = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as response:
            payload = response.read().decode("utf-8", errors="ignore")
        events = _extract_influx_uptime_events(payload)
        return {
            "success": True,
            "source": "influxdb",
            "uptime_events": events
        }
    except Exception as exc:
        return {
            "success": False,
            "source": "influxdb",
            "error": str(exc)
        }

def _influx_write_is_ready():
    return bool(
        getattr(bssci_config, "INFLUX_ENABLED", True)
        and
        bssci_config.INFLUXDB_URL
        and bssci_config.INFLUXDB_ORG
        and bssci_config.INFLUXDB_BUCKET
        and bssci_config.INFLUXDB_TOKEN
    )

def _lp_escape_measurement(value):
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")

def _lp_escape_tag(value):
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace(" ", "\\ ")
        .replace("=", "\\=")
    )

def _lp_escape_field_key(value):
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")

def _lp_encode_field_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return repr(value)
    if value is None:
        return None
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'

def _build_influx_line(measurement, tags, fields, timestamp_ns=None):
    line = _lp_escape_measurement(measurement)

    tag_parts = []
    for key, value in (tags or {}).items():
        if value is None:
            continue
        tag_parts.append(f"{_lp_escape_tag(key)}={_lp_escape_tag(value)}")
    if tag_parts:
        line += "," + ",".join(tag_parts)

    field_parts = []
    for key, value in (fields or {}).items():
        encoded = _lp_encode_field_value(value)
        if encoded is None:
            continue
        field_parts.append(f"{_lp_escape_field_key(key)}={encoded}")
    if not field_parts:
        return None

    line += " " + ",".join(field_parts)
    if timestamp_ns is not None:
        line += f" {int(timestamp_ns)}"
    return line

def _write_influx_lines(lines):
    if not _influx_write_is_ready():
        return False, "InfluxDB write config missing (URL/ORG/BUCKET/TOKEN)."
    if not lines:
        return False, "No line protocol payload to write."

    influx_url = bssci_config.INFLUXDB_URL.rstrip("/")
    write_url = (
        f"{influx_url}/api/v2/write"
        f"?org={urllib.parse.quote(bssci_config.INFLUXDB_ORG)}"
        f"&bucket={urllib.parse.quote(bssci_config.INFLUXDB_BUCKET)}"
        "&precision=ns"
    )
    payload = "\n".join(lines).encode("utf-8")
    req = urllib.request.Request(
        write_url,
        method="POST",
        data=payload,
        headers={
            "Authorization": f"Token {bssci_config.INFLUXDB_TOKEN}",
            "Content-Type": "text/plain; charset=utf-8",
            "Accept": "application/json",
        }
    )

    ssl_ctx = None
    if not bssci_config.INFLUXDB_VERIFY_SSL:
        ssl_ctx = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=5):
            pass
        return True, None
    except Exception as exc:
        return False, str(exc)

def _sanitize_inventory_field_key(key):
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(key or "").strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
    return sanitized

def _normalize_inventory_fields(data):
    normalized = {}
    for key, value in (data or {}).items():
        field_key = _sanitize_inventory_field_key(key)
        if not field_key:
            continue
        if isinstance(value, (dict, list)):
            normalized[field_key] = json.dumps(value, separators=(",", ":"), ensure_ascii=True)
        elif isinstance(value, (str, int, float, bool)) or value is None:
            normalized[field_key] = value
        else:
            normalized[field_key] = str(value)
    return normalized

def _normalize_expected_interval_seconds(value):
    if value in (None, ""):
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        raise ValueError("Expected interval must be a number of seconds.")
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("Expected interval must be greater than 0 seconds.")
    return int(max(10, min(round(seconds), 86400)))

def _normalize_reporting_mode(value):
    raw = str(value or "").strip().lower()
    if raw in {"event", "event-driven", "event_driven", "alarm", "async"}:
        return "event"
    if raw in {"periodic", "interval", "heartbeat"}:
        return "periodic"
    return "auto"

def _sensor_profile_catalog():
    return {
        "auto": {
            "id": "auto",
            "label": "Auto / inferred",
            "description": "Keep manual decoder settings or infer from sensor name and tags.",
            "short_label": "Auto",
            "payload_decoder": "auto",
            "environment_context": "auto",
            "reporting_mode": "auto",
            "expected_interval_seconds": None,
            "stale_after_hours": None,
            "enforced": False,
        },
        "lansen_e2_co2": {
            "id": "lansen_e2_co2",
            "label": "LANSEN E2 CO2",
            "description": "Periodic CO2 telemetry with a 6 minute cadence. Indoor/outdoor interpretation is controlled by environment context.",
            "short_label": "CO2",
            "payload_decoder": "lansen_e2_co2_v1",
            "environment_context": None,
            "reporting_mode": "periodic",
            "expected_interval_seconds": 360,
            "stale_after_hours": None,
            "enforced": True,
        },
        "lansen_m2_contact": {
            "id": "lansen_m2_contact",
            "label": "LANSEN M2 Contact",
            "description": "Event-driven door/window contact sensor with weekly service window.",
            "short_label": "M2 contact",
            "payload_decoder": "lansen_m2_v1",
            "environment_context": "auto",
            "reporting_mode": "event",
            "expected_interval_seconds": None,
            "stale_after_hours": 168,
            "enforced": True,
        },
        "custom": {
            "id": "custom",
            "label": "Custom",
            "description": "Manual combination of decoder, context, reporting mode and timing.",
            "short_label": "Custom",
            "payload_decoder": None,
            "environment_context": None,
            "reporting_mode": None,
            "expected_interval_seconds": None,
            "stale_after_hours": None,
            "enforced": False,
        },
    }

def _normalize_sensor_profile(value):
    raw = str(value or "").strip().lower()
    if raw in {"", "auto", "default", "infer", "inferred"}:
        return "auto"
    aliases = {
        "co2": "lansen_e2_co2",
        "lansen_co2": "lansen_e2_co2",
        "lansen_e2_co2": "lansen_e2_co2",
        "lansen_e2_co2_auto": "lansen_e2_co2",
        "lansen_e2_co2_indoor": "lansen_e2_co2",
        "lansen_e2_co2_outdoor": "lansen_e2_co2",
        "co2_indoor": "lansen_e2_co2",
        "co2_outdoor": "lansen_e2_co2",
        "m2": "lansen_m2_contact",
        "lansen_m2": "lansen_m2_contact",
        "lan-mioty-m2": "lansen_m2_contact",
        "lansen_m2_contact": "lansen_m2_contact",
        "contact": "lansen_m2_contact",
        "door_contact": "lansen_m2_contact",
        "custom": "custom",
        "manual": "custom",
    }
    normalized = aliases.get(raw, raw)
    return normalized if normalized in _sensor_profile_catalog() else "auto"

def _sensor_profile_preset(profile_id):
    return dict(_sensor_profile_catalog().get(_normalize_sensor_profile(profile_id), _sensor_profile_catalog()["auto"]))

def _sensor_profile_options():
    catalog = _sensor_profile_catalog()
    order = [
        "auto",
        "lansen_e2_co2",
        "lansen_m2_contact",
        "custom",
    ]
    return [dict(catalog[key]) for key in order if key in catalog]

def _infer_sensor_profile(sensor):
    explicit = _normalize_sensor_profile((sensor or {}).get("sensor_profile"))
    if explicit not in {"auto", "custom"}:
        return explicit
    decoder = str((sensor or {}).get("payload_decoder", "auto") or "auto").strip().lower()
    environment = str((sensor or {}).get("environment_context", "auto") or "auto").strip().lower()
    reporting_mode = _normalize_reporting_mode((sensor or {}).get("reporting_mode"))
    if decoder == "lansen_e2_co2_v1":
        return "lansen_e2_co2"
    if decoder == "lansen_m2_v1" and reporting_mode in {"auto", "event"}:
        return "lansen_m2_contact"
    return explicit

def _sensor_profile_label(profile_id):
    return str(_sensor_profile_preset(profile_id).get("label") or "Auto / inferred")

def _apply_sensor_profile_defaults(payload):
    profile_id = _normalize_sensor_profile((payload or {}).get("sensor_profile"))
    preset = _sensor_profile_preset(profile_id)
    if not preset.get("enforced"):
        return payload
    payload["payload_decoder"] = str(preset.get("payload_decoder") or payload.get("payload_decoder") or "auto")
    payload["environment_context"] = str(preset.get("environment_context") or payload.get("environment_context") or "auto")
    payload["reporting_mode"] = str(preset.get("reporting_mode") or payload.get("reporting_mode") or "auto")
    payload["expected_interval_seconds"] = preset.get("expected_interval_seconds")
    payload["stale_after_hours"] = preset.get("stale_after_hours")
    return payload

def _normalize_stale_after_hours(value):
    if value in (None, ""):
        return None
    try:
        hours = float(value)
    except (TypeError, ValueError):
        raise ValueError("Stale-after value must be a number of hours.")
    if not math.isfinite(hours) or hours <= 0:
        raise ValueError("Stale-after value must be greater than 0 hours.")
    return int(max(1, min(round(hours), 2160)))

def _infer_sensor_decoder_profile(sensor):
    raw = str((sensor or {}).get("payload_decoder", "auto") or "auto").strip().lower()
    if raw in {"lansen_e2_co2_v1", "lansen_m2_v1", "raw"}:
        return raw
    marker = " ".join([
        str((sensor or {}).get("name", "") or "").strip().lower(),
        " ".join(str(tag or "").strip().lower() for tag in ((sensor or {}).get("tags") or [])),
    ]).strip()
    if "lansen" in marker and "co2" in marker:
        return "lansen_e2_co2_v1"
    if "lansen" in marker and "m2" in marker:
        return "lansen_m2_v1"
    return "auto"

def _infer_sensor_reporting_mode(sensor):
    explicit = _normalize_reporting_mode((sensor or {}).get("reporting_mode"))
    if explicit in {"periodic", "event"}:
        return explicit, "configured"

    profile = _infer_sensor_decoder_profile(sensor)
    marker = " ".join([
        profile,
        str((sensor or {}).get("name", "") or "").strip().lower(),
        " ".join(str(tag or "").strip().lower() for tag in ((sensor or {}).get("tags") or [])),
    ]).strip()

    if profile == "lansen_m2_v1":
        return "event", "device-profile"
    if any(keyword in marker for keyword in ["door", "window", "contact", "reed", "magnet", "alarm", "panic", "button", "leak"]):
        return "event", "device-type"
    return "periodic", "default"

def _infer_sensor_stale_after_hours(sensor, reporting_mode=None):
    explicit = (sensor or {}).get("stale_after_hours")
    if explicit not in (None, ""):
        try:
            normalized = _normalize_stale_after_hours(explicit)
            if normalized:
                return normalized, "configured"
        except ValueError:
            pass

    mode = reporting_mode or _infer_sensor_reporting_mode(sensor)[0]
    if mode != "event":
        return None, "n/a"

    profile = _infer_sensor_decoder_profile(sensor)
    if profile == "lansen_m2_v1":
        return 168, "device-profile"
    return 72, "event-default"

def _infer_sensor_expected_interval_seconds(sensor):
    explicit = (sensor or {}).get("expected_interval_seconds")
    if explicit not in (None, ""):
        try:
            normalized = _normalize_expected_interval_seconds(explicit)
            if normalized:
                return normalized, "configured"
        except ValueError:
            pass

    profile = _infer_sensor_decoder_profile(sensor)
    marker = " ".join([
        profile,
        str((sensor or {}).get("name", "") or "").strip().lower(),
        " ".join(str(tag or "").strip().lower() for tag in ((sensor or {}).get("tags") or [])),
    ])

    if _infer_sensor_reporting_mode(sensor)[0] == "event":
        return 120, "event-reference"
    if profile == "lansen_e2_co2_v1" or ("co2" in marker and "lansen" in marker):
        return 360, "device-profile"
    if any(keyword in marker for keyword in ["water meter", "gas meter", "heat meter", "pulse meter", "meter", "counter"]):
        return 900, "device-type"
    if any(keyword in marker for keyword in ["temperature", "humidity", "climate", "th ", "env", "comfort"]):
        return 300, "device-type"
    if any(keyword in marker for keyword in ["motion", "occupancy", "door", "window", "button", "alarm", "leak"]):
        return 120, "device-type"
    return 120, "default"

def _resolve_sensor_expected_interval(sensor, observed_interval_seconds=None):
    reporting_mode, reporting_mode_source = _infer_sensor_reporting_mode(sensor)
    stale_after_hours, stale_after_source = _infer_sensor_stale_after_hours(sensor, reporting_mode=reporting_mode)

    if reporting_mode == "event":
        stale_threshold_seconds = int(max(3600, round((stale_after_hours or 72) * 3600)))
        return {
            "reporting_mode": reporting_mode,
            "reporting_mode_source": reporting_mode_source,
            "stale_after_hours": stale_after_hours,
            "stale_after_source": stale_after_source,
            "expected_interval_seconds": None,
            "expected_interval_source": "event-driven",
            "delay_threshold_seconds": 0,
            "offline_threshold_seconds": stale_threshold_seconds,
            "stale_threshold_seconds": stale_threshold_seconds,
        }

    base_interval, source = _infer_sensor_expected_interval_seconds(sensor)
    observed = None
    try:
        observed = float(observed_interval_seconds) if observed_interval_seconds not in (None, "") else None
    except (TypeError, ValueError):
        observed = None
    if observed is not None and math.isfinite(observed) and observed > 0:
        observed = max(10, min(round(observed), 86400))
        if observed >= base_interval * 0.75:
            base_interval = max(base_interval, observed)
            if source != "configured":
                source = "observed"

    delay_threshold = max(int(round(base_interval * 2.5)), base_interval + 90)
    offline_threshold = max(int(round(base_interval * 8.0)), delay_threshold + 240)
    return {
        "reporting_mode": reporting_mode,
        "reporting_mode_source": reporting_mode_source,
        "stale_after_hours": None,
        "stale_after_source": "n/a",
        "expected_interval_seconds": int(base_interval),
        "expected_interval_source": source,
        "delay_threshold_seconds": int(delay_threshold),
        "offline_threshold_seconds": int(offline_threshold),
        "stale_threshold_seconds": int(offline_threshold),
    }

def _default_tenant_id():
    raw = str(getattr(bssci_config, "TIMESCALE_DEFAULT_TENANT", "default") or "default").strip().lower()
    raw = re.sub(r"[^a-z0-9:_-]+", "-", raw)
    raw = raw.strip("-_:")
    return raw or "default"

def _sanitize_tenant_id(value):
    candidate = str(value or "").strip().lower()
    candidate = re.sub(r"[^a-z0-9:_-]+", "-", candidate)
    candidate = candidate.strip("-_:")
    if not candidate:
        return ""
    return candidate[:64]

def _normalize_tenant_id(value, fallback=None):
    base = _default_tenant_id() if fallback is None else str(fallback or "").strip().lower()
    base = base or _default_tenant_id()
    candidate = _sanitize_tenant_id(value)
    if not candidate:
        return base
    return candidate

def _tenant_id_from_sensor(sensor):
    return _normalize_tenant_id((sensor or {}).get("tenant_id"), fallback=_default_tenant_id())

def _tenant_id_from_base_station(bs_data):
    return _normalize_tenant_id((bs_data or {}).get("tenant_id"), fallback=_default_tenant_id())

def _is_reserved_default_tenant(tenant_id):
    return _normalize_tenant_id(tenant_id, fallback=_default_tenant_id()) == _default_tenant_id()

def _tenant_scope_display_name(tenant_id, name=None):
    normalized = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    if _is_reserved_default_tenant(normalized):
        return "Super admin"
    candidate = str(name or normalized).strip()
    return candidate or normalized

def _tenant_scope_display_description(tenant_id, description=None):
    if _is_reserved_default_tenant(tenant_id):
        return "Reserved global scope for super admin inventory and system-owned data."
    return str(description or "").strip()

def _is_super_admin(user=None):
    if isinstance(user, dict):
        role = _normalize_user_role(user.get("role", "viewer"))
        raw_tenant = _sanitize_tenant_id(user.get("tenant_id"))
        tenant_value = _normalize_user_tenant_for_role(role, raw_tenant, fallback=_default_tenant_id())
        return role == "admin" and (not str(tenant_value or "").strip() or _is_reserved_default_tenant(raw_tenant))

    if has_request_context():
        role = _normalize_user_role(session.get("role", "viewer"))
        raw_tenant = _sanitize_tenant_id(session.get("tenant_id"))
        tenant_value = _normalize_user_tenant_for_role(role, raw_tenant, fallback=_default_tenant_id())
        return role == "admin" and (not str(tenant_value or "").strip() or _is_reserved_default_tenant(raw_tenant))
    return False

def _is_global_tenant_scope(active_tenant):
    return not str(active_tenant or "").strip()

def _resolve_read_tenant_id(requested_tenant=None, *, fallback=None):
    default_fallback = _default_tenant_id() if fallback is None else str(fallback or "").strip()
    default_fallback = default_fallback or _default_tenant_id()

    if has_request_context():
        current_role = _normalize_user_role(session.get("role", "viewer"))
        active_tenant = _active_tenant_id()
        if _is_customer_role(current_role):
            if _is_global_tenant_scope(active_tenant):
                return _default_tenant_id()
            return _normalize_tenant_id(active_tenant, fallback=_default_tenant_id())
        if str(requested_tenant or "").strip():
            return _normalize_tenant_id(requested_tenant, fallback=default_fallback)
        if _is_global_tenant_scope(active_tenant):
            return default_fallback
        return _normalize_tenant_id(active_tenant, fallback=default_fallback)

    if str(requested_tenant or "").strip():
        return _normalize_tenant_id(requested_tenant, fallback=default_fallback)
    return _normalize_tenant_id(default_fallback, fallback=_default_tenant_id())

def _resolve_write_tenant_id(requested_tenant=None, *, existing_tenant=None):
    # Customer-scoped writes are always pinned to the active tenant from session.
    if has_request_context():
        current_role = _normalize_user_role(session.get("role", "viewer"))
        if _is_customer_role(current_role):
            active_tenant = _active_tenant_id()
            if _is_global_tenant_scope(active_tenant):
                return _default_tenant_id()
            return _normalize_tenant_id(active_tenant, fallback=_default_tenant_id())

    if str(requested_tenant or "").strip():
        return _normalize_tenant_id(requested_tenant, fallback=_default_tenant_id())
    if str(existing_tenant or "").strip():
        return _normalize_tenant_id(existing_tenant, fallback=_default_tenant_id())
    active_tenant = _active_tenant_id()
    if _is_global_tenant_scope(active_tenant):
        return _default_tenant_id()
    return _normalize_tenant_id(active_tenant, fallback=_default_tenant_id())

def _tenant_matches(record_tenant, active_tenant):
    if _is_global_tenant_scope(active_tenant):
        return True
    record_value = _normalize_tenant_id(record_tenant, fallback=_default_tenant_id())
    active_value = _normalize_tenant_id(active_tenant, fallback=_default_tenant_id())
    return record_value == active_value

def _active_tenant_id():
    fallback = _default_tenant_id()
    if not has_request_context():
        return fallback

    role = _normalize_user_role(session.get("role", "viewer"))
    session_tenant = _normalize_user_tenant_for_role(role, session.get("tenant_id"), fallback=fallback)
    requested_tenant = request.headers.get("X-Tenant-Id")
    if requested_tenant and role == "admin":
        return _normalize_tenant_id(requested_tenant, fallback=fallback)
    if role == "admin" and _is_global_tenant_scope(session_tenant):
        return ""
    return _normalize_tenant_id(session_tenant, fallback=fallback)

def _normalize_user_role(role):
    normalized = str(role or "viewer").strip().lower()
    if normalized == "customer":
        normalized = "viewer"
    return normalized or "viewer"

def _is_customer_role(role):
    return _normalize_user_role(role) in {"viewer", "customer"}

def _display_user_role(role):
    normalized = _normalize_user_role(role)
    if normalized == "admin":
        return "admin"
    if _is_customer_role(normalized):
        return "customer"
    return normalized

def _is_customer_blocked_api_path(path: str) -> bool:
    normalized = str(path or "").strip()
    if not normalized.startswith("/api/"):
        return False

    exact_paths = {
        "/api/bssci/status",
        "/api/service/status",
        "/api/base-stations",
        "/api/sensors/reload",
        "/api/sensors/detach-all",
        "/api/sensors/clear",
        "/api/sensors/export",
        "/api/sensors/import",
        "/api/sensors/telemetry/history/export",
        "/api/alerts/triggered",
        "/api/vm/status",
        "/api/traffic/metrics",
        "/api/health",
        "/api/network",
        "/api/grafana/dashboard-url",
        "/api/health/grafana",
        "/api/base-stations/certificates/status",
        "/api/base-stations/uptime",
        "/api/base_stations/status",
        "/api/vm/log",
        "/api/vm/capable",
        "/api/service/restart",
    }
    if normalized in exact_paths:
        return True

    blocked_prefixes = (
        "/api/users",
        "/api/tenants",
        "/api/config",
        "/api/influx",
        "/api/timescale",
        "/api/oms",
        "/api/logs",
        "/api/mqtt",
        "/api/system",
        "/api/certificates",
        "/api/container",
        "/api/audit",
        "/api/vm/activate",
        "/api/vm/deactivate",
        "/api/vm/send/",
    )
    if any(normalized.startswith(prefix) for prefix in blocked_prefixes):
        return True

    if normalized.startswith("/api/base-stations/") and "/certificate/" in normalized:
        return True
    return False

def _bootstrap_admin_default_password(seed_payload=None):
    payload = seed_payload if isinstance(seed_payload, dict) else _load_default_user_seed_payload()
    users = payload.get("users", {}) if isinstance(payload, dict) else {}
    admin = users.get("admin", {}) if isinstance(users, dict) else {}
    return str((admin if isinstance(admin, dict) else {}).get("password") or "admin123")


def _password_storage_looks_hashed(value):
    stored = str(value or "").strip()
    return stored.startswith(("pbkdf2:", "scrypt:", "argon2:"))


def _hash_password_value(raw_password):
    password = str(raw_password or "")
    if not password:
        return ""
    if _password_storage_looks_hashed(password):
        return password
    if generate_password_hash is None:
        return password
    return generate_password_hash(password, method="pbkdf2:sha256:600000")


def _verify_password_value(stored_password, candidate_password):
    stored = str(stored_password or "")
    candidate = str(candidate_password or "")
    if not stored or not candidate:
        return False
    if _password_storage_looks_hashed(stored):
        if check_password_hash is None:
            return False
        try:
            return bool(check_password_hash(stored, candidate))
        except Exception:
            return False
    return secrets.compare_digest(stored, candidate)


def _password_needs_storage_upgrade(stored_password):
    stored = str(stored_password or "").strip()
    return bool(stored) and not _password_storage_looks_hashed(stored)


def _prepare_users_payload_for_persistence(users_data):
    changed = False
    if not isinstance(users_data, dict):
        return changed
    users_map = users_data.get("users", {})
    if not isinstance(users_map, dict):
        return changed
    for _, user in users_map.items():
        if not isinstance(user, dict):
            continue
        password = str(user.get("password") or "")
        if not password:
            continue
        hashed_password = _hash_password_value(password)
        if hashed_password and hashed_password != password:
            user["password"] = hashed_password
            changed = True
    return changed


def _resolve_bootstrap_password_state(user, bootstrap_admin_password=None):
    if _normalize_user_role((user or {}).get("role", "viewer")) != "admin":
        return None
    explicit = str((user or {}).get("bootstrap_password_state") or "").strip().lower()
    if explicit in {"pending", "rotated"}:
        return "pending" if explicit == "pending" or bool((user or {}).get("require_password_change")) else "rotated"
    if bool((user or {}).get("require_password_change")):
        return "pending"
    default_password = str(bootstrap_admin_password or _bootstrap_admin_default_password())
    if _verify_password_value((user or {}).get("password"), default_password):
        return "pending"
    return "rotated"


def _maybe_upgrade_legacy_password_after_login(users_data, username, raw_password):
    if not isinstance(users_data, dict):
        return False
    users_map = users_data.get("users", {})
    if not isinstance(users_map, dict):
        return False
    user = users_map.get(username)
    if not isinstance(user, dict):
        return False
    stored_password = str(user.get("password") or "")
    if not _password_needs_storage_upgrade(stored_password):
        return False
    if not _verify_password_value(stored_password, raw_password):
        return False
    user["password"] = _hash_password_value(raw_password)
    return bool(save_users(users_data))


def _generate_temporary_password():
    return f"Temp!{secrets.token_urlsafe(6)}"


def _serialize_user_for_export(username, user):
    if not isinstance(user, dict):
        return None
    role = _normalize_user_role(user.get("role", "viewer"))
    tenant_id = _normalize_user_tenant_for_role(role, user.get("tenant_id"), fallback=_default_tenant_id())
    record = {
        "username": str(username or "").strip(),
        "name": str(user.get("name") or username or "").strip(),
        "role": _visible_role_name(role),
        "tenant_id": tenant_id,
        "active": bool(user.get("active", True)),
    }
    if role == "admin":
        record["admin_permissions"] = _normalize_admin_permissions(user.get("admin_permissions"))
    return record if record["username"] else None


def _serialize_base_station_for_export(eui, base_station):
    if not isinstance(base_station, dict):
        return None
    normalized_eui = _normalize_eui_upper(base_station.get("eui") or eui)
    if not normalized_eui:
        return None
    return {
        "eui": normalized_eui,
        "name": str(base_station.get("name") or "").strip(),
        "tags": list(base_station.get("tags") or []),
        "ip": str(base_station.get("ip") or "").strip(),
        "gps_lat": base_station.get("gps_lat"),
        "gps_lng": base_station.get("gps_lng"),
        "tenant_id": _tenant_id_from_base_station(base_station),
    }


def _parse_json_upload_or_payload():
    upload = request.files.get("file")
    if upload is not None and getattr(upload, "filename", ""):
        try:
            raw_bytes = upload.read()
            return json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Invalid uploaded JSON file: {exc}")
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return payload
    raise ValueError("Missing JSON payload or uploaded file.")

def _should_force_initial_admin_password_change(user, bootstrap_admin_password=None):
    if not bool(getattr(bssci_config, "AUTH_FORCE_INITIAL_ADMIN_PASSWORD_CHANGE", True)):
        return False
    if _normalize_user_role((user or {}).get("role", "viewer")) != "admin":
        return False
    return _resolve_bootstrap_password_state(user, bootstrap_admin_password) == "pending"

def _bootstrap_login_account_hints():
    if not bool(getattr(bssci_config, "AUTH_BOOTSTRAP_DEFAULT_USERS", True)):
        return []

    payload = _load_default_user_seed_payload()
    users = payload.get("users", {}) if isinstance(payload, dict) else {}
    hints = []

    def add_hint(username, label_key, fallback_label):
        account = users.get(username)
        if not isinstance(account, dict):
            return
        hints.append({
            "username": username,
            "password": str(account.get("password") or ""),
            "label": _ui_text(label_key, fallback_label),
        })

    add_hint("admin", "login.account_admin", "Platform admin")
    if bool(getattr(bssci_config, "AUTH_BOOTSTRAP_DEMO_USERS", True)):
        add_hint("customer", "login.account_customer", "Customer portal")
        add_hint("test", "login.account_test", "Demo tenant")
    return hints

def _default_role_permissions_map():
    customer_permissions = {
        "can_view_all": False,
        "can_add_sensors": True,
        "can_edit_sensors": False,
        "can_manage_alerts": True,
        "can_edit_config": False,
        "can_manage_certificates": False,
        "can_update_system": False,
        "visible_tabs": [
            "dashboard",
            "sensors",
            "alerts",
        ],
    }
    return {
        "admin": {
            "can_view_all": True,
            "can_add_sensors": True,
            "can_edit_sensors": True,
            "can_manage_alerts": True,
            "can_edit_config": True,
            "can_manage_certificates": True,
            "can_update_system": True,
            "visible_tabs": [
                "dashboard",
                "sensors",
                "base_stations",
                "health",
                "network",
                "traffic",
                "oms",
                "logs",
                "config",
                "certificates",
            ],
        },
        "user": {
            "can_view_all": True,
            "can_add_sensors": True,
            "can_edit_sensors": True,
            "can_manage_alerts": True,
            "can_edit_config": False,
            "can_manage_certificates": False,
            "can_update_system": False,
            "visible_tabs": [
                "dashboard",
                "sensors",
                "base_stations",
                "health",
                "network",
                "traffic",
                "oms",
                "logs",
            ],
        },
        "viewer": copy.deepcopy(customer_permissions),
        "customer": copy.deepcopy(customer_permissions),
    }

def _normalize_role_permissions_map(role_permissions):
    defaults = _default_role_permissions_map()
    source = role_permissions if isinstance(role_permissions, dict) else {}
    result = {}
    changed = not isinstance(role_permissions, dict)

    for role_name, default_permissions in defaults.items():
        existing = source.get(role_name)
        if not isinstance(existing, dict):
            result[role_name] = copy.deepcopy(default_permissions)
            changed = True
            continue

        merged = copy.deepcopy(default_permissions)
        for key, value in existing.items():
            if key == "visible_tabs":
                continue
            merged[key] = value

        existing_tabs = existing.get("visible_tabs")
        if isinstance(existing_tabs, list):
            normalized_tabs = []
            for tab in existing_tabs:
                tab_name = str(tab or "").strip()
                if not tab_name or tab_name in normalized_tabs:
                    continue
                if role_name in {"viewer", "customer"} and tab_name not in default_permissions["visible_tabs"]:
                    changed = True
                    continue
                normalized_tabs.append(tab_name)
            if role_name in {"viewer", "customer"}:
                for required_tab in default_permissions["visible_tabs"]:
                    if required_tab not in normalized_tabs:
                        normalized_tabs.append(required_tab)
                        changed = True
            merged["visible_tabs"] = normalized_tabs
        else:
            merged["visible_tabs"] = copy.deepcopy(default_permissions["visible_tabs"])
            changed = True

        if merged != existing:
            changed = True
        result[role_name] = merged

    for role_name, existing in source.items():
        if role_name not in result and isinstance(existing, dict):
            result[role_name] = existing

    return result, changed

def _resolve_role_permissions(role_permissions, role):
    normalized_role = _normalize_user_role(role)
    permissions_map = role_permissions if isinstance(role_permissions, dict) else {}
    direct = permissions_map.get(normalized_role)
    if isinstance(direct, dict):
        return direct
    if _is_customer_role(normalized_role):
        for alias in ("customer", "viewer"):
            alias_permissions = permissions_map.get(alias)
            if isinstance(alias_permissions, dict):
                return alias_permissions
    return {}

def _visible_role_name(role):
    normalized = _normalize_user_role(role)
    if normalized == "admin":
        return "admin"
    if normalized == "user":
        return "user"
    if _is_customer_role(normalized):
        return "customer"
    return normalized

def _visible_role_choices(role_permissions):
    permissions_map = role_permissions if isinstance(role_permissions, dict) else {}
    visible_roles = []
    for role_name in permissions_map.keys():
        visible_name = _visible_role_name(role_name)
        if visible_name not in visible_roles:
            visible_roles.append(visible_name)
    preferred_order = ["admin", "user", "customer"]
    ordered = [role for role in preferred_order if role in visible_roles]
    ordered.extend(role for role in visible_roles if role not in ordered)
    return ordered

def _load_default_user_seed_payload():
    fallback_payload = {
        "users": {
            "admin": {
                "password": "admin123",
                "role": "admin",
                "name": "Administrator",
                "tenant_id": "",
                "active": True,
                "admin_permissions": _normalize_admin_permissions({
                    "manage_users": True,
                    "manage_tenants": True,
                    "manage_configuration": True,
                    "manage_system": True,
                    "manage_certificates": True,
                    "view_admin_audit": True,
                    "export_admin_audit": True,
                    "clear_admin_audit": True,
                    "clear_service_logs": True,
                    "view_service_logs": True,
                }),
            },
            "customer": {
                "password": "customer123",
                "role": "customer",
                "name": "Customer",
                "tenant_id": "",
                "active": True,
            },
            "test": {
                "password": "test",
                "role": "customer",
                "name": "test",
                "tenant_id": "test",
                "active": True,
            },
        },
        "role_permissions": _default_role_permissions_map(),
    }

    try:
        with open(USER_SEED_FILE, 'r', encoding='utf-8') as f:
            payload = json.load(f)
            if isinstance(payload, dict):
                return payload
    except FileNotFoundError:
        return fallback_payload
    except Exception as e:
        logger.warning(f"Failed to read {USER_SEED_FILE}, using fallback bootstrap users: {e}")
    return fallback_payload

def _normalize_user_tenant_for_role(role, tenant_id, fallback=None):
    if _normalize_user_role(role) == "admin":
        return ""
    return _normalize_tenant_id(tenant_id, fallback=fallback or _default_tenant_id())

def _user_belongs_to_tenant(user, tenant_id, fallback=None):
    if not isinstance(user, dict):
        return False
    role = _normalize_user_role(user.get("role", "viewer"))
    if role == "admin":
        return False
    user_tenant = _normalize_user_tenant_for_role(
        role,
        user.get("tenant_id"),
        fallback=fallback or _default_tenant_id(),
    )
    return _tenant_matches(user_tenant, tenant_id)

def _timescale_is_ready():
    if not getattr(bssci_config, "TIMESCALE_ENABLED", False):
        return False, "Timescale disabled."
    if psycopg is None:
        return False, "psycopg driver not installed."
    required = {
        "TIMESCALE_HOST": getattr(bssci_config, "TIMESCALE_HOST", ""),
        "TIMESCALE_DB": getattr(bssci_config, "TIMESCALE_DB", ""),
        "TIMESCALE_USER": getattr(bssci_config, "TIMESCALE_USER", ""),
        "TIMESCALE_PASSWORD": getattr(bssci_config, "TIMESCALE_PASSWORD", ""),
    }
    missing = [key for key, value in required.items() if not str(value or "").strip()]
    if missing:
        return False, f"Timescale config missing: {', '.join(missing)}"
    return True, None

def _timescale_connect():
    ok, err = _timescale_is_ready()
    if not ok:
        return None, err
    try:
        conn = psycopg.connect(
            host=getattr(bssci_config, "TIMESCALE_HOST", "timescaledb"),
            port=int(getattr(bssci_config, "TIMESCALE_PORT", 5432)),
            dbname=getattr(bssci_config, "TIMESCALE_DB", "bssci"),
            user=getattr(bssci_config, "TIMESCALE_USER", "bssci_user"),
            password=getattr(bssci_config, "TIMESCALE_PASSWORD", ""),
            sslmode=getattr(bssci_config, "TIMESCALE_SSLMODE", "disable"),
            connect_timeout=5,
        )
        conn.autocommit = True
        return conn, None
    except Exception as exc:
        return None, str(exc)

def _timescale_apply_policies(cur):
    retention_enabled = bool(getattr(bssci_config, "TIMESCALE_RETENTION_ENABLED", True))
    compression_enabled = bool(getattr(bssci_config, "TIMESCALE_COMPRESSION_ENABLED", True))
    telemetry_retention_days = max(1, int(getattr(bssci_config, "TIMESCALE_TELEMETRY_RETENTION_DAYS", 90)))
    inventory_retention_days = max(1, int(getattr(bssci_config, "TIMESCALE_INVENTORY_RETENTION_DAYS", 365)))
    compression_after_days = max(1, int(getattr(bssci_config, "TIMESCALE_COMPRESSION_AFTER_DAYS", 7)))

    if compression_enabled:
        for table_name, segment_by in (
            ("telemetry_uplink", "tenant_id,sensor_eui,base_station_eui"),
            ("inventory_events", "tenant_id,entity_type,eui"),
            ("inventory_snapshot_points", "tenant_id,entity_type,eui"),
        ):
            try:
                cur.execute(f"""
                    ALTER TABLE {table_name} SET (
                        timescaledb.compress,
                        timescaledb.compress_orderby = 'ts DESC',
                        timescaledb.compress_segmentby = '{segment_by}'
                    )
                """)
            except Exception:
                pass

        for table_name in ("telemetry_uplink", "inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute(
                    "SELECT add_compression_policy(%s, make_interval(days => %s), if_not_exists => TRUE)",
                    (table_name, compression_after_days),
                )
            except Exception:
                pass
    else:
        for table_name in ("telemetry_uplink", "inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute("SELECT remove_compression_policy(%s, if_exists => TRUE)", (table_name,))
            except Exception:
                pass

    if retention_enabled:
        try:
            cur.execute(
                "SELECT add_retention_policy('telemetry_uplink', make_interval(days => %s), if_not_exists => TRUE)",
                (telemetry_retention_days,),
            )
        except Exception:
            pass
        for table_name in ("inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute(
                    "SELECT add_retention_policy(%s, make_interval(days => %s), if_not_exists => TRUE)",
                    (table_name, inventory_retention_days),
                )
            except Exception:
                pass
    else:
        for table_name in ("telemetry_uplink", "inventory_events", "inventory_snapshot_points"):
            try:
                cur.execute("SELECT remove_retention_policy(%s, if_exists => TRUE)", (table_name,))
            except Exception:
                pass

def _ensure_timescale_schema(conn):
    global _timescale_schema_ready
    if _timescale_schema_ready:
        return
    with _timescale_schema_lock:
        if _timescale_schema_ready:
            return
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenants (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
            cur.execute("""
                INSERT INTO tenants (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, ("default", "Default Tenant"))
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenant_registry_meta (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tenant_registry_meta_updated ON tenant_registry_meta (updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL,
                    role TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    tenant_id TEXT NOT NULL DEFAULT '',
                    active BOOLEAN NOT NULL DEFAULT TRUE,
                    require_password_change BOOLEAN NOT NULL DEFAULT FALSE,
                    admin_permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_users_role ON app_users (role)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_users_tenant ON app_users (tenant_id)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_sensors (
                    eui TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_sensors_tenant ON app_sensors (tenant_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_sensors_updated ON app_sensors (updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_base_stations (
                    eui TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_base_stations_tenant ON app_base_stations (tenant_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_base_stations_updated ON app_base_stations (updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_config_state (
                    config_key TEXT PRIMARY KEY,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_audit_log (
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    action TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    target_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'success',
                    actor TEXT NOT NULL DEFAULT 'system',
                    role TEXT NOT NULL DEFAULT '',
                    actor_tenant TEXT NOT NULL DEFAULT 'default',
                    active_tenant TEXT NOT NULL DEFAULT 'default',
                    method TEXT NOT NULL DEFAULT '',
                    path TEXT NOT NULL DEFAULT '',
                    ip TEXT NOT NULL DEFAULT '',
                    user_agent TEXT NOT NULL DEFAULT '',
                    details JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_ts ON admin_audit_log (ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_action_ts ON admin_audit_log (action, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_actor_ts ON admin_audit_log (actor, ts DESC)")
            cur.execute("""
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
                    payload JSONB
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_events_tenant_ts ON inventory_events (tenant_id, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_events_eui_ts ON inventory_events (eui, ts DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory_snapshot_points (
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    entity_type TEXT NOT NULL,
                    eui TEXT NOT NULL,
                    status TEXT,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_points_tenant_ts ON inventory_snapshot_points (tenant_id, ts DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory_snapshot_latest (
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    entity_type TEXT NOT NULL,
                    eui TEXT NOT NULL,
                    status TEXT,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, entity_type, eui)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_latest_updated ON inventory_snapshot_latest (tenant_id, updated_at DESC)")
            cur.execute("""
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
                    payload JSONB
                )
            """)
            cur.execute("ALTER TABLE telemetry_uplink ADD COLUMN IF NOT EXISTS packet_cnt BIGINT")
            cur.execute("ALTER TABLE telemetry_uplink ADD COLUMN IF NOT EXISTS msg_type TEXT NOT NULL DEFAULT 'ul'")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_ts ON telemetry_uplink (tenant_id, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_sensor_ts ON telemetry_uplink (tenant_id, sensor_eui, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_uplink_tenant_bs_ts ON telemetry_uplink (tenant_id, base_station_eui, ts DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alert_rules (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    sensor_eui TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'threshold',
                    name TEXT NOT NULL,
                    metric TEXT NOT NULL DEFAULT '',
                    condition TEXT NOT NULL DEFAULT '',
                    threshold DOUBLE PRECISION,
                    severity TEXT NOT NULL DEFAULT 'warning',
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant_sensor ON alert_rules (tenant_id, sensor_eui)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant_updated ON alert_rules (tenant_id, updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alert_state_current (
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    alert_id TEXT NOT NULL,
                    sensor_eui TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT FALSE,
                    severity TEXT NOT NULL DEFAULT 'warning',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    last_triggered_at TIMESTAMPTZ,
                    last_resolved_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, alert_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_state_current_tenant_updated ON alert_state_current (tenant_id, updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alert_events (
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    alert_id TEXT NOT NULL,
                    sensor_eui TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'warning',
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_events_tenant_ts ON alert_events (tenant_id, ts DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_events_alert_ts ON alert_events (alert_id, ts DESC)")
            try:
                cur.execute("SELECT create_hypertable('inventory_events', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                pass
            try:
                cur.execute("SELECT create_hypertable('inventory_snapshot_points', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                pass
            try:
                cur.execute("SELECT create_hypertable('telemetry_uplink', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                # Keep plain table if Timescale extension privileges are unavailable.
                pass
            try:
                cur.execute("SELECT create_hypertable('alert_events', 'ts', if_not_exists => TRUE, migrate_data => TRUE)")
            except Exception:
                pass
            _timescale_apply_policies(cur)
        _timescale_schema_ready = True

def _timescale_telemetry_enabled():
    return bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)) and bool(
        getattr(bssci_config, "TIMESCALE_TELEMETRY_WRITE_ENABLED", True)
    )


def _has_viewer_demo_inventory() -> bool:
    try:
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id="test")
        return any(str((sensor or {}).get("demo_seed") or "").strip().lower() == "viewer_demo_test" for sensor in sensors)
    except Exception:
        return False


def _ensure_viewer_demo_telemetry_seeded() -> bool:
    if not _timescale_telemetry_enabled():
        return False
    if not _has_viewer_demo_inventory():
        return False

    now_ts = time.time()
    last_attempt = float(_viewer_demo_seed_state.get("last_attempt_ts") or 0.0)
    if _viewer_demo_seed_state.get("seeded") and not _viewer_demo_seed_state.get("last_error"):
        return True
    if last_attempt > 0 and (now_ts - last_attempt) < 15.0:
        return bool(_viewer_demo_seed_state.get("seeded"))

    with _viewer_demo_seed_lock:
        now_ts = time.time()
        last_attempt = float(_viewer_demo_seed_state.get("last_attempt_ts") or 0.0)
        if _viewer_demo_seed_state.get("seeded") and not _viewer_demo_seed_state.get("last_error"):
            return True
        if last_attempt > 0 and (now_ts - last_attempt) < 15.0:
            return bool(_viewer_demo_seed_state.get("seeded"))
        _viewer_demo_seed_state["last_attempt_ts"] = now_ts

        conn = None
        try:
            from viewer_demo_telemetry import TENANT_ID as demo_tenant_id, TENANT_NAME as demo_tenant_name, build_demo_telemetry_rows

            conn, err = _timescale_connect()
            if conn is None:
                raise RuntimeError(err or "TimescaleDB not reachable")
            _ensure_timescale_schema(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM telemetry_uplink WHERE tenant_id = %s", (demo_tenant_id,))
                existing = int((cur.fetchone() or [0])[0] or 0)
                seeded_now = existing <= 0
                if existing <= 0:
                    cur.execute(
                        """
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id)
                        DO UPDATE SET name = EXCLUDED.name
                        """,
                        (demo_tenant_id, demo_tenant_name),
                    )
                    rows = build_demo_telemetry_rows()
                    cur.executemany(
                        """
                        INSERT INTO telemetry_uplink
                            (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
                        VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        [
                            (
                                row["ts"],
                                row["tenant_id"],
                                row["sensor_eui"],
                                row["base_station_eui"],
                                row["snr"],
                                row["rssi"],
                                row.get("packet_loss_pct"),
                                row["packet_cnt"],
                                row.get("msg_type") or "ul",
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in rows
                        ],
                    )
            if seeded_now:
                try:
                    _sync_inventory_snapshot_to_timescale(trigger="viewer_demo_bootstrap")
                except Exception as sync_exc:
                    logger.warning("Viewer demo inventory snapshot bootstrap failed: %s", sync_exc)
                try:
                    _evaluate_triggered_alerts("test", persist_state=True)
                except Exception as alert_exc:
                    logger.warning("Viewer demo alert bootstrap failed: %s", alert_exc)
            _viewer_demo_seed_state["seeded"] = True
            _viewer_demo_seed_state["last_error"] = ""
            return True
        except Exception as exc:
            _viewer_demo_seed_state["seeded"] = False
            _viewer_demo_seed_state["last_error"] = str(exc)
            logger.warning("Viewer demo telemetry bootstrap failed: %s", exc)
            return False
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

def _note_timescale_uplink_stats(**kwargs):
    with _timescale_uplink_stats_lock:
        for key, value in kwargs.items():
            _timescale_uplink_stats[key] = value

def _bump_timescale_uplink_stats(key, delta=1):
    with _timescale_uplink_stats_lock:
        _timescale_uplink_stats[key] = int(_timescale_uplink_stats.get(key, 0) or 0) + int(delta)

def _timescale_observe_queue():
    qsize = _timescale_uplink_queue.qsize()
    with _timescale_uplink_stats_lock:
        current = int(_timescale_uplink_stats.get("queue_high_water", 0) or 0)
        if qsize > current:
            _timescale_uplink_stats["queue_high_water"] = qsize
    return qsize


def _observe_timescale_write_latency(latency_ms: float):
    value = max(0.0, float(latency_ms or 0.0))
    with _timescale_uplink_stats_lock:
        total = float(_timescale_uplink_stats.get("write_latency_total_ms", 0.0) or 0.0) + value
        samples = int(_timescale_uplink_stats.get("write_latency_samples", 0) or 0) + 1
        max_seen = max(float(_timescale_uplink_stats.get("write_latency_max_ms", 0.0) or 0.0), value)
        _timescale_uplink_stats["write_latency_total_ms"] = total
        _timescale_uplink_stats["write_latency_samples"] = samples
        _timescale_uplink_stats["write_latency_last_ms"] = round(value, 2)
        _timescale_uplink_stats["write_latency_max_ms"] = round(max_seen, 2)
        _timescale_uplink_stats["write_latency_avg_ms"] = round(total / samples, 2) if samples > 0 else 0.0

def _timescale_retry_delay(attempt_number: int) -> float:
    if attempt_number <= 0:
        return 0.0
    raw_delay = _TIMESCALE_UPLINK_RETRY_BASE_SECONDS * (2 ** (attempt_number - 1))
    return min(_TIMESCALE_UPLINK_RETRY_MAX_SECONDS, raw_delay)

def get_timescale_uplink_runtime_stats():
    with _timescale_uplink_stats_lock:
        stats = dict(_timescale_uplink_stats)
    queue_size = _timescale_uplink_queue.qsize()
    queue_max = int(getattr(_timescale_uplink_queue, "maxsize", 0) or 0)
    stats["queue_size"] = queue_size
    stats["queue_maxsize"] = queue_max
    stats["queue_utilization_pct"] = round((queue_size / queue_max) * 100.0, 2) if queue_max > 0 else None
    return stats

def _timescale_uplink_worker():
    logger.info("Timescale uplink worker started")
    while not _timescale_uplink_stop.is_set():
        try:
            first = _timescale_uplink_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        batch = [first]
        while len(batch) < _TIMESCALE_UPLINK_BATCH_SIZE:
            try:
                batch.append(_timescale_uplink_queue.get_nowait())
            except queue.Empty:
                break

        if not _timescale_telemetry_enabled():
            _bump_timescale_uplink_stats("failed_batches", 1)
            _bump_timescale_uplink_stats("dropped_write_failures", len(batch))
            _note_timescale_uplink_stats(last_error="telemetry writes disabled; batch discarded")
            continue

        rows = []
        for item in batch:
            try:
                ts_value = item.get("ts")
                if isinstance(ts_value, datetime):
                    ts = ts_value.astimezone(timezone.utc)
                elif isinstance(ts_value, (int, float)):
                    value = float(ts_value)
                    if value > 1_000_000_000_000:  # nanoseconds
                        value = value / 1_000_000_000.0
                    ts = datetime.fromtimestamp(value, tz=timezone.utc)
                else:
                    ts = datetime.now(timezone.utc)
                rows.append((
                    ts,
                    _normalize_tenant_id(item.get("tenant_id"), fallback=_default_tenant_id()),
                    str(item.get("sensor_eui") or "").lower(),
                    (str(item.get("base_station_eui") or "").lower() or None),
                    float(item["snr"]) if item.get("snr") is not None else None,
                    float(item["rssi"]) if item.get("rssi") is not None else None,
                    float(item["packet_loss_pct"]) if item.get("packet_loss_pct") is not None else None,
                    int(item["packet_cnt"]) if item.get("packet_cnt") is not None else None,
                    str(item.get("msg_type") or "ul")[:16],
                    json.dumps(item.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                ))
            except Exception as row_exc:
                _bump_timescale_uplink_stats("dropped_invalid", 1)
                _note_timescale_uplink_stats(last_error=f"invalid telemetry row: {row_exc}")
                logger.warning("Timescale telemetry row dropped (invalid payload): %s", row_exc)

        if not rows:
            continue

        write_success = False
        had_retry = False
        final_error = ""

        for attempt in range(_TIMESCALE_UPLINK_MAX_RETRIES + 1):
            conn = None
            attempt_started = time.perf_counter()
            try:
                conn, err = _timescale_connect()
                if conn is None:
                    final_error = f"connect failed: {err}"
                else:
                    _ensure_timescale_schema(conn)
                    with conn.cursor() as cur:
                        tenant_ids = sorted({row[1] for row in rows if str(row[1]).strip()})
                        if tenant_ids:
                            cur.executemany("""
                                INSERT INTO tenants (id, name)
                                VALUES (%s, %s)
                                ON CONFLICT (id) DO NOTHING
                            """, [(tenant_id, tenant_id) for tenant_id in tenant_ids])
                        cur.executemany("""
                            INSERT INTO telemetry_uplink
                                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, rows)
                    write_success = True
                    _observe_timescale_write_latency((time.perf_counter() - attempt_started) * 1000.0)
                    break
            except Exception as exc:
                final_error = str(exc)
            finally:
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass

            if attempt < _TIMESCALE_UPLINK_MAX_RETRIES:
                had_retry = True
                delay = _timescale_retry_delay(attempt + 1)
                _bump_timescale_uplink_stats("retry_attempts", 1)
                _note_timescale_uplink_stats(
                    last_error=final_error,
                    last_retry_ts=time.time(),
                    last_retry_delay_sec=delay,
                )
                logger.warning(
                    "Timescale uplink write retry %s/%s in %.2fs (batch=%s, error=%s)",
                    attempt + 1,
                    _TIMESCALE_UPLINK_MAX_RETRIES,
                    delay,
                    len(rows),
                    final_error,
                )
                time.sleep(delay)

        if write_success:
            _bump_timescale_uplink_stats("written", len(rows))
            _note_timescale_uplink_stats(last_error="", last_write_ts=time.time())
            if had_retry:
                _bump_timescale_uplink_stats("retried_batches", 1)
        else:
            _bump_timescale_uplink_stats("failed_batches", 1)
            _bump_timescale_uplink_stats("dropped_write_failures", len(rows))
            _note_timescale_uplink_stats(last_error=final_error)
            logger.error("Timescale uplink write dropped batch=%s after retries: %s", len(rows), final_error)

    logger.info("Timescale uplink worker stopped")

def _ensure_timescale_uplink_worker_started():
    global _timescale_uplink_thread
    if _timescale_uplink_thread and _timescale_uplink_thread.is_alive():
        return
    with _timescale_uplink_lock:
        if _timescale_uplink_thread and _timescale_uplink_thread.is_alive():
            return
        _timescale_uplink_stop.clear()
        _timescale_uplink_thread = threading.Thread(target=_timescale_uplink_worker, daemon=True)
        _timescale_uplink_thread.start()

def record_runtime_uplink_telemetry(sensor_eui, base_station_eui=None, snr=None, rssi=None, packet_loss_pct=None, payload=None, packet_cnt=None, msg_type="ul", ts=None, tenant_id=None):
    if not _timescale_telemetry_enabled():
        return False, "Timescale telemetry writes disabled."
    sensor_key = str(sensor_eui or "").strip().lower()
    if not sensor_key:
        return False, "sensor_eui is required"
    resolved_tenant = _normalize_tenant_id(
        tenant_id if tenant_id is not None else _resolve_sensor_tenant(sensor_key),
        fallback=_default_tenant_id()
    )
    _ensure_timescale_uplink_worker_started()
    item = {
        "tenant_id": resolved_tenant,
        "sensor_eui": sensor_key,
        "base_station_eui": str(base_station_eui or "").strip().lower(),
        "snr": snr,
        "rssi": rssi,
        "packet_loss_pct": packet_loss_pct,
        "packet_cnt": packet_cnt,
        "msg_type": str(msg_type or "ul").lower(),
        "payload": payload or {},
        "ts": ts if ts is not None else time.time(),
    }
    try:
        _timescale_uplink_queue.put_nowait(item)
        _bump_timescale_uplink_stats("queued", 1)
        _timescale_observe_queue()
        return True, None
    except queue.Full:
        _bump_timescale_uplink_stats("backpressure_events", 1)
        _bump_timescale_uplink_stats("dropped", 1)
        _note_timescale_uplink_stats(last_error="uplink queue full")
        return False, "queue full"

def _timescale_fetch_telemetry_summary(window_minutes=60, bucket_seconds=60, top_limit=10):
    window_minutes = max(1, min(int(window_minutes or 60), 7 * 24 * 60))
    bucket_seconds = max(15, min(int(bucket_seconds or 60), 3600))
    top_limit = max(1, min(int(top_limit or 10), 100))
    ok, err = _timescale_is_ready()
    if not ok:
        return {
            "success": False,
            "enabled": bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)),
            "error": err,
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
        }

    conn, conn_err = _timescale_connect()
    if conn is None:
        return {
            "success": False,
            "enabled": True,
            "error": conn_err,
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
        }

    tenant_id = _active_tenant_id()
    try:
        _ensure_timescale_schema(conn)
        summary = {
            "success": True,
            "enabled": True,
            "error": None,
            "tenant_id": tenant_id,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
        }
        with conn.cursor() as cur:
            if _is_global_tenant_scope(tenant_id):
                cur.execute("""
                    SELECT
                        COUNT(*)::BIGINT AS uplink_total,
                        COUNT(DISTINCT sensor_eui)::BIGINT AS sensor_count,
                        (COUNT(DISTINCT base_station_eui) FILTER (WHERE base_station_eui IS NOT NULL AND base_station_eui <> ''))::BIGINT AS base_station_count,
                        MAX(ts) AS last_ts
                    FROM telemetry_uplink
                    WHERE ts >= NOW() - (%s * INTERVAL '1 minute')
                """, (window_minutes,))
            else:
                cur.execute("""
                    SELECT
                        COUNT(*)::BIGINT AS uplink_total,
                        COUNT(DISTINCT sensor_eui)::BIGINT AS sensor_count,
                        (COUNT(DISTINCT base_station_eui) FILTER (WHERE base_station_eui IS NOT NULL AND base_station_eui <> ''))::BIGINT AS base_station_count,
                        MAX(ts) AS last_ts
                    FROM telemetry_uplink
                    WHERE tenant_id = %s
                      AND ts >= NOW() - (%s * INTERVAL '1 minute')
                """, (_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), window_minutes))
            row = cur.fetchone() or (0, 0, 0, None)
            summary["uplink_total"] = int(row[0] or 0)
            summary["sensor_count"] = int(row[1] or 0)
            summary["base_station_count"] = int(row[2] or 0)
            summary["last_ts"] = row[3].isoformat() if row[3] else None

            bucket_interval = f"{bucket_seconds} seconds"
            if _is_global_tenant_scope(tenant_id):
                cur.execute("""
                    SELECT
                        EXTRACT(EPOCH FROM bucket)::BIGINT AS bucket_ts,
                        COUNT(*)::BIGINT AS uplinks,
                        AVG(snr)::DOUBLE PRECISION AS avg_snr,
                        AVG(rssi)::DOUBLE PRECISION AS avg_rssi
                    FROM (
                        SELECT time_bucket(%s::interval, ts) AS bucket, snr, rssi
                        FROM telemetry_uplink
                        WHERE ts >= NOW() - (%s * INTERVAL '1 minute')
                    ) t
                    GROUP BY bucket
                    ORDER BY bucket ASC
                """, (bucket_interval, window_minutes))
            else:
                cur.execute("""
                    SELECT
                        EXTRACT(EPOCH FROM bucket)::BIGINT AS bucket_ts,
                        COUNT(*)::BIGINT AS uplinks,
                        AVG(snr)::DOUBLE PRECISION AS avg_snr,
                        AVG(rssi)::DOUBLE PRECISION AS avg_rssi
                    FROM (
                        SELECT time_bucket(%s::interval, ts) AS bucket, snr, rssi
                        FROM telemetry_uplink
                        WHERE tenant_id = %s
                          AND ts >= NOW() - (%s * INTERVAL '1 minute')
                    ) t
                    GROUP BY bucket
                    ORDER BY bucket ASC
                """, (bucket_interval, _normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), window_minutes))
            summary["series"] = [
                {
                    "timestamp": int(series_row[0]),
                    "uplinks": int(series_row[1] or 0),
                    "avg_snr": float(series_row[2]) if series_row[2] is not None else None,
                    "avg_rssi": float(series_row[3]) if series_row[3] is not None else None,
                }
                for series_row in (cur.fetchall() or [])
            ]

            if _is_global_tenant_scope(tenant_id):
                cur.execute("""
                    SELECT
                        sensor_eui,
                        COUNT(*)::BIGINT AS uplinks,
                        AVG(snr)::DOUBLE PRECISION AS avg_snr,
                        AVG(rssi)::DOUBLE PRECISION AS avg_rssi,
                        AVG(packet_loss_pct)::DOUBLE PRECISION AS avg_loss_pct,
                        MAX(ts) AS last_seen
                    FROM telemetry_uplink
                    WHERE ts >= NOW() - (%s * INTERVAL '1 minute')
                    GROUP BY sensor_eui
                    ORDER BY uplinks DESC
                    LIMIT %s
                """, (window_minutes, top_limit))
            else:
                cur.execute("""
                    SELECT
                        sensor_eui,
                        COUNT(*)::BIGINT AS uplinks,
                        AVG(snr)::DOUBLE PRECISION AS avg_snr,
                        AVG(rssi)::DOUBLE PRECISION AS avg_rssi,
                        AVG(packet_loss_pct)::DOUBLE PRECISION AS avg_loss_pct,
                        MAX(ts) AS last_seen
                    FROM telemetry_uplink
                    WHERE tenant_id = %s
                      AND ts >= NOW() - (%s * INTERVAL '1 minute')
                    GROUP BY sensor_eui
                    ORDER BY uplinks DESC
                    LIMIT %s
                """, (_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), window_minutes, top_limit))
            summary["top_sensors"] = [
                {
                    "sensor_eui": str(sensor_row[0] or "").lower(),
                    "uplinks": int(sensor_row[1] or 0),
                    "avg_snr": float(sensor_row[2]) if sensor_row[2] is not None else None,
                    "avg_rssi": float(sensor_row[3]) if sensor_row[3] is not None else None,
                    "avg_packet_loss_pct": float(sensor_row[4]) if sensor_row[4] is not None else None,
                    "last_seen": sensor_row[5].isoformat() if sensor_row[5] else None,
                }
                for sensor_row in (cur.fetchall() or [])
            ]

            if _is_global_tenant_scope(tenant_id):
                cur.execute("""
                    SELECT
                        ts,
                        sensor_eui,
                        base_station_eui,
                        packet_cnt,
                        msg_type,
                        snr,
                        rssi,
                        packet_loss_pct
                    FROM telemetry_uplink
                    WHERE ts >= NOW() - (%s * INTERVAL '1 minute')
                    ORDER BY ts DESC
                    LIMIT 200
                """, (window_minutes,))
            else:
                cur.execute("""
                    SELECT
                        ts,
                        sensor_eui,
                        base_station_eui,
                        packet_cnt,
                        msg_type,
                        snr,
                        rssi,
                        packet_loss_pct
                    FROM telemetry_uplink
                    WHERE tenant_id = %s
                      AND ts >= NOW() - (%s * INTERVAL '1 minute')
                    ORDER BY ts DESC
                    LIMIT 200
                """, (_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), window_minutes))
            summary["recent_messages"] = [
                {
                    "ts": row[0].isoformat() if row[0] else None,
                    "sensor_eui": str(row[1] or "").lower(),
                    "base_station_eui": str(row[2] or "").lower() if row[2] else None,
                    "packet_cnt": int(row[3]) if row[3] is not None else None,
                    "msg_type": str(row[4] or "ul"),
                    "snr": float(row[5]) if row[5] is not None else None,
                    "rssi": float(row[6]) if row[6] is not None else None,
                    "packet_loss_pct": float(row[7]) if row[7] is not None else None,
                }
                for row in (cur.fetchall() or [])
            ]
        return summary
    except Exception as exc:
        return {
            "success": False,
            "enabled": True,
            "error": str(exc),
            "series": [],
            "top_sensors": [],
            "recent_messages": [],
            "uplink_total": 0,
            "sensor_count": 0,
            "base_station_count": 0,
            "last_ts": None,
            "window_minutes": window_minutes,
            "bucket_seconds": bucket_seconds,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _timescale_fetch_sensor_payload_history(sensor_eui: str, tenant_id: Optional[str] = None, limit: int = 10):
    limit = max(1, min(int(limit or 10), 50))
    eui_lower = str(sensor_eui or "").strip().lower()
    tenant_scope = _active_tenant_id() if tenant_id is None else tenant_id
    if not eui_lower:
        return []

    ok, err = _timescale_is_ready()
    if not ok:
        return []

    conn, conn_err = _timescale_connect()
    if conn is None:
        return []

    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            if _is_global_tenant_scope(tenant_scope):
                cur.execute(
                    """
                    SELECT ts, base_station_eui, packet_cnt, snr, rssi, msg_type, payload
                    FROM telemetry_uplink
                    WHERE sensor_eui = %s
                      AND payload IS NOT NULL
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (eui_lower, limit),
                )
            else:
                tenant = _normalize_tenant_id(tenant_scope, fallback=_default_tenant_id())
                cur.execute(
                    """
                    SELECT ts, base_station_eui, packet_cnt, snr, rssi, msg_type, payload
                    FROM telemetry_uplink
                    WHERE tenant_id = %s
                      AND sensor_eui = %s
                      AND payload IS NOT NULL
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (tenant, eui_lower, limit),
                )
            rows = cur.fetchall() or []
        out = []
        sensor_lookup = {}
        try:
            sensor_lookup = {
                str(sensor.get("eui") or "").strip().upper(): sensor
                for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=tenant_scope)
            }
        except Exception:
            sensor_lookup = {}
        sensor_config = sensor_lookup.get(eui_lower.upper())
        for row in rows:
            payload_obj = row[6] if isinstance(row[6], dict) else {}
            raw_dec = payload_obj.get("data") if isinstance(payload_obj.get("data"), list) else None
            raw_hex = ""
            if isinstance(raw_dec, list):
                try:
                    raw_hex = bytes(int(v) & 0xFF for v in raw_dec).hex()
                except Exception:
                    raw_hex = ""
            decoded_payload = _decode_telemetry_payload_for_sensor(
                eui_lower.upper(),
                raw_dec,
                sensor_config=sensor_config,
            )
            out.append(
                {
                    "sensor_eui": eui_lower.upper(),
                    "base_station_eui": str(row[1] or "").upper(),
                    "packet_cnt": int(row[2]) if row[2] is not None else None,
                    "snr": float(row[3]) if row[3] is not None else None,
                    "rssi": float(row[4]) if row[4] is not None else None,
                    "rx_time_ns": int(payload_obj.get("rxTime")) if payload_obj.get("rxTime") is not None else None,
                    "received_at": row[0].isoformat() if row[0] else None,
                    "raw_hex": raw_hex,
                    "raw_dec": raw_dec if isinstance(raw_dec, list) else [],
                    "decoded": decoded_payload,
                }
            )
        return out
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_sensor_lookup(tenant_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    tenant = _active_tenant_id() if tenant_id is None else tenant_id
    try:
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=tenant)
    except Exception:
        sensors = []
    return {
        str(sensor.get("eui") or "").strip().upper(): sensor
        for sensor in sensors
        if str(sensor.get("eui") or "").strip()
    }


def _decode_telemetry_payload_for_sensor(
    sensor_eui: str,
    raw_dec: Any,
    sensor_config: Optional[Dict[str, Any]] = None,
):
    if not isinstance(raw_dec, list):
        return {"profile": "raw", "values": {}, "raw": {}}

    try:
        payload_bytes = [int(value) & 0xFF for value in raw_dec]
    except Exception:
        return {"profile": "raw", "values": {}, "raw": {}}

    sensor_config = dict(sensor_config or {})
    profile = _infer_sensor_decoder_profile(sensor_config)
    tls_server = tls_server_instance

    try:
        tls_server_cls = None
        if not tls_server:
            try:
                from TLSServer import TLSServer as tls_server_cls  # Local import avoids startup coupling.
            except Exception:
                tls_server_cls = None
        if profile == "lansen_e2_co2_v1":
            decoder = getattr(tls_server, "_decode_lansen_e2_co2_payload", None) if tls_server else None
            decoder = decoder or getattr(tls_server_cls, "_decode_lansen_e2_co2_payload", None)
            if decoder:
                decoded = decoder(payload_bytes)
                if decoded:
                    return decoded
        if profile == "lansen_m2_v1":
            decoder = getattr(tls_server, "_decode_lansen_m2_payload", None) if tls_server else None
            decoder = decoder or getattr(tls_server_cls, "_decode_lansen_m2_payload", None)
            if decoder:
                decoded = decoder(payload_bytes)
                if decoded:
                    return decoded
        if tls_server and hasattr(tls_server, "_decode_sensor_payload"):
            decoded = tls_server._decode_sensor_payload(str(sensor_eui or "").upper(), payload_bytes)
            if isinstance(decoded, dict) and decoded:
                return decoded
    except Exception:
        pass

    return {"profile": "raw", "values": {}, "raw": {}}


def _summarize_decoded_values(decoded: Optional[Dict[str, Any]]) -> list[str]:
    decoded = decoded if isinstance(decoded, dict) else {}
    profile = str(decoded.get("profile") or "raw").strip().lower()
    values = decoded.get("values") if isinstance(decoded.get("values"), dict) else {}

    if profile == "lansen_e2_co2_v1":
        parts = []
        co2 = values.get("co2_1_ppm")
        temp = values.get("temperature_1_c")
        rh = values.get("humidity_1_pct")
        battery = values.get("battery_v_est")
        if co2 is not None:
            parts.append(f"CO2 {co2} ppm")
        if temp is not None:
            parts.append(f"Temp {float(temp):.2f} C")
        if rh is not None:
            parts.append(f"RH {rh} %")
        if battery is not None:
            parts.append(f"Battery {float(battery):.1f} V")
        return parts

    if profile == "lansen_m2_v1":
        parts = []
        openings = values.get("total_openings")
        last_alarm = values.get("last_alarm_input")
        alarm_active = values.get("any_alarm_active")
        battery = values.get("battery_v_est")
        if openings is not None:
            parts.append(f"Openings {openings}")
        if last_alarm:
            parts.append(f"Last alarm {last_alarm}")
        parts.append("Alarm active" if alarm_active else "No active alarm")
        if battery is not None:
            parts.append(f"Battery {float(battery):.1f} V")
        return parts

    parts = []
    for key, value in list(values.items())[:4]:
        if isinstance(value, bool):
            rendered = "yes" if value else "no"
        elif isinstance(value, float):
            rendered = f"{value:.2f}"
        else:
            rendered = str(value)
        parts.append(f"{key.replace('_', ' ')}: {rendered}")
    return parts


def _build_telemetry_history_filters(
    tenant_id: Optional[str] = None,
    *,
    sensor_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
):
    tenant = _active_tenant_id() if tenant_id is None else tenant_id
    sensor_lookup = sensor_lookup or _get_sensor_lookup(tenant)
    sensors = [
        {
            "eui": eui,
            "name": str(sensor.get("name") or "").strip(),
            "profile": _infer_sensor_decoder_profile(sensor),
        }
        for eui, sensor in sorted(sensor_lookup.items(), key=lambda item: ((item[1].get("name") or "").lower(), item[0]))
    ]

    base_stations = []
    try:
        base_station_config = load_base_station_config()
        for eui, data in (base_station_config.get("base_stations", {}) or {}).items():
            if not _tenant_matches(_tenant_id_from_base_station(data), tenant):
                continue
            base_stations.append(
                {
                    "eui": str(eui or "").upper(),
                    "name": str((data or {}).get("name") or "").strip(),
                }
            )
    except Exception:
        base_stations = []

    return {
        "sensors": sensors,
        "base_stations": sorted(base_stations, key=lambda item: ((item.get("name") or "").lower(), item.get("eui") or "")),
        "profiles": [
            {"value": "all", "label": "All payloads"},
            {"value": "lansen_e2_co2_v1", "label": "LANSEN E2 CO2"},
            {"value": "lansen_m2_v1", "label": "LANSEN M2"},
            {"value": "raw", "label": "Raw / unknown"},
        ],
    }


def _timescale_fetch_sensor_telemetry_history(
    *,
    tenant_id: Optional[str] = None,
    sensor_eui: Optional[str] = None,
    base_station_eui: Optional[str] = None,
    profile: Optional[str] = None,
    minutes: int = 1440,
    limit: int = 100,
    offset: int = 0,
):
    tenant = _active_tenant_id() if tenant_id is None else tenant_id
    limit = max(1, min(int(limit or 100), 5000))
    offset = max(0, int(offset or 0))
    minutes = max(5, min(int(minutes or 1440), 60 * 24 * 90))
    profile = str(profile or "all").strip().lower()
    sensor_filter = str(sensor_eui or "").strip().upper()
    bs_filter = str(base_station_eui or "").strip().upper()

    ok, err = _timescale_is_ready()
    if not ok:
        return {"success": False, "enabled": False, "error": err or "TimescaleDB is not ready", "rows": [], "total": 0}

    conn, conn_err = _timescale_connect()
    if conn is None:
        return {"success": False, "enabled": True, "error": conn_err or "TimescaleDB connection failed", "rows": [], "total": 0}

    sensor_lookup = _get_sensor_lookup(tenant)
    allowed_sensor_euis = None
    if profile not in {"", "all"}:
        if profile == "raw":
            allowed_sensor_euis = {
                eui.lower()
                for eui, sensor in sensor_lookup.items()
                if _infer_sensor_decoder_profile(sensor) == "auto"
            }
        else:
            allowed_sensor_euis = {
                eui.lower()
                for eui, sensor in sensor_lookup.items()
                if _infer_sensor_decoder_profile(sensor) == profile
            }
        if not allowed_sensor_euis:
            return {
                "success": True,
                "enabled": True,
                "error": None,
                "rows": [],
                "total": 0,
                "limit": limit,
                "offset": offset,
                "minutes": minutes,
                "telemetry_worker": get_timescale_uplink_runtime_stats(),
            }

    filters = ["ts >= NOW() - (%s * INTERVAL '1 minute')"]
    params = [minutes]
    if not _is_global_tenant_scope(tenant):
        filters.insert(0, "tenant_id = %s")
        params.insert(0, _normalize_tenant_id(tenant, fallback=_default_tenant_id()))
    if sensor_filter:
        filters.append("sensor_eui = %s")
        params.append(sensor_filter.lower())
    if bs_filter:
        filters.append("base_station_eui = %s")
        params.append(bs_filter.lower())
    if allowed_sensor_euis is not None:
        filters.append("sensor_eui = ANY(%s)")
        params.append(list(sorted(allowed_sensor_euis)))

    where_sql = " AND ".join(filters)

    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM telemetry_uplink WHERE {where_sql}",
                tuple(params),
            )
            total = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT ts, sensor_eui, base_station_eui, packet_cnt, msg_type, snr, rssi, packet_loss_pct, payload
                FROM telemetry_uplink
                WHERE {where_sql}
                ORDER BY ts DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            rows = cur.fetchall() or []

        results = []
        for row in rows:
            payload_obj = row[8] if isinstance(row[8], dict) else {}
            raw_dec = payload_obj.get("data") if isinstance(payload_obj.get("data"), list) else []
            sensor_key = str(row[1] or "").upper()
            sensor_config = sensor_lookup.get(sensor_key)
            decoded = _decode_telemetry_payload_for_sensor(sensor_key, raw_dec, sensor_config=sensor_config)
            results.append(
                {
                    "ts": row[0].isoformat() if row[0] else None,
                    "sensor_eui": sensor_key,
                    "sensor_name": str((sensor_config or {}).get("name") or "").strip(),
                    "base_station_eui": str(row[2] or "").upper() if row[2] else "",
                    "packet_cnt": int(row[3]) if row[3] is not None else None,
                    "msg_type": str(row[4] or "ul"),
                    "snr": float(row[5]) if row[5] is not None else None,
                    "rssi": float(row[6]) if row[6] is not None else None,
                    "packet_loss_pct": float(row[7]) if row[7] is not None else None,
                    "decoded": decoded,
                    "decoded_summary": _summarize_decoded_values(decoded),
                    "raw_hex": bytes(int(value) & 0xFF for value in raw_dec).hex(" ") if isinstance(raw_dec, list) else "",
                    "raw_dec": raw_dec if isinstance(raw_dec, list) else [],
                }
            )

        return {
            "success": True,
            "enabled": True,
            "error": None,
            "rows": results,
            "total": total,
            "limit": limit,
            "offset": offset,
            "minutes": minutes,
            "telemetry_worker": get_timescale_uplink_runtime_stats(),
            "filters": _build_telemetry_history_filters(tenant, sensor_lookup=sensor_lookup),
        }
    except Exception as exc:
        return {"success": False, "enabled": True, "error": str(exc), "rows": [], "total": 0}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _telemetry_csv_response(rows: list[Dict[str, Any]], *, filename: str):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "timestamp",
        "sensor_eui",
        "sensor_name",
        "base_station_eui",
        "profile",
        "packet_cnt",
        "snr_db",
        "rssi_dbm",
        "packet_loss_pct",
        "decoded_summary",
        "decoded_values_json",
        "raw_hex",
    ])
    for row in rows:
        decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
        writer.writerow([
            row.get("ts") or "",
            row.get("sensor_eui") or "",
            row.get("sensor_name") or "",
            row.get("base_station_eui") or "",
            decoded.get("profile") or "raw",
            row.get("packet_cnt") if row.get("packet_cnt") is not None else "",
            row.get("snr") if row.get("snr") is not None else "",
            row.get("rssi") if row.get("rssi") is not None else "",
            row.get("packet_loss_pct") if row.get("packet_loss_pct") is not None else "",
            " | ".join(str(item) for item in (row.get("decoded_summary") or [])),
            json.dumps(decoded.get("values") or {}, separators=(",", ":"), ensure_ascii=True),
            row.get("raw_hex") or "",
        ])

    csv_text = output.getvalue()
    response = make_response(csv_text)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response

def _parse_bool_arg(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

def _timescale_fetch_tenant_dump(tenant_id, telemetry_limit=50000, events_limit=50000):
    telemetry_limit = max(1, min(int(telemetry_limit or 50000), 250000))
    events_limit = max(1, min(int(events_limit or 50000), 250000))
    conn, err = _timescale_connect()
    if conn is None:
        return False, err, {}
    try:
        _ensure_timescale_schema(conn)
        dump = {
            "tenant_id": tenant_id,
            "inventory_events": [],
            "inventory_snapshot_points": [],
            "inventory_snapshot_latest": [],
            "telemetry_uplink": [],
        }
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, created_at FROM tenants WHERE id = %s", (tenant_id,))
            row = cur.fetchone()
            if row:
                dump["tenant"] = {"id": row[0], "name": row[1], "created_at": str(row[2])}
            else:
                dump["tenant"] = {"id": tenant_id, "name": tenant_id, "created_at": None}

            cur.execute("""
                SELECT ts, tenant_id, entity_type, action, eui, actor, event, has_payload, payload_size, source, payload
                FROM inventory_events
                WHERE tenant_id = %s
                ORDER BY ts DESC
                LIMIT %s
            """, (tenant_id, events_limit))
            dump["inventory_events"] = [
                {
                    "ts": str(r[0]),
                    "tenant_id": r[1],
                    "entity_type": r[2],
                    "action": r[3],
                    "eui": r[4],
                    "actor": r[5],
                    "event": r[6],
                    "has_payload": bool(r[7]),
                    "payload_size": int(r[8] or 0),
                    "source": r[9],
                    "payload": r[10] or {},
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT ts, tenant_id, entity_type, eui, status, trigger, payload
                FROM inventory_snapshot_points
                WHERE tenant_id = %s
                ORDER BY ts DESC
                LIMIT %s
            """, (tenant_id, events_limit))
            dump["inventory_snapshot_points"] = [
                {
                    "ts": str(r[0]),
                    "tenant_id": r[1],
                    "entity_type": r[2],
                    "eui": r[3],
                    "status": r[4],
                    "trigger": r[5],
                    "payload": r[6] or {},
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT tenant_id, entity_type, eui, status, trigger, payload, updated_at
                FROM inventory_snapshot_latest
                WHERE tenant_id = %s
                ORDER BY updated_at DESC
            """, (tenant_id,))
            dump["inventory_snapshot_latest"] = [
                {
                    "tenant_id": r[0],
                    "entity_type": r[1],
                    "eui": r[2],
                    "status": r[3],
                    "trigger": r[4],
                    "payload": r[5] or {},
                    "updated_at": str(r[6]),
                }
                for r in (cur.fetchall() or [])
            ]

            cur.execute("""
                SELECT ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload
                FROM telemetry_uplink
                WHERE tenant_id = %s
                ORDER BY ts DESC
                LIMIT %s
            """, (tenant_id, telemetry_limit))
            dump["telemetry_uplink"] = [
                {
                    "ts": str(r[0]),
                    "tenant_id": r[1],
                    "sensor_eui": r[2],
                    "base_station_eui": r[3],
                    "snr": float(r[4]) if r[4] is not None else None,
                    "rssi": float(r[5]) if r[5] is not None else None,
                    "packet_loss_pct": float(r[6]) if r[6] is not None else None,
                    "packet_cnt": int(r[7]) if r[7] is not None else None,
                    "msg_type": r[8],
                    "payload": r[9] or {},
                }
                for r in (cur.fetchall() or [])
            ]
        return True, None, dump
    except Exception as exc:
        return False, str(exc), {}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _record_inventory_event_to_influx(entity, action, eui, data=None):
    if not bssci_config.INFLUX_INVENTORY_WRITE_ENABLED:
        return False, "Influx inventory writes disabled."
    measurement = bssci_config.INFLUX_INVENTORY_MEASUREMENT or "bssci_inventory_events"
    event_time_ns = int(time.time() * 1_000_000_000)
    actor = session.get("username", "system") if has_request_context() else "system"

    tags = {
        "entity": str(entity or "").lower(),
        "action": str(action or "").lower(),
        "eui": str(eui or "").lower(),
        "actor": actor,
        "source": "service_center_ui",
    }
    fields = {
        "event": f"{entity}_{action}",
        "has_payload": bool(data),
        "payload_size": len(json.dumps(data, ensure_ascii=True)) if data is not None else 0,
    }
    fields.update(_normalize_inventory_fields(data))

    line = _build_influx_line(measurement, tags, fields, event_time_ns)
    if not line:
        return False, "Failed to construct line protocol payload."
    return _write_influx_lines([line])

def _record_inventory_event_to_timescale(entity, action, eui, data=None):
    if not getattr(bssci_config, "TIMESCALE_INVENTORY_WRITE_ENABLED", True):
        return False, "Timescale inventory writes disabled."
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    tenant_id = _active_tenant_id()
    actor = session.get("username", "system") if has_request_context() else "system"
    payload_json = json.dumps(data or {}, separators=(",", ":"), ensure_ascii=True)
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tenants (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (tenant_id, tenant_id))
            cur.execute("""
                INSERT INTO inventory_events
                    (tenant_id, entity_type, action, eui, actor, event, has_payload, payload_size, source, payload)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (
                tenant_id,
                str(entity or "").lower(),
                str(action or "").lower(),
                str(eui or "").lower(),
                actor,
                f"{entity}_{action}",
                bool(data),
                len(payload_json) if data is not None else 0,
                "service_center_ui",
                payload_json,
            ))
        return True, None
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _record_inventory_event(entity, action, eui, data=None):
    sink_results = []
    if bssci_config.INFLUX_INVENTORY_WRITE_ENABLED:
        sink_results.append(("influx",) + _record_inventory_event_to_influx(entity, action, eui, data=data))
    if getattr(bssci_config, "TIMESCALE_INVENTORY_WRITE_ENABLED", True):
        sink_results.append(("timescale",) + _record_inventory_event_to_timescale(entity, action, eui, data=data))

    if not sink_results:
        return False, "All inventory sinks disabled."

    successful = [result for result in sink_results if result[1]]
    if successful:
        return True, None

    errors = [f"{name}: {err}" for name, _, err in sink_results if err]
    return False, "; ".join(errors) if errors else "Inventory write failed."

def _try_record_inventory_event(entity, action, eui, data=None):
    ok, err = _record_inventory_event(entity, action, eui, data=data)
    if not ok and err:
        err_lower = err.lower()
        if "disabled" in err_lower or "config missing" in err_lower:
            return ok, err
        logger.warning("Inventory sink write failed for %s %s %s: %s", entity, action, eui, err)
    return ok, err

def _sensor_runtime_snapshot_by_eui():
    """Build lightweight runtime map for sensors from TLS server structures."""
    snapshot = {}
    global tls_server_instance
    tls_server = tls_server_instance
    if not tls_server:
        return snapshot

    try:
        packet_stats = getattr(tls_server, "sensor_packet_stats", {}) or {}
        for eui_upper, stats in packet_stats.items():
            eui = str(eui_upper or "").strip().upper()
            if not eui:
                continue
            snr_count = int(stats.get("snr_count", 0) or 0)
            rssi_count = int(stats.get("rssi_count", 0) or 0)
            avg_snr = (stats.get("snr_sum", 0.0) / snr_count) if snr_count > 0 else None
            avg_rssi = (stats.get("rssi_sum", 0.0) / rssi_count) if rssi_count > 0 else None
            snapshot[eui] = {
                "packets_received": int(stats.get("packets_received", 0) or 0),
                "packets_lost": int(stats.get("packets_lost", 0) or 0),
                "last_seen": float(stats.get("last_seen", 0) or 0),
                "avg_snr": avg_snr,
                "avg_rssi": avg_rssi,
                "frame_counter": int(stats.get("frame_counter", 0) or 0),
            }
    except Exception:
        pass

    return snapshot


def _timescale_fetch_latest_sensor_snapshots(tenant_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    tenant = _active_tenant_id() if tenant_id is None else tenant_id
    conn, err = _timescale_connect()
    if conn is None:
        return {}
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            if _is_global_tenant_scope(tenant):
                cur.execute(
                    """
                    SELECT eui, payload, updated_at
                    FROM inventory_snapshot_latest
                    WHERE entity_type = 'sensor'
                    """,
                )
            else:
                cur.execute(
                    """
                    SELECT eui, payload, updated_at
                    FROM inventory_snapshot_latest
                    WHERE tenant_id = %s AND entity_type = 'sensor'
                    """,
                    (_normalize_tenant_id(tenant, fallback=_default_tenant_id()),),
                )
            rows = cur.fetchall() or []
        result: Dict[str, Dict[str, Any]] = {}
        for eui, payload, updated_at in rows:
            sensor_eui = str(eui or "").strip().upper()
            if not sensor_eui:
                continue
            result[sensor_eui] = {
                "payload": payload if isinstance(payload, dict) else {},
                "updated_at": updated_at.isoformat() if updated_at else None,
            }
        return result
    except Exception:
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _base_station_runtime_snapshot_by_eui():
    """Build runtime map for base station states."""
    result = {}
    global tls_server_instance
    tls_server = tls_server_instance
    if not tls_server:
        return result

    try:
        connected_map = getattr(tls_server, "connected_base_stations", {}) or {}
        for _, bs_eui in list(connected_map.items()):
            eui = str(bs_eui or "").strip().lower()
            if eui:
                result[eui] = {"status": "connected"}
    except Exception:
        pass

    try:
        connecting_map = getattr(tls_server, "connecting_base_stations", {}) or {}
        for _, bs_eui in list(connecting_map.items()):
            eui = str(bs_eui or "").strip().lower()
            if not eui:
                continue
            if eui not in result:
                result[eui] = {"status": "connecting"}
    except Exception:
        pass

    try:
        health = getattr(tls_server, "base_station_health", {}) or {}
        for eui, metrics in health.items():
            key = str(eui or "").strip().lower()
            if not key:
                continue
            result.setdefault(key, {})
            result[key].update({
                "cpu_load": metrics.get("cpuLoad"),
                "mem_load": metrics.get("memLoad"),
                "duty_cycle": metrics.get("dutyCycle"),
                "uptime": metrics.get("uptime"),
            })
    except Exception:
        pass

    return result

def _build_inventory_snapshot_lines(trigger):
    """Create line protocol rows for all configured sensors and base stations."""
    measurement = bssci_config.INFLUX_SNAPSHOT_MEASUREMENT or "bssci_inventory_snapshot"
    timestamp_ns = int(time.time() * 1_000_000_000)
    lines = []

    # Sensors from config
    sensors = _load_all_sensors()

    runtime_sensor_map = _sensor_runtime_snapshot_by_eui()
    runtime_registered = set()
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            runtime_registered = {str(k).upper() for k in tls_server.registered_sensors.keys()}
    except Exception:
        runtime_registered = set()

    for sensor in sensors:
        eui = str(sensor.get("eui", "")).strip().upper()
        if not eui:
            continue
        tenant_id = _tenant_id_from_sensor(sensor)
        runtime = runtime_sensor_map.get(eui, {})
        packets_received = int(runtime.get("packets_received", 0) or 0)
        packets_lost = int(runtime.get("packets_lost", 0) or 0)
        line = _build_influx_line(
            measurement,
            {
                "entity": "sensor",
                "eui": eui.lower(),
                "tenant": tenant_id,
                "trigger": trigger,
            },
            {
                "configured": True,
                "registered": eui in runtime_registered,
                "bidi": bool(sensor.get("bidi", False)),
                "name": str(sensor.get("name", "") or ""),
                "short_addr": str(sensor.get("shortAddr", "") or ""),
                "tags_json": json.dumps(_normalize_sensor_tags(sensor.get("tags", [])), separators=(",", ":"), ensure_ascii=True),
                "tags_count": len(_normalize_sensor_tags(sensor.get("tags", []))),
                "gps_lat": sensor.get("gps_lat"),
                "gps_lng": sensor.get("gps_lng"),
                "packets_received": packets_received,
                "packets_lost": packets_lost,
                "packet_loss_pct": (packets_lost / (packets_received + packets_lost) * 100.0) if (packets_received + packets_lost) > 0 else 0.0,
                "avg_snr": runtime.get("avg_snr"),
                "avg_rssi": runtime.get("avg_rssi"),
                "last_seen_ts": int(runtime.get("last_seen", 0) or 0),
            },
            timestamp_ns
        )
        if line:
            lines.append(line)

    # Base stations from config
    bs_config = load_base_station_config().get("base_stations", {})
    bs_runtime_map = _base_station_runtime_snapshot_by_eui()

    # Count sensors per base station from registration table
    connected_sensors_per_bs = {}
    try:
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            for _, reg_data in (tls_server.registered_sensors or {}).items():
                for reg in reg_data.get("registrations", []) or []:
                    bs_eui = str(reg.get("bsEui", "")).strip().lower()
                    if bs_eui:
                        connected_sensors_per_bs[bs_eui] = connected_sensors_per_bs.get(bs_eui, 0) + 1
    except Exception:
        connected_sensors_per_bs = {}

    for eui, bs_data in (bs_config or {}).items():
        eui_lower = str(eui or "").strip().lower()
        if not eui_lower:
            continue
        tenant_id = _tenant_id_from_base_station(bs_data)
        runtime = bs_runtime_map.get(eui_lower, {})
        status = runtime.get("status", "disconnected")

        line = _build_influx_line(
            measurement,
            {
                "entity": "base_station",
                "eui": eui_lower,
                "tenant": tenant_id,
                "trigger": trigger,
                "status": status,
            },
            {
                "configured": True,
                "name": str(bs_data.get("name", "") or ""),
                "ip": str(bs_data.get("ip", "") or ""),
                "tags_json": json.dumps(bs_data.get("tags", []), separators=(",", ":"), ensure_ascii=True),
                "gps_lat": bs_data.get("gps_lat"),
                "gps_lng": bs_data.get("gps_lng"),
                "connected": status == "connected",
                "connecting": status == "connecting",
                "connected_sensors": int(connected_sensors_per_bs.get(eui_lower, 0)),
                "cpu_load": runtime.get("cpu_load"),
                "mem_load": runtime.get("mem_load"),
                "duty_cycle": runtime.get("duty_cycle"),
                "uptime": int(runtime.get("uptime", 0) or 0),
            },
            timestamp_ns
        )
        if line:
            lines.append(line)

    return lines

def _build_inventory_snapshot_records(trigger):
    timestamp_iso = datetime.now(timezone.utc).isoformat()
    records = []

    sensors = _load_all_sensors()

    runtime_sensor_map = _sensor_runtime_snapshot_by_eui()
    runtime_registered = set()
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            runtime_registered = {str(k).upper() for k in tls_server.registered_sensors.keys()}
    except Exception:
        runtime_registered = set()

    for sensor in sensors:
        eui = str(sensor.get("eui", "")).strip().upper()
        if not eui:
            continue
        tenant_id = _tenant_id_from_sensor(sensor)
        runtime = runtime_sensor_map.get(eui, {})
        packets_received = int(runtime.get("packets_received", 0) or 0)
        packets_lost = int(runtime.get("packets_lost", 0) or 0)
        registered = eui in runtime_registered
        payload = {
            "configured": True,
            "registered": registered,
            "bidi": bool(sensor.get("bidi", False)),
            "name": str(sensor.get("name", "") or ""),
            "short_addr": str(sensor.get("shortAddr", "") or ""),
            "tags": _normalize_sensor_tags(sensor.get("tags", [])),
            "gps_lat": sensor.get("gps_lat"),
            "gps_lng": sensor.get("gps_lng"),
            "packets_received": packets_received,
            "packets_lost": packets_lost,
            "packet_loss_pct": (packets_lost / (packets_received + packets_lost) * 100.0) if (packets_received + packets_lost) > 0 else 0.0,
            "avg_snr": runtime.get("avg_snr"),
            "avg_rssi": runtime.get("avg_rssi"),
            "last_seen_ts": int(runtime.get("last_seen", 0) or 0),
            "frame_counter": int(runtime.get("frame_counter", 0) or 0),
            "snapshot_time": timestamp_iso,
        }
        records.append({
            "tenant_id": tenant_id,
            "entity_type": "sensor",
            "eui": eui.lower(),
            "status": "registered" if registered else "configured",
            "trigger": trigger,
            "payload": payload,
        })

    bs_config = load_base_station_config().get("base_stations", {})
    bs_runtime_map = _base_station_runtime_snapshot_by_eui()

    connected_sensors_per_bs = {}
    try:
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, "registered_sensors"):
            for _, reg_data in (tls_server.registered_sensors or {}).items():
                for reg in reg_data.get("registrations", []) or []:
                    bs_eui = str(reg.get("bsEui", "")).strip().lower()
                    if bs_eui:
                        connected_sensors_per_bs[bs_eui] = connected_sensors_per_bs.get(bs_eui, 0) + 1
    except Exception:
        connected_sensors_per_bs = {}

    for eui, bs_data in (bs_config or {}).items():
        eui_lower = str(eui or "").strip().lower()
        if not eui_lower:
            continue
        tenant_id = _tenant_id_from_base_station(bs_data)
        runtime = bs_runtime_map.get(eui_lower, {})
        status = runtime.get("status", "disconnected")
        payload = {
            "configured": True,
            "name": str(bs_data.get("name", "") or ""),
            "ip": str(bs_data.get("ip", "") or ""),
            "tags": bs_data.get("tags", []),
            "gps_lat": bs_data.get("gps_lat"),
            "gps_lng": bs_data.get("gps_lng"),
            "connected": status == "connected",
            "connecting": status == "connecting",
            "connected_sensors": int(connected_sensors_per_bs.get(eui_lower, 0)),
            "cpu_load": runtime.get("cpu_load"),
            "mem_load": runtime.get("mem_load"),
            "duty_cycle": runtime.get("duty_cycle"),
            "uptime": int(runtime.get("uptime", 0) or 0),
            "snapshot_time": timestamp_iso,
        }
        records.append({
            "tenant_id": tenant_id,
            "entity_type": "base_station",
            "eui": eui_lower,
            "status": status,
            "trigger": trigger,
            "payload": payload,
        })

    return records

def _sync_inventory_snapshot_to_influx(trigger="manual"):
    if not bool(getattr(bssci_config, "INFLUX_ENABLED", True)):
        return {
            "success": False,
            "error": "InfluxDB integration is disabled.",
            "line_count": 0,
        }
    lines = _build_inventory_snapshot_lines(trigger=trigger)
    sensor_count = sum(1 for line in lines if ",entity=sensor," in line)
    bs_count = sum(1 for line in lines if ",entity=base_station," in line)
    ok, err = _write_influx_lines(lines)
    return {
        "success": ok,
        "error": err,
        "trigger": trigger,
        "line_count": len(lines),
        "sensor_points": sensor_count,
        "base_station_points": bs_count,
    }

def _sync_inventory_snapshot_to_timescale(trigger="manual"):
    if not getattr(bssci_config, "TIMESCALE_SNAPSHOT_ENABLED", True):
        return {
            "success": False,
            "error": "Timescale snapshot writes disabled.",
            "trigger": trigger,
            "line_count": 0,
            "sensor_points": 0,
            "base_station_points": 0,
        }

    conn, err = _timescale_connect()
    if conn is None:
        return {
            "success": False,
            "error": err,
            "trigger": trigger,
            "line_count": 0,
            "sensor_points": 0,
            "base_station_points": 0,
        }

    records = _build_inventory_snapshot_records(trigger=trigger)
    sensor_count = sum(1 for record in records if record.get("entity_type") == "sensor")
    bs_count = sum(1 for record in records if record.get("entity_type") == "base_station")
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            tenant_ids = sorted({
                _normalize_tenant_id(record.get("tenant_id"), fallback=_default_tenant_id())
                for record in records
            })
            if tenant_ids:
                cur.executemany("""
                    INSERT INTO tenants (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, [(tenant_id, tenant_id) for tenant_id in tenant_ids])

            for record in records:
                tenant_id = _normalize_tenant_id(record.get("tenant_id"), fallback=_default_tenant_id())
                payload_json = json.dumps(record.get("payload") or {}, separators=(",", ":"), ensure_ascii=True)
                cur.execute("""
                    INSERT INTO inventory_snapshot_points
                        (tenant_id, entity_type, eui, status, trigger, payload)
                    VALUES
                        (%s, %s, %s, %s, %s, %s::jsonb)
                """, (
                    tenant_id,
                    record.get("entity_type"),
                    record.get("eui"),
                    record.get("status"),
                    record.get("trigger"),
                    payload_json,
                ))
                cur.execute("""
                    INSERT INTO inventory_snapshot_latest
                        (tenant_id, entity_type, eui, status, trigger, payload, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (tenant_id, entity_type, eui)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        trigger = EXCLUDED.trigger,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                """, (
                    tenant_id,
                    record.get("entity_type"),
                    record.get("eui"),
                    record.get("status"),
                    record.get("trigger"),
                    payload_json,
                ))
        return {
            "success": True,
            "error": None,
            "trigger": trigger,
            "line_count": len(records),
            "sensor_points": sensor_count,
            "base_station_points": bs_count,
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "trigger": trigger,
            "line_count": len(records),
            "sensor_points": sensor_count,
            "base_station_points": bs_count,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _influx_snapshot_worker():
    logger.info("Influx snapshot worker started")
    # Initial snapshot shortly after startup
    time.sleep(3)
    last_influx_sync = 0.0
    last_timescale_sync = 0.0
    while not _influx_snapshot_stop.is_set():
        influx_interval = max(15, int(getattr(bssci_config, "INFLUX_SNAPSHOT_INTERVAL_SECONDS", 60)))
        timescale_interval = max(15, int(getattr(bssci_config, "TIMESCALE_SNAPSHOT_INTERVAL_SECONDS", 60)))
        influx_enabled = bool(getattr(bssci_config, "INFLUX_ENABLED", True)) and bool(getattr(bssci_config, "INFLUX_SNAPSHOT_ENABLED", True))
        timescale_enabled = bool(getattr(bssci_config, "TIMESCALE_SNAPSHOT_ENABLED", True))
        now = time.time()
        if influx_enabled and (last_influx_sync <= 0.0 or (now - last_influx_sync) >= influx_interval):
            result = _sync_inventory_snapshot_to_influx(trigger="interval")
            last_influx_sync = now
            if not result.get("success") and result.get("error"):
                err = str(result.get("error", "")).lower()
                if "config missing" not in err:
                    logger.warning("Periodic Influx snapshot failed: %s", result.get("error"))
        if timescale_enabled and (last_timescale_sync <= 0.0 or (now - last_timescale_sync) >= timescale_interval):
            result_ts = _sync_inventory_snapshot_to_timescale(trigger="interval")
            last_timescale_sync = now
            if not result_ts.get("success") and result_ts.get("error"):
                err = str(result_ts.get("error", "")).lower()
                if "disabled" not in err and "missing" not in err:
                    logger.warning("Periodic Timescale snapshot failed: %s", result_ts.get("error"))
        if influx_enabled and timescale_enabled:
            wait_interval = min(influx_interval, timescale_interval)
        elif influx_enabled:
            wait_interval = influx_interval
        elif timescale_enabled:
            wait_interval = timescale_interval
        else:
            wait_interval = 30
        _influx_snapshot_stop.wait(wait_interval)
    logger.info("Influx snapshot worker stopped")

def _ensure_influx_snapshot_worker_started():
    global _influx_snapshot_thread
    if _influx_snapshot_thread and _influx_snapshot_thread.is_alive():
        return
    _influx_snapshot_stop.clear()
    _influx_snapshot_thread = threading.Thread(target=_influx_snapshot_worker, daemon=True)
    _influx_snapshot_thread.start()

def _validate_eui(eui):
    return bool(re.match(r'^[0-9a-f]{16}$', eui.lower()))

def _normalize_sensor_tags(value):
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = re.split(r"[,\|;]", value)
    else:
        raw_items = []

    result = []
    seen = set()
    for item in raw_items:
        tag = str(item or "").strip()
        if not tag:
            continue
        tag_key = tag.lower()
        if tag_key in seen:
            continue
        seen.add(tag_key)
        result.append(tag)
    return result

def _normalize_base_station_route_list(value):
    """Normalize a list/string of base-station EUIs to unique uppercase values."""
    if isinstance(value, str):
        raw_items = re.split(r"[,\|;]", value)
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = []

    result = []
    seen = set()
    for item in raw_items:
        eui = str(item or "").strip().upper()
        if not eui:
            continue
        if not re.match(r"^[0-9A-F]{16}$", eui):
            continue
        if eui in seen:
            continue
        seen.add(eui)
        result.append(eui)
    return result

def _update_sensor_attached_base_stations(sensor_eui, base_station_euis, tenant_id=None):
    """Persist selected attach targets for one sensor inside sensor config."""
    target_eui = str(sensor_eui or "").strip().upper()
    if not target_eui:
        return False

    active_tenant = _active_tenant_id() if tenant_id is None else tenant_id
    normalized_targets = _normalize_base_station_route_list(base_station_euis)
    sensors = _load_all_sensors()
    changed = False
    found = False

    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        if str(sensor.get("eui", "")).strip().upper() != target_eui:
            continue
        if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
            continue
        found = True

        current_targets = _normalize_base_station_route_list(sensor.get("attached_base_stations", []))
        if normalized_targets:
            sensor["attached_base_stations"] = normalized_targets
            changed = current_targets != normalized_targets
        else:
            if "attached_base_stations" in sensor:
                sensor.pop("attached_base_stations", None)
                changed = True
        break

    if found and changed:
        _save_all_sensors(sensors)
    return found and changed


def _sensor_audit_snapshot(sensor: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    sensor = dict(sensor or {})
    return {
        "eui": _normalize_eui_upper(sensor.get("eui")),
        "name": str(sensor.get("name") or "").strip(),
        "tenant_id": _tenant_id_from_sensor(sensor),
        "short_addr": str(sensor.get("shortAddr") or "").strip(),
        "bidi": bool(sensor.get("bidi", False)),
        "tags": _normalize_sensor_tags(sensor.get("tags", [])),
        "sensor_profile": str(sensor.get("sensor_profile") or "auto"),
        "payload_decoder": str(sensor.get("payload_decoder") or "auto"),
        "environment_context": str(sensor.get("environment_context") or "auto"),
        "reporting_mode": _normalize_reporting_mode(sensor.get("reporting_mode")),
        "expected_interval_seconds": _normalize_expected_interval_seconds(sensor.get("expected_interval_seconds")),
        "stale_after_hours": _normalize_stale_after_hours(sensor.get("stale_after_hours")),
        "gps_lat": sensor.get("gps_lat"),
        "gps_lng": sensor.get("gps_lng"),
        "marker_color": sensor.get("marker_color"),
        "attached_base_stations": _normalize_base_station_route_list(sensor.get("attached_base_stations", [])),
        "shared_tenants": [
            _normalize_tenant_id(item, fallback=_default_tenant_id())
            for item in (sensor.get("shared_tenants") or [])
        ],
    }


def _base_station_audit_snapshot(eui: Any, base_station: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base_station = dict(base_station or {})
    return {
        "eui": _normalize_eui_upper(base_station.get("eui") or eui),
        "name": str(base_station.get("name") or "").strip(),
        "tenant_id": _tenant_id_from_base_station(base_station),
        "ip": str(base_station.get("ip") or "").strip(),
        "tags": list(base_station.get("tags") or []),
        "gps_lat": base_station.get("gps_lat"),
        "gps_lng": base_station.get("gps_lng"),
    }


def _audit_changed_fields(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> list[str]:
    before = dict(before or {})
    after = dict(after or {})
    changed = []
    for key in sorted(set(before.keys()) | set(after.keys())):
        if before.get(key) != after.get(key):
            changed.append(str(key))
    return changed

def _load_all_sensors():
    if _db_first_config_enabled():
        bootstrap_state = _bootstrap_sensors_store_if_needed()
        sensors, err = _load_sensors_payload_from_db()
        if isinstance(sensors, list):
            return sensors
        if err:
            logger.warning("Falling back to %s for sensors load: %s", SENSORS_RECOVERY_FILE, err)
        elif bootstrap_state.get("seeded"):
            logger.info("Bootstrapped sensors store from %s", bootstrap_state.get("source"))
    try:
        with open(bssci_config.SENSOR_CONFIG_FILE, "r", encoding="utf-8") as f:
            sensors = json.load(f) or []
        if isinstance(sensors, list):
            return sensors
    except Exception:
        pass
    return []

def _save_all_sensors(sensors):
    if _db_first_config_enabled():
        ok, err = _save_sensors_payload_to_db(list(sensors or []))
        if not ok:
            logger.warning("Falling back to %s for sensors save: %s", SENSORS_RECOVERY_FILE, err)
        else:
            _invalidate_viewer_sensor_list_cache()
            _invalidate_customer_dashboard_cache()
            _invalidate_incident_feed_cache()
            return
    with open(bssci_config.SENSOR_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(list(sensors or []), f, indent=4, ensure_ascii=False)
    _invalidate_viewer_sensor_list_cache()
    _invalidate_customer_dashboard_cache()
    _invalidate_incident_feed_cache()


def _invalidate_viewer_sensor_list_cache(tenant_id: Optional[str] = None):
    with _viewer_sensor_list_cache_lock:
        if tenant_id is None:
            _viewer_sensor_list_cache.clear()
            return
        _viewer_sensor_list_cache.pop(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), None)

def _invalidate_customer_dashboard_cache(tenant_id: Optional[str] = None):
    with _customer_dashboard_cache_lock:
        if tenant_id is None:
            _customer_dashboard_cache.clear()
        else:
            _customer_dashboard_cache.pop(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), None)
    with _customer_dashboard_summary_cache_lock:
        if tenant_id is None:
            _customer_dashboard_summary_cache.clear()
        else:
            _customer_dashboard_summary_cache.pop(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), None)
    with _customer_dashboard_runtime_cache_lock:
        if tenant_id is None:
            _customer_dashboard_runtime_cache.clear()
            return
        _customer_dashboard_runtime_cache.pop(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), None)

def _get_cached_customer_dashboard_payload(tenant_id: str):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _customer_dashboard_cache_lock:
        entry = _customer_dashboard_cache.get(tenant_key)
        if not entry:
            return None
        age = time.time() - float(entry.get("ts") or 0.0)
        if age > _CUSTOMER_DASHBOARD_CACHE_TTL_SECONDS:
            _customer_dashboard_cache.pop(tenant_key, None)
            return None
        return copy.deepcopy(entry.get("payload") or {})

def _store_cached_customer_dashboard_payload(tenant_id: str, payload: Dict[str, Any]):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _customer_dashboard_cache_lock:
        _customer_dashboard_cache[tenant_key] = {
            "ts": time.time(),
            "payload": copy.deepcopy(payload or {}),
        }

def _get_cached_customer_dashboard_summary_payload(tenant_id: str):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _customer_dashboard_summary_cache_lock:
        entry = _customer_dashboard_summary_cache.get(tenant_key)
        if not entry:
            return None
        age = time.time() - float(entry.get("ts") or 0.0)
        if age > _CUSTOMER_DASHBOARD_SUMMARY_CACHE_TTL_SECONDS:
            _customer_dashboard_summary_cache.pop(tenant_key, None)
            return None
        return copy.deepcopy(entry.get("payload") or {})

def _store_cached_customer_dashboard_summary_payload(tenant_id: str, payload: Dict[str, Any]):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _customer_dashboard_summary_cache_lock:
        _customer_dashboard_summary_cache[tenant_key] = {
            "ts": time.time(),
            "payload": copy.deepcopy(payload or {}),
        }

def _get_cached_customer_dashboard_runtime_payload(tenant_id: str):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _customer_dashboard_runtime_cache_lock:
        entry = _customer_dashboard_runtime_cache.get(tenant_key)
        if not entry:
            return None
        age = time.time() - float(entry.get("ts") or 0.0)
        if age > _CUSTOMER_DASHBOARD_RUNTIME_CACHE_TTL_SECONDS:
            _customer_dashboard_runtime_cache.pop(tenant_key, None)
            return None
        return copy.deepcopy(entry.get("payload") or {})

def _store_cached_customer_dashboard_runtime_payload(tenant_id: str, payload: Dict[str, Any]):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _customer_dashboard_runtime_cache_lock:
        _customer_dashboard_runtime_cache[tenant_key] = {
            "ts": time.time(),
            "payload": copy.deepcopy(payload or {}),
        }

def _invalidate_incident_feed_cache(tenant_id: Optional[str] = None):
    with _incident_feed_cache_lock:
        if tenant_id is None:
            _incident_feed_cache.clear()
            return
        _incident_feed_cache.pop(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()), None)

def _get_cached_incident_feed_payload(tenant_id: str):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _incident_feed_cache_lock:
        entry = _incident_feed_cache.get(tenant_key)
        if not entry:
            return None
        age = time.time() - float(entry.get("ts") or 0.0)
        if age > _INCIDENT_FEED_CACHE_TTL_SECONDS:
            _incident_feed_cache.pop(tenant_key, None)
            return None
        return copy.deepcopy(entry.get("payload") or {})

def _store_cached_incident_feed_payload(tenant_id: str, payload: Dict[str, Any]):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _incident_feed_cache_lock:
        _incident_feed_cache[tenant_key] = {
            "ts": time.time(),
            "payload": copy.deepcopy(payload or {}),
        }


def _get_cached_viewer_sensor_list(tenant_id: str):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _viewer_sensor_list_cache_lock:
        entry = _viewer_sensor_list_cache.get(tenant_key)
        if not entry:
            return None
        age = time.time() - float(entry.get("ts") or 0.0)
        if age > _VIEWER_SENSOR_LIST_CACHE_TTL_SECONDS:
            _viewer_sensor_list_cache.pop(tenant_key, None)
            return None
        return copy.deepcopy(entry.get("payload") or {})


def _store_cached_viewer_sensor_list(tenant_id: str, payload: Dict[str, Any]):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    with _viewer_sensor_list_cache_lock:
        _viewer_sensor_list_cache[tenant_key] = {
            "ts": time.time(),
            "payload": copy.deepcopy(payload or {}),
        }

# ── Alert helpers ────────────────────────────────────────────────────────────

_ALERTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts.json")
_alerts_lock = threading.Lock()

def _load_alerts_from_file() -> list:
    try:
        with open(_ALERTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _save_alerts_to_file(alerts: list) -> None:
    with open(_ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(alerts or []), f, indent=4, ensure_ascii=False)

def _resolve_alert_tenant_id(alert: Optional[Dict[str, Any]]) -> str:
    if isinstance(alert, dict) and str(alert.get("tenant_id") or "").strip():
        return _normalize_tenant_id(alert.get("tenant_id"), fallback=_default_tenant_id())
    eui_upper = str((alert or {}).get("sensor_eui") or "").strip().upper()
    if eui_upper:
        try:
            for sensor in _load_all_sensors() or []:
                if str((sensor or {}).get("eui") or "").strip().upper() == eui_upper:
                    return _tenant_id_from_sensor(sensor)
        except Exception:
            pass
    return _default_tenant_id()

def _normalize_stored_alert(alert: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(alert, dict):
        return None
    sensor_eui = str(alert.get("sensor_eui") or "").strip().upper()
    if not sensor_eui:
        return None
    kind = _normalize_alert_kind(alert.get("kind"))
    severity = str(alert.get("severity") or "warning").strip().lower()
    if severity not in {"warning", "critical"}:
        severity = "warning"
    metric = str(alert.get("metric") or "").strip()
    condition = str(alert.get("condition") or "").strip()
    threshold = alert.get("threshold")
    if kind != "threshold":
        metric = ""
        condition = ""
        threshold = None
    else:
        try:
            threshold = float(threshold)
        except (TypeError, ValueError):
            return None
    tenant_id = _resolve_alert_tenant_id(alert)
    created_at = str(alert.get("created_at") or datetime.now(timezone.utc).isoformat())
    name = str(alert.get("name") or "").strip()
    if not name:
        name = f"{metric} {condition} {threshold}" if kind == "threshold" else f"{sensor_eui} offline"
    return {
        "id": str(alert.get("id") or _uuid_mod.uuid4()),
        "tenant_id": tenant_id,
        "sensor_eui": sensor_eui,
        "kind": kind,
        "name": name,
        "metric": metric,
        "condition": condition,
        "threshold": threshold,
        "severity": severity,
        "enabled": bool(alert.get("enabled", True)),
        "created_at": created_at,
    }

def _load_alerts_from_db(conn) -> list:
    _ensure_timescale_schema(conn)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tenant_id, payload
            FROM alert_rules
            ORDER BY created_at ASC, id ASC
        """)
        rows = cur.fetchall() or []
    alerts = []
    for tenant_id, payload in rows:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("tenant_id", tenant_id)
        normalized = _normalize_stored_alert(payload)
        if normalized:
            alerts.append(normalized)
    return alerts

def _save_alerts_to_db(conn, alerts: list) -> None:
    normalized_alerts = []
    for alert in alerts or []:
        normalized = _normalize_stored_alert(alert)
        if normalized:
            normalized_alerts.append(normalized)

    _ensure_timescale_schema(conn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM alert_rules")
            tenant_ids = sorted({_normalize_tenant_id(a.get("tenant_id"), fallback=_default_tenant_id()) for a in normalized_alerts})
            if tenant_ids:
                cur.executemany("""
                    INSERT INTO tenants (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, [(tenant_id, tenant_id) for tenant_id in tenant_ids])
            if normalized_alerts:
                rows = [
                    (
                        alert["id"],
                        _normalize_tenant_id(alert.get("tenant_id"), fallback=_default_tenant_id()),
                        alert["sensor_eui"],
                        alert["kind"],
                        alert["name"],
                        alert.get("metric") or "",
                        alert.get("condition") or "",
                        alert.get("threshold"),
                        alert["severity"],
                        bool(alert.get("enabled", True)),
                        alert["created_at"],
                        json.dumps(alert, separators=(",", ":"), ensure_ascii=True),
                    )
                    for alert in normalized_alerts
                ]
                cur.executemany("""
                    INSERT INTO alert_rules
                        (id, tenant_id, sensor_eui, kind, name, metric, condition, threshold, severity, enabled, created_at, updated_at, payload)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz, NOW(), %s::jsonb)
                """, rows)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass

def _migrate_alerts_file_to_db(conn) -> None:
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM alert_rules")
            row = cur.fetchone()
            existing_count = int((row or [0])[0] or 0)
        if existing_count > 0:
            return
        legacy_alerts = _load_alerts_from_file()
        if not legacy_alerts:
            return
        _save_alerts_to_db(conn, legacy_alerts)
    except Exception:
        return

def _load_alerts() -> list:
    conn = None
    try:
        conn, err = _timescale_connect()
        if conn is not None:
            _migrate_alerts_file_to_db(conn)
            return _load_alerts_from_db(conn)
    except Exception:
        pass
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    alerts = []
    for alert in _load_alerts_from_file():
        normalized = _normalize_stored_alert(alert)
        if normalized:
            alerts.append(normalized)
    return alerts

def _save_alerts(alerts: list) -> None:
    normalized_alerts = []
    for alert in alerts or []:
        normalized = _normalize_stored_alert(alert)
        if normalized:
            normalized_alerts.append(normalized)

    conn = None
    with _alerts_lock:
        try:
            conn, err = _timescale_connect()
            if conn is not None:
                _save_alerts_to_db(conn, normalized_alerts)
        except Exception:
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        _save_alerts_to_file(normalized_alerts)
    _invalidate_incident_feed_cache()

_ALERT_EVENTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_events.json")
_ALERT_STATE_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_state.json")
_alert_events_lock = threading.Lock()
_alert_state_lock_fs = threading.Lock()

def _load_alert_events_file() -> list:
    try:
        with open(_ALERT_EVENTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _append_alert_event_file(event: dict) -> None:
    with _alert_events_lock:
        events = _load_alert_events_file()
        events.append(event)
        # Keep last 2000 events
        if len(events) > 2000:
            events = events[-2000:]
        try:
            with open(_ALERT_EVENTS_FILE, "w", encoding="utf-8") as f:
                json.dump(events, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

def _load_alert_state_file() -> dict:
    try:
        with open(_ALERT_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_alert_state_file(state: dict) -> None:
    with _alert_state_lock_fs:
        try:
            with open(_ALERT_STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

def _iso_timestamp_value(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    text = str(value).strip()
    return text or None

def _normalize_runtime_history_record(
    record: Any,
    *,
    fallback_tenant: Optional[str] = None,
    default_source: str = "threshold",
) -> Optional[Dict[str, Any]]:
    if not isinstance(record, dict):
        return None
    incident_id = str(record.get("id") or record.get("alert_id") or "").strip()
    sensor_eui = str(record.get("sensor_eui") or record.get("eui") or "").strip().upper()
    if not incident_id or not sensor_eui:
        return None

    tenant_id = _normalize_tenant_id(
        record.get("tenant_id"),
        fallback=fallback_tenant or _default_tenant_id(),
    )
    severity_raw = str(record.get("severity") or record.get("tier") or "warning").strip().lower()
    severity = "critical" if severity_raw in {"critical", "error", "danger"} else "warning"
    source = str(record.get("source") or default_source or "threshold").strip().lower() or "threshold"
    kind = str(record.get("kind") or ("activity" if source == "activity" else "threshold")).strip().lower()
    sensor_name = str(record.get("sensor_name") or record.get("name") or sensor_eui).strip() or sensor_eui
    payload = dict(record.get("payload") or {}) if isinstance(record.get("payload"), dict) else {}
    payload.setdefault("id", incident_id)
    payload.setdefault("sensor_eui", sensor_eui)
    payload.setdefault("sensor_name", sensor_name)
    payload.setdefault("name", str(record.get("name") or sensor_name).strip() or sensor_name)
    payload.setdefault("severity", severity)
    payload.setdefault("source", source)
    payload.setdefault("kind", kind)

    optional_fields = {
        "metric": record.get("metric"),
        "condition": record.get("condition"),
        "threshold": record.get("threshold"),
        "current_value": record.get("current_value"),
        "current_status": record.get("current_status"),
        "hours_since_last_seen": record.get("hours_since_last_seen"),
        "reason": record.get("reason"),
        "desc": record.get("desc") or record.get("text"),
        "val_str": record.get("val_str") if record.get("val_str") is not None else record.get("valStr"),
        "rule_id": record.get("rule_id") if record.get("rule_id") is not None else record.get("ruleId"),
        "link_url": record.get("link_url") if record.get("link_url") is not None else record.get("linkUrl"),
    }
    for key, value in optional_fields.items():
        if value not in (None, ""):
            payload[key] = value

    triggered_at = _iso_timestamp_value(record.get("triggered_at"))
    if not triggered_at:
        triggered_at = _iso_timestamp_value(record.get("triggeredAt"))
    if triggered_at:
        payload["triggered_at"] = triggered_at

    return {
        "id": incident_id,
        "tenant_id": tenant_id,
        "sensor_eui": sensor_eui,
        "severity": severity,
        "payload": payload,
    }

def _persist_runtime_history_state(known_records: list, active_records: list) -> None:
    normalized_known = [
        item for item in (
            _normalize_runtime_history_record(record)
            for record in (known_records or [])
        )
        if item
    ]
    if not normalized_known:
        return

    normalized_active = [
        item for item in (
            _normalize_runtime_history_record(
                record,
                fallback_tenant=(normalized_known[0].get("tenant_id") if normalized_known else _default_tenant_id()),
            )
            for record in (active_records or [])
        )
        if item
    ]
    active_by_key = {
        (str(item.get("tenant_id")), str(item.get("id"))): item
        for item in normalized_active
    }

    conn = None
    try:
        conn, err = _timescale_connect()
        if conn is None:
            raise RuntimeError("db unavailable")
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tenant_id, alert_id, is_active, last_triggered_at, last_resolved_at, payload
                FROM alert_state_current
            """)
            state_rows = cur.fetchall() or []
            existing = {}
            for tenant_id, alert_id, is_active, last_triggered_at, last_resolved_at, payload in state_rows:
                existing[(str(tenant_id), str(alert_id))] = {
                    "is_active": bool(is_active),
                    "last_triggered_at": last_triggered_at,
                    "last_resolved_at": last_resolved_at,
                    "payload": payload if isinstance(payload, dict) else {},
                }

            now_iso = datetime.now(timezone.utc).isoformat()
            for known in normalized_known:
                key = (str(known.get("tenant_id")), str(known.get("id")))
                previous = existing.get(key) or {}
                was_active = bool(previous.get("is_active", False))
                active_item = active_by_key.get(key)
                is_active_now = active_item is not None
                if not is_active_now and not was_active:
                    continue

                if is_active_now:
                    active_payload = dict(active_item.get("payload") or {})
                    active_payload.setdefault(
                        "triggered_at",
                        _iso_timestamp_value(previous.get("last_triggered_at")) or now_iso,
                    )
                    payload_for_state = active_payload
                    payload_json = json.dumps(active_payload, separators=(",", ":"), ensure_ascii=True)
                    if not was_active:
                        cur.execute("""
                            INSERT INTO alert_events
                                (tenant_id, alert_id, sensor_eui, event_type, severity, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s::jsonb)
                        """, (
                            known.get("tenant_id"),
                            known.get("id"),
                            known.get("sensor_eui"),
                            "triggered",
                            known.get("severity", "warning"),
                            payload_json,
                        ))
                    last_triggered_at = active_payload.get("triggered_at") or now_iso
                    last_resolved_at = previous.get("last_resolved_at")
                else:
                    resolved_payload = dict(previous.get("payload") or known.get("payload") or {})
                    resolved_payload.setdefault(
                        "triggered_at",
                        _iso_timestamp_value(previous.get("last_triggered_at")),
                    )
                    resolved_payload["resolved_at"] = now_iso
                    payload_for_state = resolved_payload
                    payload_json = json.dumps(resolved_payload, separators=(",", ":"), ensure_ascii=True)
                    cur.execute("""
                        INSERT INTO alert_events
                            (tenant_id, alert_id, sensor_eui, event_type, severity, payload)
                        VALUES
                            (%s, %s, %s, %s, %s, %s::jsonb)
                    """, (
                        known.get("tenant_id"),
                        known.get("id"),
                        known.get("sensor_eui"),
                        "resolved",
                        previous.get("payload", {}).get("severity") or known.get("severity", "warning"),
                        payload_json,
                    ))
                    last_triggered_at = (
                        resolved_payload.get("triggered_at")
                        or _iso_timestamp_value(previous.get("last_triggered_at"))
                    )
                    last_resolved_at = now_iso

                cur.execute("""
                    INSERT INTO alert_state_current
                        (tenant_id, alert_id, sensor_eui, is_active, severity, payload, last_triggered_at, last_resolved_at, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s::jsonb, %s::timestamptz, %s::timestamptz, NOW())
                    ON CONFLICT (tenant_id, alert_id)
                    DO UPDATE SET
                        sensor_eui = EXCLUDED.sensor_eui,
                        is_active = EXCLUDED.is_active,
                        severity = EXCLUDED.severity,
                        payload = EXCLUDED.payload,
                        last_triggered_at = EXCLUDED.last_triggered_at,
                        last_resolved_at = EXCLUDED.last_resolved_at,
                        updated_at = NOW()
                """, (
                    known.get("tenant_id"),
                    known.get("id"),
                    known.get("sensor_eui"),
                    is_active_now,
                    payload_for_state.get("severity") or known.get("severity", "warning"),
                    payload_json,
                    last_triggered_at,
                    last_resolved_at,
                ))
    except Exception:
        try:
            state = _load_alert_state_file()
            now_iso = datetime.now(timezone.utc).isoformat()
            for known in normalized_known:
                state_key = f"{known.get('tenant_id')}::{known.get('id')}"
                previous = state.get(state_key) or {}
                was_active = bool(previous.get("is_active", False))
                active_item = active_by_key.get((str(known.get("tenant_id")), str(known.get("id"))))
                is_active_now = active_item is not None
                if not is_active_now and not was_active:
                    continue

                if is_active_now:
                    active_payload = dict(active_item.get("payload") or {})
                    active_payload.setdefault("triggered_at", previous.get("triggered_at") or now_iso)
                    if not was_active:
                        _append_alert_event_file({
                            "ts": now_iso,
                            "alert_id": known.get("id"),
                            "tenant_id": known.get("tenant_id"),
                            "sensor_eui": known.get("sensor_eui"),
                            "event_type": "triggered",
                            "severity": known.get("severity", "warning"),
                            **active_payload,
                        })
                    state[state_key] = {
                        "is_active": True,
                        "triggered_at": active_payload.get("triggered_at"),
                        "payload": active_payload,
                    }
                else:
                    resolved_payload = dict(previous.get("payload") or known.get("payload") or {})
                    resolved_payload.setdefault("triggered_at", previous.get("triggered_at"))
                    resolved_payload["resolved_at"] = now_iso
                    _append_alert_event_file({
                        "ts": now_iso,
                        "alert_id": known.get("id"),
                        "tenant_id": known.get("tenant_id"),
                        "sensor_eui": known.get("sensor_eui"),
                        "event_type": "resolved",
                        "severity": previous.get("payload", {}).get("severity") or known.get("severity", "warning"),
                        **resolved_payload,
                    })
                    state[state_key] = {
                        "is_active": False,
                        "triggered_at": resolved_payload.get("triggered_at"),
                        "resolved_at": now_iso,
                        "payload": resolved_payload,
                    }
            _save_alert_state_file(state)
        except Exception:
            return
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

def _persist_alert_runtime_state(alerts: list, triggered: list) -> None:
    normalized_alerts = [a for a in (_normalize_stored_alert(alert) for alert in alerts or []) if a]
    if not normalized_alerts:
        return
    known_records = []
    for alert in normalized_alerts:
        kind = _normalize_alert_kind(alert.get("kind"))
        known_records.append({
            "id": alert.get("id"),
            "tenant_id": alert.get("tenant_id"),
            "sensor_eui": alert.get("sensor_eui"),
            "severity": alert.get("severity"),
            "name": alert.get("name"),
            "kind": kind,
            "metric": alert.get("metric"),
            "condition": alert.get("condition"),
            "threshold": alert.get("threshold"),
            "source": "offline_rule" if kind == "sensor_offline" else "threshold",
            "payload": dict(alert),
        })
    active_records = []
    for item in (triggered or []):
        kind = _normalize_alert_kind(item.get("kind"))
        active_records.append({
            "id": item.get("id"),
            "tenant_id": item.get("tenant_id"),
            "sensor_eui": item.get("sensor_eui"),
            "severity": item.get("severity"),
            "name": item.get("name"),
            "kind": kind,
            "metric": item.get("metric"),
            "condition": item.get("condition"),
            "threshold": item.get("threshold"),
            "current_value": item.get("current_value"),
            "current_status": item.get("current_status"),
            "hours_since_last_seen": item.get("hours_since_last_seen"),
            "triggered_at": item.get("triggered_at"),
            "source": "offline_rule" if kind == "sensor_offline" else "threshold",
            "payload": dict(item),
        })
    _persist_runtime_history_state(known_records, active_records)

def _persist_alert_state_file_fallback(alerts: list, triggered: list) -> None:
    """Backward-compatible wrapper; generic runtime history fallback handles file persistence."""
    try:
        _persist_alert_runtime_state(alerts, triggered)
    except Exception:
        pass

def _eval_alert(condition: str, value: float, threshold: float) -> bool:
    try:
        v, t = float(value), float(threshold)
        return {"gt": v > t, "gte": v >= t, "lt": v < t, "lte": v <= t, "eq": abs(v - t) < 1e-9}.get(condition, False)
    except Exception:
        return False

def _sensor_latest_values(eui: str, active_tenant: str) -> dict:
    """Return latest decoded metric values for a sensor. Fast: tries runtime first, then Timescale."""
    global tls_server_instance
    tls = tls_server_instance
    if tls:
        try:
            uplink = tls.get_sensor_latest_uplink(eui.upper())
            if uplink:
                vals = (uplink.get("decoded") or {}).get("values") or {}
                if vals:
                    return vals
        except Exception:
            pass
    try:
        query_tenant = _resolve_query_tenant_for_sensor(eui.upper(), active_tenant)
        rows = _timescale_fetch_sensor_payload_history(eui.upper(), tenant_id=query_tenant, limit=1)
        if rows:
            return (rows[0].get("decoded") or {}).get("values") or {}
    except Exception:
        pass
    return {}


def _sensor_latest_decoded_payload(eui: str, active_tenant: str) -> dict:
    """Return the latest decoded payload envelope for a sensor."""
    global tls_server_instance
    eui_upper = str(eui or "").strip().upper()
    if not eui_upper:
        return {"profile": "raw", "values": {}}

    tls = tls_server_instance
    if tls:
        try:
            uplink = tls.get_sensor_latest_uplink(eui_upper)
            if uplink and isinstance(uplink.get("decoded"), dict):
                return uplink.get("decoded") or {"profile": "raw", "values": {}}
        except Exception:
            pass

    try:
        query_tenant = _resolve_query_tenant_for_sensor(eui_upper, active_tenant)
        rows = _timescale_fetch_sensor_payload_history(eui_upper, tenant_id=query_tenant, limit=1)
        if rows and isinstance(rows[0].get("decoded"), dict):
            return rows[0].get("decoded") or {"profile": "raw", "values": {}}
    except Exception:
        pass
    return {"profile": "raw", "values": {}}


def _normalize_alert_kind(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"sensor_offline", "offline", "sensor_unavailable", "availability"}:
        return "sensor_offline"
    return "threshold"


def _build_visible_sensor_lookup(active_tenant: str) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    try:
        sensors = _load_all_sensors()
    except Exception:
        sensors = []
    for sensor in sensors or []:
        if not isinstance(sensor, dict):
            continue
        eui_upper = str(sensor.get("eui") or "").strip().upper()
        if not eui_upper:
            continue
        if not (
            _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant)
            or _sensor_shared_with_tenant(sensor, active_tenant)
        ):
            continue
        payload = dict(sensor)
        payload["eui"] = eui_upper
        payload["name"] = str(sensor.get("name") or "").strip()
        payload["tags"] = _normalize_sensor_tags(sensor.get("tags", []))
        lookup[eui_upper] = payload
    return lookup


def _alert_metric_metadata() -> Dict[str, Dict[str, str]]:
    return {
        "co2_1_ppm":                        {"label": "CO₂",                        "unit": "ppm", "value_type": "number"},
        "co2_2_ppm":                        {"label": "CO₂ (predch.)",              "unit": "ppm", "value_type": "number"},
        "temperature_1_c":                  {"label": "Teplota",                    "unit": "°C",  "value_type": "number"},
        "temperature_2_c":                  {"label": "Teplota 2",                  "unit": "°C",  "value_type": "number"},
        "humidity_1_pct":                   {"label": "Vlhkosť",                    "unit": "%",   "value_type": "number"},
        "humidity_2_pct":                   {"label": "Vlhkosť 2",                  "unit": "%",   "value_type": "number"},
        "battery_v_est":                    {"label": "Batéria",                    "unit": "V",   "value_type": "number"},
        "battery_mv":                       {"label": "Batéria",                    "unit": "mV",  "value_type": "number"},
        "any_alarm_active":                 {"label": "Alarm aktívny",              "unit": "",    "value_type": "boolean"},
        "alarm_count":                      {"label": "Počet alarmov",              "unit": "",    "value_type": "number"},
        "open_count":                       {"label": "Počet otvorení",             "unit": "",    "value_type": "number"},
        "total_openings":                   {"label": "Počet otvorení (celk.)",     "unit": "",    "value_type": "number"},
        "last_alarm_duration_s":            {"label": "Trvanie alarmu",             "unit": "s",   "value_type": "number"},
        "last_alarm_input":                 {"label": "Vstup alarmu",               "unit": "",    "value_type": "number"},
        "tamper":                           {"label": "Tamper / sabotáž",           "unit": "",    "value_type": "boolean"},
        "internal_magnet_alarm":            {"label": "Interný alarm",              "unit": "",    "value_type": "boolean"},
        "external_alarm":                   {"label": "Externý alarm",              "unit": "",    "value_type": "boolean"},
        "internal_magnet_alarm_last_5min":  {"label": "Interný alarm (5 min)",      "unit": "",    "value_type": "boolean"},
        "internal_magnet_alarm_last_10min": {"label": "Interný alarm (10 min)",     "unit": "",    "value_type": "boolean"},
        "internal_magnet_alarm_last_1h":    {"label": "Interný alarm (1 hod)",      "unit": "",    "value_type": "boolean"},
        "internal_magnet_alarm_last_24h":   {"label": "Interný alarm (24 hod)",     "unit": "",    "value_type": "boolean"},
        "external_alarm_last_5min":         {"label": "Externý alarm (5 min)",      "unit": "",    "value_type": "boolean"},
        "external_alarm_last_10min":        {"label": "Externý alarm (10 min)",     "unit": "",    "value_type": "boolean"},
        "external_alarm_last_1h":           {"label": "Externý alarm (1 hod)",      "unit": "",    "value_type": "boolean"},
        "external_alarm_last_24h":          {"label": "Externý alarm (24 hod)",     "unit": "",    "value_type": "boolean"},
        "minutes_since_last_alarm":         {"label": "Minúty od posledného alarmu","unit": "min", "value_type": "number"},
        "duration_last_alarm_minutes":      {"label": "Trvanie posledného alarmu",  "unit": "min", "value_type": "number"},
        "co2_last_calibration_ppm":         {"label": "CO₂ kalibrácia",             "unit": "ppm", "value_type": "number"},
        "days_to_next_calibration":         {"label": "Dní do kalibrácie",          "unit": "d",   "value_type": "number"},
        "calibration_not_done":             {"label": "Kalibrácia chýba",           "unit": "",    "value_type": "boolean"},
        "co2_error":                        {"label": "Chyba CO₂ senzora",          "unit": "",    "value_type": "boolean"},
        "operating_years":                  {"label": "Roky prevádzky",             "unit": "r.",  "value_type": "number"},
        "runtime_years":                    {"label": "Roky behu",                  "unit": "r.",  "value_type": "number"},
        "low_batt":                         {"label": "Slabá batéria",              "unit": "",    "value_type": "boolean"},
        "sabotage_internal":                {"label": "Interná sabotáž",            "unit": "",    "value_type": "boolean"},
        "sabotage_external":                {"label": "Externá sabotáž",            "unit": "",    "value_type": "boolean"},
        "async_message":                    {"label": "Asynchrónna správa",         "unit": "",    "value_type": "boolean"},
    }


def _alert_profile_default_metric_keys() -> Dict[str, list[str]]:
    return {
        "lansen_e2_co2_v1": [
            "co2_1_ppm",
            "co2_2_ppm",
            "temperature_1_c",
            "temperature_2_c",
            "humidity_1_pct",
            "humidity_2_pct",
            "battery_v_est",
            "co2_last_calibration_ppm",
            "days_to_next_calibration",
            "calibration_not_done",
            "co2_error",
        ],
        "lansen_m2_v1": [
            "total_openings",
            "any_alarm_active",
            "internal_magnet_alarm",
            "external_alarm",
            "internal_magnet_alarm_last_5min",
            "internal_magnet_alarm_last_10min",
            "internal_magnet_alarm_last_1h",
            "internal_magnet_alarm_last_24h",
            "external_alarm_last_5min",
            "external_alarm_last_10min",
            "external_alarm_last_1h",
            "external_alarm_last_24h",
            "minutes_since_last_alarm",
            "duration_last_alarm_minutes",
            "battery_v_est",
            "battery_mv",
            "low_batt",
            "sabotage_internal",
            "sabotage_external",
            "async_message",
        ],
    }


def _humanize_alert_metric_key(metric_key: Any) -> str:
    text = str(metric_key or "").strip().replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text.title() if text else "Metric"


def _resolve_sensor_alert_profile(sensor_config: Optional[Dict[str, Any]], latest_profile: Any = None) -> str:
    sensor_config = dict(sensor_config or {})
    latest = str(latest_profile or "").strip().lower()
    if latest and latest not in {"raw", "auto"}:
        return latest

    raw_decoder = str(sensor_config.get("payload_decoder") or "auto").strip().lower()
    if raw_decoder not in {"", "auto", "default", "heuristic"}:
        if raw_decoder in {"lansen_e2_co2_v1", "lansen_m2_v1", "raw"}:
            return raw_decoder
        try:
            from TLSServer import get_custom_payload_profile
            if get_custom_payload_profile(raw_decoder):
                return raw_decoder
        except Exception:
            pass

    inferred = _infer_sensor_decoder_profile(sensor_config)
    return inferred if inferred not in {"", "auto"} else "raw"


def _build_sensor_alert_metric_options(sensor_config: Optional[Dict[str, Any]], active_tenant: str) -> list[Dict[str, Any]]:
    sensor_config = dict(sensor_config or {})
    sensor_eui = str(sensor_config.get("eui") or "").strip().upper()
    if not sensor_eui:
        return []

    metadata = _alert_metric_metadata()
    decoded = _sensor_latest_decoded_payload(sensor_eui, active_tenant)
    values = decoded.get("values") if isinstance(decoded.get("values"), dict) else {}
    latest_profile = str(decoded.get("profile") or "").strip().lower()
    profile_id = _resolve_sensor_alert_profile(sensor_config, latest_profile=latest_profile)
    known_order = _alert_profile_default_metric_keys().get(profile_id, [])
    custom_profile = None
    try:
        from TLSServer import get_custom_payload_profile
        custom_profile = get_custom_payload_profile(profile_id)
    except Exception:
        custom_profile = None

    metric_keys: list[str] = []
    if values:
        candidate_keys = [
            key
            for key, value in values.items()
            if isinstance(value, (int, float, bool)) and not isinstance(value, str)
        ]
        if known_order:
            metric_keys.extend([key for key in known_order if key in candidate_keys])
        for key in candidate_keys:
            if key not in metric_keys:
                metric_keys.append(key)
    elif known_order:
        metric_keys.extend(known_order)
    elif isinstance(custom_profile, dict):
        for field in custom_profile.get("fields") or []:
            field_type = str(field.get("type") or "uint").strip().lower()
            field_key = str(field.get("key") or "").strip()
            if field_key and field_type in {"uint", "int", "bool"}:
                metric_keys.append(field_key)

    # Apply the same sensor-type display filter as sensor_detail.html
    _DETAIL_SKIP = {
        'co2_last_calibration_ppm', 'days_to_next_calibration', 'co2_error',
        'calibration_not_done', 'temperature_2_c', 'humidity_2_pct',
        'pressure_pa', 'altitude_m',
    }
    sensor_profile_str = str(sensor_config.get('sensor_profile') or '').lower()
    profile_check = profile_id.lower() + ' ' + sensor_profile_str
    if 'm2' in profile_check or 'door' in profile_check or 'contact' in profile_check or 'open_count' in values:
        _display = {'any_alarm_active', 'open_count', 'last_alarm_duration_s', 'battery_v_est', 'tamper'}
        metric_keys = [k for k in metric_keys if k in _display]
    elif 'co2' in profile_check or 'air' in profile_check or 'co2_1_ppm' in values:
        _display = {'co2_1_ppm', 'temperature_1_c', 'humidity_1_pct', 'battery_v_est'}
        metric_keys = [k for k in metric_keys if k in _display]
    else:
        metric_keys = [k for k in metric_keys if k not in _DETAIL_SKIP]

    custom_field_map = {}
    if isinstance(custom_profile, dict):
        custom_field_map = {
            str(field.get("key") or "").strip(): field
            for field in (custom_profile.get("fields") or [])
            if str(field.get("key") or "").strip()
        }

    options: list[Dict[str, Any]] = []
    for metric_key in metric_keys:
        meta = dict(metadata.get(metric_key) or {})
        if metric_key in custom_field_map:
            field = custom_field_map[metric_key]
            meta.setdefault("label", str(field.get("label") or metric_key))
            meta.setdefault("unit", str(field.get("unit") or ""))
            field_type = str(field.get("type") or "uint").strip().lower()
            meta.setdefault("value_type", "boolean" if field_type == "bool" else "number")
        if not meta:
            value = values.get(metric_key)
            inferred_type = "boolean" if isinstance(value, bool) else "number"
            meta = {
                "label": _humanize_alert_metric_key(metric_key),
                "unit": "",
                "value_type": inferred_type,
            }
        options.append({
            "value": metric_key,
            "label": str(meta.get("label") or _humanize_alert_metric_key(metric_key)),
            "unit": str(meta.get("unit") or ""),
            "value_type": str(meta.get("value_type") or "number"),
        })

    return options


def _parse_iso_timestamp_to_unix(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return float(parsed.timestamp())
    except Exception:
        return 0.0


def _normalize_sensor_activity_status(activity_status: Any) -> str:
    raw = str(activity_status or "").strip().lower()
    if not raw:
        return "no_data"
    aliases = {
        "online": "active",
        "connected": "active",
        "recent": "active",
        "warn": "warning",
        "degraded": "warning",
        "inactive": "offline",
        "disconnected": "offline",
    }
    return aliases.get(raw, raw)


def _sensor_activity_ui_meta(activity_status: Any) -> Dict[str, Any]:
    normalized = _normalize_sensor_activity_status(activity_status)
    ui_tier = "unknown"
    incident = False
    incident_severity = None

    if normalized in {"active", "quiet"}:
        ui_tier = "online"
    elif normalized in {"warning", "stale", "auto_detach_pending"}:
        ui_tier = "warn"
        incident = True
        incident_severity = "warn"
    elif normalized in {"offline", "auto_detached"}:
        ui_tier = "offline"
        incident = True
        incident_severity = "error"

    return {
        "activity_status": normalized,
        "ui_tier": ui_tier,
        "status_incident": incident,
        "status_incident_severity": incident_severity,
    }


def _sensor_availability_snapshot(
    sensor_config: Optional[Dict[str, Any]],
    active_tenant: str,
    *,
    runtime_status: Optional[Dict[str, Any]] = None,
    packet_stats: Optional[Dict[str, Any]] = None,
    snapshot_sensor_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    sensor_config = dict(sensor_config or {})
    sensor_eui = str(sensor_config.get("eui") or "").strip().upper()
    interval_meta = _resolve_sensor_expected_interval(sensor_config, None)
    snapshot = {
        "sensor_eui": sensor_eui,
        "activity_status": "no_data",
        "last_seen_timestamp": 0.0,
        "hours_since_last_seen": 0.0,
        **interval_meta,
    }
    if not sensor_eui:
        return snapshot

    if runtime_status is None or packet_stats is None:
        global tls_server_instance
        tls_server = tls_server_instance
        if runtime_status is None and tls_server and hasattr(tls_server, "get_sensor_registration_status"):
            try:
                runtime_status = tls_server.get_sensor_registration_status() or {}
            except Exception:
                runtime_status = {}
        if packet_stats is None and tls_server and hasattr(tls_server, "sensor_packet_stats"):
            try:
                packet_stats = getattr(tls_server, "sensor_packet_stats", {}) or {}
            except Exception:
                packet_stats = {}
    runtime_status = runtime_status or {}
    packet_stats = packet_stats or {}
    snapshot_sensor_map = snapshot_sensor_map or {}

    runtime_entry = (
        runtime_status.get(sensor_eui)
        or runtime_status.get(sensor_eui.upper())
        or runtime_status.get(sensor_eui.lower())
        or {}
    )
    stats = (
        packet_stats.get(sensor_eui)
        or packet_stats.get(sensor_eui.upper())
        or packet_stats.get(sensor_eui.lower())
        or {}
    )

    observed_interval = stats.get("avg_interval_seconds") if isinstance(stats, dict) else None
    snapshot.update(_resolve_sensor_expected_interval(sensor_config, observed_interval))

    runtime_activity = str(runtime_entry.get("activity_status") or "").strip().lower()
    if runtime_activity:
        snapshot["activity_status"] = runtime_activity

    last_seen_ts = 0.0
    try:
        last_seen_ts = float(runtime_entry.get("last_seen_timestamp") or 0.0)
    except (TypeError, ValueError):
        last_seen_ts = 0.0
    if last_seen_ts <= 0:
        try:
            last_seen_ts = float(stats.get("last_seen") or 0.0)
        except (TypeError, ValueError):
            last_seen_ts = 0.0
    if last_seen_ts <= 0:
        snapshot_entry = (
            snapshot_sensor_map.get(sensor_eui)
            or snapshot_sensor_map.get(sensor_eui.upper())
            or snapshot_sensor_map.get(sensor_eui.lower())
            or {}
        )
        snapshot_payload = snapshot_entry.get("payload") if isinstance(snapshot_entry, dict) else {}
        try:
            last_seen_ts = float((snapshot_payload or {}).get("last_seen_ts") or 0.0)
        except (TypeError, ValueError):
            last_seen_ts = 0.0
        if last_seen_ts <= 0:
            try:
                last_seen_ts = _parse_iso_timestamp_to_unix((snapshot_payload or {}).get("received_at"))
            except Exception:
                last_seen_ts = 0.0
        if last_seen_ts <= 0:
            try:
                last_seen_ts = _parse_iso_timestamp_to_unix((snapshot_entry or {}).get("updated_at"))
            except Exception:
                last_seen_ts = 0.0

    snapshot["last_seen_timestamp"] = float(last_seen_ts or 0.0)
    if last_seen_ts > 0:
        now_ts = datetime.now(timezone.utc).timestamp()
        inactive_seconds = max(0.0, now_ts - last_seen_ts)
        snapshot["hours_since_last_seen"] = round(inactive_seconds / 3600.0, 2)
        if runtime_activity in {"auto_detached", "auto_detach_pending", "offline"}:
            snapshot["activity_status"] = runtime_activity
        elif snapshot.get("reporting_mode") == "event":
            if inactive_seconds > float(snapshot.get("stale_threshold_seconds") or 0):
                snapshot["activity_status"] = "stale"
            elif inactive_seconds <= 3600:
                snapshot["activity_status"] = "active"
            else:
                snapshot["activity_status"] = "quiet"
        else:
            if inactive_seconds > float(snapshot.get("offline_threshold_seconds") or 0):
                snapshot["activity_status"] = "warning"
            else:
                snapshot["activity_status"] = "active"
    elif runtime_activity:
        snapshot["activity_status"] = runtime_activity
    else:
        created_at_ts = _parse_iso_timestamp_to_unix(sensor_config.get("created_at"))
        if created_at_ts > 0:
            now_ts = datetime.now(timezone.utc).timestamp()
            age_seconds = max(0.0, now_ts - created_at_ts)
            reporting_mode = str(snapshot.get("reporting_mode") or "").strip().lower()
            if reporting_mode == "event":
                grace_seconds = min(
                    max(3600.0, float(snapshot.get("stale_threshold_seconds") or 0.0)),
                    86400.0,
                )
                if age_seconds >= grace_seconds:
                    snapshot["activity_status"] = "stale"
            else:
                grace_seconds = min(
                    max(900.0, float(snapshot.get("offline_threshold_seconds") or 0.0)),
                    3600.0,
                )
                if age_seconds >= grace_seconds:
                    snapshot["activity_status"] = "warning"

    snapshot.update(_sensor_activity_ui_meta(snapshot.get("activity_status")))
    return snapshot


def _build_sensor_availability_lookup(
    active_tenant: str,
    visible_sensors: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    runtime_status: Optional[Dict[str, Any]] = None,
    packet_stats: Optional[Dict[str, Any]] = None,
    snapshot_sensor_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    visible_sensors = visible_sensors or _build_visible_sensor_lookup(active_tenant)
    if runtime_status is None or packet_stats is None:
        global tls_server_instance
        tls_server = tls_server_instance
        if runtime_status is None:
            runtime_status = {}
            if tls_server and hasattr(tls_server, "get_sensor_registration_status"):
                try:
                    runtime_status = tls_server.get_sensor_registration_status() or {}
                except Exception:
                    runtime_status = {}
        if packet_stats is None:
            packet_stats = {}
            if tls_server and hasattr(tls_server, "sensor_packet_stats"):
                try:
                    packet_stats = getattr(tls_server, "sensor_packet_stats", {}) or {}
                except Exception:
                    packet_stats = {}
    if snapshot_sensor_map is None:
        snapshot_sensor_map = _timescale_fetch_latest_sensor_snapshots(active_tenant)
    availability_lookup: Dict[str, Dict[str, Any]] = {}
    for sensor_eui, sensor_config in visible_sensors.items():
        availability_lookup[sensor_eui] = _sensor_availability_snapshot(
            sensor_config,
            active_tenant,
            runtime_status=runtime_status,
            packet_stats=packet_stats,
            snapshot_sensor_map=snapshot_sensor_map,
        )
    return availability_lookup


def _sensor_is_unavailable(activity_status: Any) -> bool:
    meta = _sensor_activity_ui_meta(activity_status)
    return bool(meta.get("status_incident"))


def _evaluate_triggered_alerts(
    active_tenant: str,
    visible_sensors: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    availability_by_eui: Optional[Dict[str, Dict[str, Any]]] = None,
    latest_values_by_eui: Optional[Dict[str, Dict[str, Any]]] = None,
    persist_state: bool = True,
) -> list[Dict[str, Any]]:
    visible_sensors = visible_sensors or _build_visible_sensor_lookup(active_tenant)
    alerts = [
        {
            **a,
            "kind": _normalize_alert_kind(a.get("kind")),
            "enabled": bool(a.get("enabled", True)),
        }
        for a in _load_alerts()
        if a.get('enabled', True) and str(a.get('sensor_eui', '')).strip().upper() in visible_sensors
    ]
    if not alerts:
        return []

    from collections import defaultdict

    by_sensor = defaultdict(list)
    for alert in alerts:
        by_sensor[str(alert.get('sensor_eui', '')).upper()].append(alert)

    triggered = []
    for eui, sensor_alerts in by_sensor.items():
        if not eui:
            continue
        sensor_config = visible_sensors.get(eui)
        values = dict((latest_values_by_eui or {}).get(eui) or {})
        if not values:
            values = _sensor_latest_values(eui, active_tenant)
        availability = None
        for alert in sensor_alerts:
            if alert.get('kind') == 'sensor_offline':
                if availability is None:
                    availability = dict((availability_by_eui or {}).get(eui) or {})
                    if not availability:
                        availability = _sensor_availability_snapshot(sensor_config, active_tenant)
                if _sensor_is_unavailable(availability.get('activity_status')):
                    triggered.append({
                        **alert,
                        "current_status": availability.get("activity_status"),
                        "hours_since_last_seen": availability.get("hours_since_last_seen"),
                    })
                continue

            metric = alert.get('metric', '')
            if metric not in values:
                continue
            raw_val = values[metric]
            try:
                num_val = float(raw_val)
            except (TypeError, ValueError):
                continue
            if _eval_alert(alert.get('condition', 'gt'), num_val, alert.get('threshold', 0)):
                triggered.append({
                    **alert,
                    "current_value": num_val,
                })

    if persist_state:
        _persist_alert_runtime_state(alerts, triggered)
    now_iso = datetime.now(timezone.utc).isoformat()
    for item in triggered:
        if not item.get("triggered_at"):
            item["triggered_at"] = now_iso
    triggered.sort(key=lambda a: 0 if a.get('severity') == 'critical' else 1)
    return triggered


def _format_alert_metric_value(metric: Any, value: Any) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    unit = str((_alert_metric_metadata().get(str(metric or "").strip()) or {}).get("unit") or "")
    if unit == "°C":
        return f"{num:.1f}{unit}"
    if num.is_integer() or abs(num) >= 10:
        return f"{int(round(num))}{unit}"
    return f"{num:.2f}{unit}"


def _alert_metric_label(metric: Any) -> str:
    metric_key = str(metric or "").strip()
    if not metric_key:
        return "Metrika"
    meta = _alert_metric_metadata().get(metric_key) or {}
    return str(meta.get("label") or _humanize_alert_metric_key(metric_key))


def _format_threshold_value(metric: Any, value: Any) -> str:
    return _format_alert_metric_value(metric, value)


def _estimate_sensor_incident_started_at(availability: Dict[str, Any]) -> Optional[str]:
    try:
        last_seen_ts = float(availability.get("last_seen_timestamp") or 0.0)
    except (TypeError, ValueError):
        last_seen_ts = 0.0
    if last_seen_ts <= 0:
        return None

    status = _normalize_sensor_activity_status(availability.get("activity_status"))
    if status == "warning":
        offset_seconds = float(availability.get("offline_threshold_seconds") or 0.0)
    elif status == "stale":
        offset_seconds = float(availability.get("stale_threshold_seconds") or 0.0)
    elif status in {"offline", "auto_detached"}:
        offset_seconds = float(availability.get("offline_threshold_seconds") or 0.0)
    elif status == "auto_detach_pending":
        offset_seconds = float(getattr(bssci_config, "AUTO_DETACH_TIMEOUT", 259200) or 259200)
    else:
        return None

    if offset_seconds <= 0:
        return None
    started_at = datetime.fromtimestamp(last_seen_ts + offset_seconds, tz=timezone.utc)
    now_dt = datetime.now(timezone.utc)
    if started_at > now_dt:
        started_at = now_dt
    return started_at.isoformat()


def _generic_sensor_availability_reason(activity_status: Any) -> str:
    return "Senzor neposlal dáta v očakávanom čase."


def _build_current_incidents(active_tenant: str) -> list[Dict[str, Any]]:
    visible_sensors = _build_visible_sensor_lookup(active_tenant)
    availability_by_eui = _build_sensor_availability_lookup(active_tenant, visible_sensors=visible_sensors)
    latest_values_by_eui = _timescale_fetch_latest_sensor_decoded_values_map(
        active_tenant,
        sensor_euis=list(visible_sensors.keys()),
    )
    enabled_offline_rule_sensors = {
        str(alert.get("sensor_eui") or "").strip().upper()
        for alert in _load_alerts()
        if bool(alert.get("enabled", True)) and _normalize_alert_kind(alert.get("kind")) == "sensor_offline"
    }

    incidents: list[Dict[str, Any]] = []
    activity_known_records: list[Dict[str, Any]] = []
    activity_active_records: list[Dict[str, Any]] = []

    for sensor_eui, sensor_config in visible_sensors.items():
        availability = dict(availability_by_eui.get(sensor_eui) or {})
        sensor_name = str(sensor_config.get("name") or sensor_eui)
        status_incident = bool(availability.get("status_incident"))
        severity = "error" if availability.get("status_incident_severity") == "error" else "warn"
        raw_status = str(availability.get("activity_status") or "").strip().lower()
        reason = _generic_sensor_availability_reason(raw_status)
        hours_since_last_seen = availability.get("hours_since_last_seen")
        hours_str = ""
        try:
            if hours_since_last_seen not in (None, ""):
                hours_str = f"{float(hours_since_last_seen):.1f} h"
        except (TypeError, ValueError):
            hours_str = ""
        activity_record = {
            "id": f"{sensor_eui}:activity",
            "tenant_id": active_tenant,
            "sensor_eui": sensor_eui,
            "sensor_name": sensor_name,
            "name": sensor_name,
            "severity": "critical" if severity == "error" else "warning",
            "source": "activity",
            "kind": "activity",
            "reason": reason,
            "desc": reason,
            "current_status": raw_status,
            "hours_since_last_seen": hours_since_last_seen,
            "triggered_at": _estimate_sensor_incident_started_at(availability),
            "val_str": hours_str,
            "link_url": f"/sensors/{urllib.parse.quote(sensor_eui)}",
        }
        activity_known_records.append(activity_record)
        if not status_incident or sensor_eui in enabled_offline_rule_sensors:
            continue
        incidents.append({
            "id": activity_record["id"],
            "tier": severity,
            "level": "danger" if severity == "error" else "warn",
            "severity": activity_record["severity"],
            "name": sensor_name,
            "text": reason,
            "desc": reason,
            "reason": reason,
            "eui": sensor_eui,
            "source": "activity",
            "kind": "activity",
            "ackKey": f"{sensor_eui}:activity",
            "triggeredAt": activity_record["triggered_at"],
            "ruleId": None,
            "valStr": hours_str,
            "linkUrl": activity_record["link_url"],
            "current_status": raw_status,
            "hours_since_last_seen": hours_since_last_seen,
        })
        activity_active_records.append(activity_record)

    if activity_known_records:
        _persist_runtime_history_state(activity_known_records, activity_active_records)

    for alert in _evaluate_triggered_alerts(
        active_tenant,
        visible_sensors=visible_sensors,
        availability_by_eui=availability_by_eui,
        latest_values_by_eui=latest_values_by_eui,
        persist_state=True,
    ):
        kind = _normalize_alert_kind(alert.get("kind"))
        sensor_eui = str(alert.get("sensor_eui") or "").strip().upper()
        sensor_name = str((visible_sensors.get(sensor_eui) or {}).get("name") or sensor_eui or "—")
        if kind == "sensor_offline":
            reason = _generic_sensor_availability_reason(alert.get("current_status"))
            desc = reason
            val_str = (
                f"{float(alert.get('hours_since_last_seen')):.1f} h"
                if alert.get("hours_since_last_seen") not in (None, "")
                else ""
            )
            source = "offline_rule"
        else:
            metric_label = _alert_metric_label(alert.get("metric"))
            current_value_str = _format_threshold_value(alert.get("metric"), alert.get("current_value"))
            threshold_str = _format_threshold_value(alert.get("metric"), alert.get("threshold"))
            condition_label = {
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "eq": "=",
            }.get(str(alert.get("condition") or "").strip().lower(), str(alert.get("condition") or "").strip())
            reason = f"{metric_label}: {current_value_str} {condition_label} {threshold_str}".strip()
            desc = reason
            val_str = current_value_str
            source = "threshold"
        severity = "error" if alert.get("severity") == "critical" else "warn"
        incidents.append({
            "id": str(alert.get("id") or f"{sensor_eui}:{kind}"),
            "tier": severity,
            "level": "danger" if severity == "error" else "warn",
            "severity": str(alert.get("severity") or "warning"),
            "name": sensor_name,
            "text": desc,
            "desc": desc,
            "reason": reason,
            "eui": sensor_eui,
            "source": source,
            "kind": kind,
            "ackKey": f"{str(alert.get('id') or sensor_eui)}:{kind}",
            "triggeredAt": alert.get("triggered_at"),
            "ruleId": alert.get("id"),
            "valStr": val_str,
            "linkUrl": f"/sensors/{urllib.parse.quote(sensor_eui)}" if sensor_eui else "/alerts",
        })

    incidents.sort(key=lambda item: (0 if item.get("tier") == "error" else 1, str(item.get("name") or ""), str(item.get("id") or "")))
    return incidents

# ── End alert helpers ────────────────────────────────────────────────────────

def _sensor_shared_with_tenant(sensor, active_tenant):
    """Return True if active_tenant is in the sensor's shared_tenants list."""
    if _is_global_tenant_scope(active_tenant):
        return False  # global scope already matched by _tenant_matches
    shared = sensor.get("shared_tenants") or []
    if not isinstance(shared, list):
        return False
    active_norm = _normalize_tenant_id(active_tenant, fallback=_default_tenant_id())
    return any(
        _normalize_tenant_id(t, fallback=_default_tenant_id()) == active_norm
        for t in shared
    )

def _resolve_query_tenant_for_sensor(sensor_eui, requesting_tenant):
    """For shared sensors return the owner's tenant_id so DB queries hit the right partition."""
    if _is_global_tenant_scope(requesting_tenant):
        return requesting_tenant
    eui_up = str(sensor_eui or "").strip().upper()
    if not eui_up:
        return requesting_tenant
    for s in _load_all_sensors():
        if str(s.get("eui", "")).upper() == eui_up:
            owner = _tenant_id_from_sensor(s)
            if _tenant_matches(owner, requesting_tenant):
                return requesting_tenant
            if _sensor_shared_with_tenant(s, requesting_tenant):
                return owner
            break
    return requesting_tenant

def _filter_sensors_for_tenant(sensors, tenant_id=None):
    active_tenant = _active_tenant_id() if tenant_id is None else tenant_id
    filtered = []
    for sensor in (sensors or []):
        if not isinstance(sensor, dict):
            continue
        if _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
            payload = dict(sensor)
            payload["tenant_id"] = _tenant_id_from_sensor(sensor)
            filtered.append(payload)
        elif _sensor_shared_with_tenant(sensor, active_tenant):
            payload = dict(sensor)
            payload["tenant_id"] = _tenant_id_from_sensor(sensor)
            payload["is_shared"] = True
            filtered.append(payload)
    return filtered

def _filter_base_stations_for_tenant(base_stations, tenant_id=None):
    active_tenant = _active_tenant_id() if tenant_id is None else tenant_id
    result = {}
    for eui, bs_data in (base_stations or {}).items():
        if not isinstance(bs_data, dict):
            continue
        if _tenant_matches(_tenant_id_from_base_station(bs_data), active_tenant):
            payload = dict(bs_data)
            payload["tenant_id"] = _tenant_id_from_base_station(bs_data)
            result[eui] = payload
    return result

def _reassign_base_stations_for_tenant(tenant_id, selected_base_station_euis):
    tenant_key = _normalize_tenant_id(tenant_id, fallback=_default_tenant_id())
    default_tenant = _default_tenant_id()
    selected = {str(eui).strip().lower() for eui in _normalize_base_station_route_list(selected_base_station_euis)}
    config = load_base_station_config()
    base_stations = config.setdefault("base_stations", {})
    assigned = []
    released = []
    moved_in = []
    changed = False

    for raw_eui, raw_bs in list((base_stations or {}).items()):
        if not isinstance(raw_bs, dict):
            continue
        eui_lower = str(raw_eui or "").strip().lower()
        if not eui_lower:
            continue
        current_tenant = _tenant_id_from_base_station(raw_bs)
        if eui_lower in selected:
            assigned.append(eui_lower.upper())
            if not _tenant_matches(current_tenant, tenant_key):
                raw_bs["tenant_id"] = tenant_key
                moved_in.append(eui_lower.upper())
                changed = True
        elif tenant_key != default_tenant and _tenant_matches(current_tenant, tenant_key):
            raw_bs["tenant_id"] = default_tenant
            released.append(eui_lower.upper())
            changed = True

    if changed:
        save_base_station_config(config)

    return {
        "assigned": sorted(assigned),
        "moved_in": sorted(moved_in),
        "released": sorted(released),
        "changed": changed,
    }

def _resolve_sensor_tenant(sensor_eui):
    sensor_key = str(sensor_eui or "").strip().upper()
    if not sensor_key:
        return _default_tenant_id()
    for sensor in _load_all_sensors():
        if str((sensor or {}).get("eui", "")).strip().upper() == sensor_key:
            return _tenant_id_from_sensor(sensor)
    return _default_tenant_id()

def _normalize_optional_coordinate(value, label):
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if raw == "":
            return None
        value = raw
    try:
        num = float(value)
    except Exception:
        raise ValueError(f"Invalid {label} value")
    if not math.isfinite(num):
        raise ValueError(f"Invalid {label} value")
    return num

def _normalize_gps_coordinates(lat_value, lng_value):
    lat = _normalize_optional_coordinate(lat_value, "latitude")
    lng = _normalize_optional_coordinate(lng_value, "longitude")
    if lat is None and lng is None:
        return None, None
    if lat is None or lng is None:
        raise ValueError("Both latitude and longitude are required")
    if lat < -90 or lat > 90:
        raise ValueError("Latitude must be in range -90..90")
    if lng < -180 or lng > 180:
        raise ValueError("Longitude must be in range -180..180")
    return round(lat, 6), round(lng, 6)

def _coverage_positions_file():
    return "coverage_positions.json"

def _normalize_coverage_position_record(raw_key, raw_position):
    """Normalize one coverage position record to canonical key/payload."""
    if not isinstance(raw_position, dict):
        return None, None

    key_raw = str(raw_key or "").strip()
    key_upper = key_raw.upper()

    key_device_type = ""
    key_eui = ""
    if key_upper.startswith("BS_"):
        key_device_type = "bs"
        key_eui = key_upper.split("_", 1)[1] if "_" in key_upper else ""
    elif key_upper.startswith("SENSOR_"):
        key_device_type = "sensor"
        key_eui = key_upper.split("_", 1)[1] if "_" in key_upper else ""

    raw_device_type = str(raw_position.get("deviceType", "") or "").strip().lower()
    if raw_device_type in {"bs", "base_station"}:
        device_type = "bs"
    elif raw_device_type == "sensor":
        device_type = "sensor"
    else:
        device_type = key_device_type or "sensor"

    eui = _normalize_eui_upper(raw_position.get("eui") or key_eui)
    if not eui:
        return None, None

    raw_pos_type = str(raw_position.get("type", "") or "").strip().lower()
    pos_type = raw_pos_type if raw_pos_type in {"osm", "floorplan"} else "osm"

    normalized = {
        "type": pos_type,
        "deviceType": device_type,
        "eui": eui,
    }

    if pos_type == "osm":
        try:
            lat, lng = _normalize_gps_coordinates(raw_position.get("lat"), raw_position.get("lng"))
        except ValueError:
            return None, None
        if lat is None or lng is None:
            return None, None
        normalized["lat"] = float(lat)
        normalized["lng"] = float(lng)
    else:
        try:
            x = float(raw_position.get("x"))
            y = float(raw_position.get("y"))
        except (TypeError, ValueError):
            return None, None
        normalized["x"] = x
        normalized["y"] = y

    canonical_key = f"{device_type}_{eui}"
    return canonical_key, normalized

def _merge_coverage_position_payload(existing, candidate):
    """Prefer canonical OSM payload when duplicates collide by canonical key."""
    if not isinstance(existing, dict):
        return candidate
    if not isinstance(candidate, dict):
        return existing
    existing_type = str(existing.get("type", "")).lower()
    candidate_type = str(candidate.get("type", "")).lower()
    if existing_type == candidate_type:
        return candidate
    if candidate_type == "osm":
        return candidate
    if existing_type == "osm":
        return existing
    return candidate

def _canonicalize_coverage_positions_state(state):
    if not isinstance(state, dict):
        return {"positions": {}}, True

    raw_positions = state.get("positions", {})
    if not isinstance(raw_positions, dict):
        state["positions"] = {}
        return state, True

    normalized_positions = {}
    changed = False

    for raw_key, raw_position in raw_positions.items():
        canonical_key, normalized_payload = _normalize_coverage_position_record(raw_key, raw_position)
        if not canonical_key:
            changed = True
            continue
        existing = normalized_positions.get(canonical_key)
        merged = _merge_coverage_position_payload(existing, normalized_payload)
        normalized_positions[canonical_key] = merged
        if canonical_key != str(raw_key) or merged != raw_position:
            changed = True

    if normalized_positions != raw_positions:
        changed = True
    state["positions"] = normalized_positions
    return state, changed

def _load_coverage_positions_state():
    positions_file = _coverage_positions_file()
    try:
        if os.path.exists(positions_file):
            with open(positions_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("positions", {})
                    data, changed = _canonicalize_coverage_positions_state(data)
                    if changed:
                        _save_coverage_positions_state(data)
                    return data
    except Exception:
        pass
    return {"positions": {}}

def _save_coverage_positions_state(data):
    positions_file = _coverage_positions_file()
    with open(positions_file, "w") as f:
        json.dump(data, f, indent=2)

def _coverage_position_key(device_type, eui):
    eui_upper = str(eui or "").strip().upper()
    prefix = "bs" if str(device_type or "").lower() == "bs" else "sensor"
    return f"{prefix}_{eui_upper}", eui_upper

def _upsert_device_gps_position(device_type, eui, gps_lat, gps_lng):
    key, eui_upper = _coverage_position_key(device_type, eui)
    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})

    if gps_lat is None or gps_lng is None:
        existing = positions.get(key)
        if isinstance(existing, dict) and existing.get("type") == "osm":
            positions.pop(key, None)
    else:
        positions[key] = {
            "type": "osm",
            "lat": float(gps_lat),
            "lng": float(gps_lng),
            "deviceType": "bs" if str(device_type).lower() == "bs" else "sensor",
            "eui": eui_upper
        }

    _save_coverage_positions_state(state)

def _remove_device_position(device_type, eui):
    key, _ = _coverage_position_key(device_type, eui)
    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})
    if key in positions:
        positions.pop(key, None)
        _save_coverage_positions_state(state)

def _sync_inventory_gps_to_coverage_positions():
    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})
    if not isinstance(positions, dict):
        positions = {}
        state["positions"] = positions

    desired = {}

    # Base stations
    bs_config = load_base_station_config().get("base_stations", {}) or {}
    for bs_eui, bs_data in bs_config.items():
        try:
            gps_lat, gps_lng = _normalize_gps_coordinates((bs_data or {}).get("gps_lat"), (bs_data or {}).get("gps_lng"))
        except ValueError:
            gps_lat, gps_lng = None, None
        key, eui_upper = _coverage_position_key("bs", bs_eui)
        if gps_lat is not None and gps_lng is not None:
            desired[key] = {
                "type": "osm",
                "lat": gps_lat,
                "lng": gps_lng,
                "deviceType": "bs",
                "eui": eui_upper
            }

    # Sensors
    try:
        sensors = _load_all_sensors()
    except Exception:
        sensors = []

    for sensor in sensors:
        sensor_eui = str((sensor or {}).get("eui", "")).strip()
        if not sensor_eui:
            continue
        try:
            gps_lat, gps_lng = _normalize_gps_coordinates((sensor or {}).get("gps_lat"), (sensor or {}).get("gps_lng"))
        except ValueError:
            gps_lat, gps_lng = None, None
        key, eui_upper = _coverage_position_key("sensor", sensor_eui)
        if gps_lat is not None and gps_lng is not None:
            desired[key] = {
                "type": "osm",
                "lat": gps_lat,
                "lng": gps_lng,
                "deviceType": "sensor",
                "eui": eui_upper
            }

    changed = False

    # Upsert desired osm positions
    for key, payload in desired.items():
        existing = positions.get(key)
        if not isinstance(existing, dict) or existing.get("type") == "osm":
            if existing != payload:
                positions[key] = payload
                changed = True

    if changed:
        _save_coverage_positions_state(state)
    return state

def _sync_coverage_positions_to_inventory(tenant_id=None, only_missing=True, state=None, allowed_keys=None):
    """
    Backfill inventory GPS from saved coverage map positions.
    - Only uses OSM positions (lat/lng).
    - `only_missing=True` updates only devices without GPS in inventory.
    """
    active_tenant = _active_tenant_id() if tenant_id is None else tenant_id
    payload = state if isinstance(state, dict) else _load_coverage_positions_state()
    positions = payload.get("positions", {}) if isinstance(payload, dict) else {}
    if not isinstance(positions, dict):
        return {"sensor": 0, "bs": 0, "total": 0}

    if allowed_keys is None:
        tenant_sensor_keys = {
            f"sensor_{str(sensor.get('eui', '')).strip().upper()}"
            for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
        }
        tenant_bs_keys = {
            f"bs_{str(eui).strip().upper()}"
            for eui in _filter_base_stations_for_tenant(
                load_base_station_config().get("base_stations", {}),
                tenant_id=active_tenant,
            ).keys()
        }
        allowed_keys = tenant_sensor_keys | tenant_bs_keys
    else:
        allowed_keys = set(allowed_keys)

    # Snapshot current inventory GPS (tenant-scoped)
    sensor_has_gps = set()
    for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant):
        sensor_eui = _normalize_eui_upper((sensor or {}).get("eui", ""))
        if not sensor_eui:
            continue
        try:
            lat, lng = _normalize_gps_coordinates((sensor or {}).get("gps_lat"), (sensor or {}).get("gps_lng"))
            if lat is not None and lng is not None:
                sensor_has_gps.add(sensor_eui)
        except ValueError:
            continue

    bs_has_gps = set()
    for bs_eui, bs_data in _filter_base_stations_for_tenant(
        load_base_station_config().get("base_stations", {}),
        tenant_id=active_tenant,
    ).items():
        bs_eui_norm = _normalize_eui_upper(bs_eui)
        if not bs_eui_norm:
            continue
        try:
            lat, lng = _normalize_gps_coordinates((bs_data or {}).get("gps_lat"), (bs_data or {}).get("gps_lng"))
            if lat is not None and lng is not None:
                bs_has_gps.add(bs_eui_norm)
        except ValueError:
            continue

    updated_sensor = 0
    updated_bs = 0

    for key, pos in positions.items():
        canonical_key, normalized_pos = _normalize_coverage_position_record(key, pos)
        if not canonical_key or not isinstance(normalized_pos, dict):
            continue
        if canonical_key not in allowed_keys:
            continue
        if str(normalized_pos.get("type", "")).strip().lower() != "osm":
            continue

        try:
            lat, lng = _normalize_gps_coordinates(normalized_pos.get("lat"), normalized_pos.get("lng"))
        except ValueError:
            continue

        eui = _normalize_eui_upper(normalized_pos.get("eui"))
        if not eui:
            continue

        is_bs = str(normalized_pos.get("deviceType", "")).strip().lower() in {"bs", "base_station"}
        if is_bs:
            if only_missing and eui in bs_has_gps:
                continue
            try:
                _update_base_station_gps_by_eui(eui, lat, lng)
                bs_has_gps.add(eui)
                updated_bs += 1
            except Exception:
                continue
        else:
            if only_missing and eui in sensor_has_gps:
                continue
            try:
                _update_sensor_gps_by_eui(eui, lat, lng)
                sensor_has_gps.add(eui)
                updated_sensor += 1
            except Exception:
                continue

    return {
        "sensor": updated_sensor,
        "bs": updated_bs,
        "total": updated_sensor + updated_bs,
    }

def _update_sensor_gps_by_eui(eui: str, gps_lat: float, gps_lng: float) -> None:
    sensor_eui = _normalize_eui_upper(eui)
    if not sensor_eui:
        raise ValueError("Sensor EUI is required")

    sensors = _load_all_sensors()

    if not isinstance(sensors, list):
        raise ValueError("Sensor configuration is invalid")

    active_tenant = _active_tenant_id()
    found = False
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        if (
            _normalize_eui_upper(sensor.get("eui", "")) == sensor_eui
            and _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant)
        ):
            sensor["gps_lat"] = gps_lat
            sensor["gps_lng"] = gps_lng
            found = True
            break

    if not found:
        raise ValueError(f"Sensor {sensor_eui} not found")

    _save_all_sensors(sensors)

def _update_base_station_gps_by_eui(eui: str, gps_lat: float, gps_lng: float) -> None:
    bs_eui = _normalize_eui_upper(eui)
    if not bs_eui:
        raise ValueError("Base station EUI is required")

    config = load_base_station_config()
    base_stations = config.setdefault("base_stations", {})
    if not isinstance(base_stations, dict):
        raise ValueError("Base station configuration is invalid")

    target_key = None
    for key in base_stations.keys():
        if _normalize_eui_upper(key) == bs_eui:
            if not _tenant_matches(_tenant_id_from_base_station(base_stations.get(key, {})), _active_tenant_id()):
                continue
            target_key = key
            break

    if target_key is None:
        raise ValueError(f"Base station {bs_eui} not found")

    bs_data = dict(base_stations.get(target_key, {}) or {})
    before_snapshot = _base_station_audit_snapshot(target_key, bs_data)
    bs_data["gps_lat"] = gps_lat
    bs_data["gps_lng"] = gps_lng
    base_stations[target_key] = bs_data
    save_base_station_config(config)
    after_snapshot = _base_station_audit_snapshot(target_key, bs_data)
    _record_admin_audit(
        action='base_station.update_gps',
        entity='base_station',
        target_id=bs_eui,
        status='success',
        details={
            'tenant_id': _tenant_id_from_base_station(bs_data),
            'before': before_snapshot,
            'after': after_snapshot,
            'changed_fields': _audit_changed_fields(before_snapshot, after_snapshot),
        },
    )

def _normalize_sensor_payload(data):
    def _normalize_payload_decoder(value):
        raw = str(value or "").strip().lower()
        if raw in {"", "auto", "default", "heuristic"}:
            return "auto"
        if raw in {"lansen_e2_co2_v1", "lansen_co2", "lansen-e2-co2"}:
            return "lansen_e2_co2_v1"
        if raw in {"lansen_m2_v1", "lansen_m2", "lan-mioty-m2", "m2"}:
            return "lansen_m2_v1"
        if raw in {"raw", "none"}:
            return "raw"
        return "auto"

    def _normalize_environment_context(value):
        raw = str(value or "").strip().lower()
        if raw in {"indoor", "inside", "room"}:
            return "indoor"
        if raw in {"outdoor", "outside", "ambient"}:
            return "outdoor"
        return "auto"

    payload = dict(data or {})
    payload["eui"] = str(payload.get("eui", "")).strip().upper()
    payload["nwKey"] = str(payload.get("nwKey", "")).strip().upper()
    payload["shortAddr"] = str(payload.get("shortAddr", "0000")).strip().upper() or "0000"
    payload["bidi"] = bool(payload.get("bidi", False))
    payload["name"] = str(payload.get("name", "") or "").strip()
    payload["tags"] = _normalize_sensor_tags(payload.get("tags", []))
    payload["sensor_profile"] = _normalize_sensor_profile(payload.get("sensor_profile"))
    payload["payload_decoder"] = _normalize_payload_decoder(payload.get("payload_decoder"))
    payload["environment_context"] = _normalize_environment_context(payload.get("environment_context"))
    payload["reporting_mode"] = _normalize_reporting_mode(payload.get("reporting_mode"))
    payload["expected_interval_seconds"] = _normalize_expected_interval_seconds(payload.get("expected_interval_seconds"))
    payload["stale_after_hours"] = _normalize_stale_after_hours(payload.get("stale_after_hours"))
    payload = _apply_sensor_profile_defaults(payload)
    gps_lat, gps_lng = _normalize_gps_coordinates(payload.get("gps_lat"), payload.get("gps_lng"))
    payload["gps_lat"] = gps_lat
    payload["gps_lng"] = gps_lng
    created_at = str(payload.get("created_at") or "").strip()
    if created_at:
        payload["created_at"] = created_at
    else:
        payload.pop("created_at", None)
    payload["tenant_id"] = _normalize_tenant_id(payload.get("tenant_id"), fallback=_active_tenant_id())
    # Preserve shared_tenants as-is (managed via /api/sensors/<eui>/share)
    raw_shared = payload.get("shared_tenants")
    if isinstance(raw_shared, list):
        payload["shared_tenants"] = [
            _normalize_tenant_id(t, fallback=_default_tenant_id())
            for t in raw_shared if str(t or "").strip()
        ]
    else:
        payload.pop("shared_tenants", None)
    return payload

def _ensure_ca_exists():
    if os.path.exists('certs/ca_cert.pem') and os.path.exists('certs/ca_key.pem'):
        return True
    os.makedirs('certs', exist_ok=True)
    result = subprocess.run(['openssl', 'genrsa', '-out', 'certs/ca_key.pem', '2048'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return False
    result = subprocess.run(['openssl', 'req', '-new', '-x509', '-key', 'certs/ca_key.pem',
                            '-out', 'certs/ca_cert.pem', '-days', '365',
                            '-subj', '/C=US/ST=State/L=City/O=BSSCI/CN=BSSCI-CA'],
                           capture_output=True, text=True, timeout=30)
    return result.returncode == 0

def _generate_bs_certificate(eui, audit_context: str = "system"):
    eui = eui.lower()
    config = load_base_station_config()
    bs_data = dict(config.get("base_stations", {}).get(eui, {}) or {})
    tenant_id = _tenant_id_from_base_station(bs_data)
    before_snapshot = _base_station_audit_snapshot(eui, bs_data) if bs_data else None
    if not _validate_eui(eui):
        message = "Invalid EUI format"
        _record_admin_audit(
            action='base_station.generate_certificate',
            entity='base_station',
            target_id=eui,
            status='error',
            details={
                'tenant_id': tenant_id,
                'message': message,
                'audit_context': audit_context,
                'before': before_snapshot,
            },
        )
        return False, message
    if not _ensure_ca_exists():
        message = "Failed to ensure CA exists"
        _record_admin_audit(
            action='base_station.generate_certificate',
            entity='base_station',
            target_id=eui,
            status='error',
            details={
                'tenant_id': tenant_id,
                'message': message,
                'audit_context': audit_context,
                'before': before_snapshot,
            },
        )
        return False, message
    bs_cert_dir = os.path.join('certs', f'bs_{eui}')
    os.makedirs(bs_cert_dir, exist_ok=True)
    key_path = os.path.join(bs_cert_dir, f'{eui}_key.pem')
    csr_path = os.path.join(bs_cert_dir, f'{eui}.csr')
    cert_path = os.path.join(bs_cert_dir, f'{eui}_cert.pem')
    result = subprocess.run(['openssl', 'genrsa', '-out', key_path, '2048'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        message = f"Key generation failed: {result.stderr}"
        _record_admin_audit(
            action='base_station.generate_certificate',
            entity='base_station',
            target_id=eui,
            status='error',
            details={
                'tenant_id': tenant_id,
                'message': message,
                'audit_context': audit_context,
                'before': before_snapshot,
            },
        )
        return False, message
    result = subprocess.run(['openssl', 'req', '-new', '-key', key_path, '-out', csr_path,
                            '-subj', f'/C=US/ST=State/L=City/O=BSSCI/CN={eui}'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        message = f"CSR generation failed: {result.stderr}"
        _record_admin_audit(
            action='base_station.generate_certificate',
            entity='base_station',
            target_id=eui,
            status='error',
            details={
                'tenant_id': tenant_id,
                'message': message,
                'audit_context': audit_context,
                'before': before_snapshot,
            },
        )
        return False, message
    result = subprocess.run(['openssl', 'x509', '-req', '-in', csr_path,
                            '-CA', 'certs/ca_cert.pem', '-CAkey', 'certs/ca_key.pem',
                            '-CAcreateserial', '-out', cert_path, '-days', '365'],
                           capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        message = f"Certificate signing failed: {result.stderr}"
        _record_admin_audit(
            action='base_station.generate_certificate',
            entity='base_station',
            target_id=eui,
            status='error',
            details={
                'tenant_id': tenant_id,
                'message': message,
                'audit_context': audit_context,
                'before': before_snapshot,
            },
        )
        return False, message
    if os.path.exists(csr_path):
        os.remove(csr_path)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=365)
    if eui in config.get("base_stations", {}):
        config["base_stations"][eui]["cert_generated"] = now.strftime('%Y-%m-%dT%H:%M:%S')
        config["base_stations"][eui]["cert_expires"] = expires.strftime('%Y-%m-%dT%H:%M:%S')
        save_base_station_config(config)
    after_data = dict(config.get("base_stations", {}).get(eui, {}) or {})
    after_snapshot = _base_station_audit_snapshot(eui, after_data) if after_data else None
    _record_admin_audit(
        action='base_station.generate_certificate',
        entity='base_station',
        target_id=eui,
        status='success',
        details={
            'tenant_id': _tenant_id_from_base_station(after_data) or tenant_id,
            'audit_context': audit_context,
            'before': before_snapshot,
            'after': after_snapshot,
            'changed_fields': _audit_changed_fields(before_snapshot or {}, after_snapshot or {}),
            'cert_generated': after_data.get('cert_generated'),
            'cert_expires': after_data.get('cert_expires'),
        },
    )
    return True, "Certificate generated successfully"

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bssci-service-secret-key-change-me')
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = bool(getattr(bssci_config, "SESSION_COOKIE_SECURE", False))
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
    minutes=max(5, int(getattr(bssci_config, "AUTH_SESSION_TIMEOUT_MINUTES", 30) or 30))
)

# Configure logger for this module
logger = logging.getLogger(__name__)

_auth_security_lock = threading.Lock()
_login_rate_limit_by_ip = {}
_api_rate_limit_by_actor = {}
_session_timeout_seconds = max(
    300.0, float(getattr(bssci_config, "AUTH_SESSION_TIMEOUT_MINUTES", 30) or 30) * 60.0
)
_login_rate_limit_attempts = max(1, int(getattr(bssci_config, "AUTH_LOGIN_RATE_LIMIT_ATTEMPTS", 8) or 8))
_login_rate_limit_window_seconds = max(
    30.0, float(getattr(bssci_config, "AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS", 300) or 300)
)
_login_rate_limit_lockout_seconds = max(
    _login_rate_limit_window_seconds,
    float(getattr(bssci_config, "AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS", 900) or 900),
)
_api_rate_limit_requests = max(50, int(getattr(bssci_config, "AUTH_API_RATE_LIMIT_REQUESTS", 300) or 300))
_api_rate_limit_window_seconds = max(
    5.0, float(getattr(bssci_config, "AUTH_API_RATE_LIMIT_WINDOW_SECONDS", 60) or 60)
)

ADMIN_SCOPE_DEFINITIONS = {
    "manage_users": {
        "label": "Manage users",
        "description": "Create, update and delete user accounts.",
        "default": True,
    },
    "manage_tenants": {
        "label": "Manage tenants",
        "description": "Create/update/delete tenants and tenant import/export.",
        "default": True,
    },
    "manage_configuration": {
        "label": "Manage configuration",
        "description": "Read/write service configuration and operational settings.",
        "default": True,
    },
    "manage_system": {
        "label": "Manage system operations",
        "description": "Restart service/container and reset operational counters.",
        "default": True,
    },
    "manage_certificates": {
        "label": "Manage certificates",
        "description": "Issue, upload, restore and download certificate assets.",
        "default": True,
    },
    "view_admin_audit": {
        "label": "View admin audit",
        "description": "Read admin audit trail entries.",
        "default": True,
    },
    "export_admin_audit": {
        "label": "Export admin audit",
        "description": "Export admin audit entries (JSON/CSV).",
        "default": True,
    },
    "clear_admin_audit": {
        "label": "Clear admin audit",
        "description": "Clear persisted admin audit history.",
        "default": True,
    },
    "clear_service_logs": {
        "label": "Clear service logs",
        "description": "Clear in-memory service log stream.",
        "default": True,
    },
    "view_service_logs": {
        "label": "View service logs",
        "description": "Read global in-memory service log stream.",
        "default": True,
    },
}


def _default_admin_permissions():
    return {scope: bool(meta.get("default", True)) for scope, meta in ADMIN_SCOPE_DEFINITIONS.items()}


def _normalize_admin_permissions(raw_permissions):
    defaults = _default_admin_permissions()
    if raw_permissions is None:
        return defaults
    if isinstance(raw_permissions, dict):
        normalized = dict(defaults)
        for scope in ADMIN_SCOPE_DEFINITIONS.keys():
            if scope in raw_permissions:
                normalized[scope] = bool(raw_permissions.get(scope))
        return normalized
    if isinstance(raw_permissions, (list, tuple, set)):
        allowed = {str(item).strip() for item in raw_permissions}
        return {scope: scope in allowed for scope in ADMIN_SCOPE_DEFINITIONS.keys()}
    raw_text = str(raw_permissions or "").strip()
    if not raw_text:
        return defaults
    allowed = {chunk.strip() for chunk in raw_text.split(",") if chunk.strip()}
    return {scope: scope in allowed for scope in ADMIN_SCOPE_DEFINITIONS.keys()}


def _has_admin_scope(user, scope):
    if not isinstance(user, dict):
        return False
    if _normalize_user_role(user.get("role", "viewer")) != "admin":
        return False
    normalized_scope = str(scope or "").strip()
    if not normalized_scope:
        return False
    permissions = _normalize_admin_permissions(user.get("admin_permissions"))
    return bool(permissions.get(normalized_scope, False))


def _apply_grantable_admin_permissions(actor_user, requested_permissions, existing_permissions=None):
    """
    Constrain requested admin scopes to what the actor can grant.
    - For scopes actor has: apply requested value.
    - For scopes actor does not have:
      - on update (existing_permissions provided): preserve existing value
      - on create: force False
    """
    requested = _normalize_admin_permissions(requested_permissions)
    existing = _normalize_admin_permissions(existing_permissions) if existing_permissions is not None else None
    actor_scopes = _normalize_admin_permissions((actor_user or {}).get("admin_permissions"))

    # Backward compatibility: if actor has full scope set, keep requested payload untouched.
    if actor_scopes and all(bool(actor_scopes.get(scope, False)) for scope in ADMIN_SCOPE_DEFINITIONS.keys()):
        return requested

    constrained = {}
    for scope in ADMIN_SCOPE_DEFINITIONS.keys():
        if bool(actor_scopes.get(scope, False)):
            constrained[scope] = bool(requested.get(scope, False))
        elif existing is not None:
            constrained[scope] = bool(existing.get(scope, False))
        else:
            constrained[scope] = False
    return constrained


def _cleanup_rate_limit_buckets(now_ts):
    stale_after = max(
        _login_rate_limit_window_seconds + _login_rate_limit_lockout_seconds,
        _api_rate_limit_window_seconds * 2.0,
    )
    for bucket in (_login_rate_limit_by_ip, _api_rate_limit_by_actor):
        stale_keys = []
        for key, state in bucket.items():
            last_seen = float(state.get("last_seen", 0.0) or 0.0)
            if last_seen > 0.0 and (now_ts - last_seen) > stale_after:
                stale_keys.append(key)
        for key in stale_keys:
            bucket.pop(key, None)


def _check_login_rate_limit(ip_addr):
    now_ts = time.time()
    with _auth_security_lock:
        _cleanup_rate_limit_buckets(now_ts)
        state = _login_rate_limit_by_ip.setdefault(
            str(ip_addr or "unknown"),
            {"attempts": deque(), "blocked_until": 0.0, "last_seen": now_ts},
        )
        state["last_seen"] = now_ts
        blocked_until = float(state.get("blocked_until", 0.0) or 0.0)
        if blocked_until > now_ts:
            return False, int(max(1, round(blocked_until - now_ts)))
        attempts = state.setdefault("attempts", deque())
        while attempts and (now_ts - float(attempts[0])) > _login_rate_limit_window_seconds:
            attempts.popleft()
        if len(attempts) >= _login_rate_limit_attempts:
            state["blocked_until"] = now_ts + _login_rate_limit_lockout_seconds
            attempts.clear()
            return False, int(_login_rate_limit_lockout_seconds)
        return True, 0


def _register_login_failure(ip_addr):
    now_ts = time.time()
    with _auth_security_lock:
        state = _login_rate_limit_by_ip.setdefault(
            str(ip_addr or "unknown"),
            {"attempts": deque(), "blocked_until": 0.0, "last_seen": now_ts},
        )
        state["last_seen"] = now_ts
        attempts = state.setdefault("attempts", deque())
        while attempts and (now_ts - float(attempts[0])) > _login_rate_limit_window_seconds:
            attempts.popleft()
        attempts.append(now_ts)
        if len(attempts) >= _login_rate_limit_attempts:
            state["blocked_until"] = now_ts + _login_rate_limit_lockout_seconds
            attempts.clear()


def _register_login_success(ip_addr):
    with _auth_security_lock:
        _login_rate_limit_by_ip.pop(str(ip_addr or "unknown"), None)


def _check_api_rate_limit(actor_key):
    now_ts = time.time()
    with _auth_security_lock:
        _cleanup_rate_limit_buckets(now_ts)
        state = _api_rate_limit_by_actor.setdefault(
            str(actor_key or "anonymous"),
            {"hits": deque(), "last_seen": now_ts},
        )
        state["last_seen"] = now_ts
        hits = state.setdefault("hits", deque())
        while hits and (now_ts - float(hits[0])) > _api_rate_limit_window_seconds:
            hits.popleft()
        if len(hits) >= _api_rate_limit_requests:
            retry_after = int(max(1, round(_api_rate_limit_window_seconds - (now_ts - float(hits[0])))))
            return False, retry_after
        hits.append(now_ts)
    return True, 0

TENANT_REGISTRY_FILE = "tenants.json"
USER_SEED_FILE = "users.default.json"
USERS_RECOVERY_FILE = "users.json"
SENSORS_RECOVERY_FILE = getattr(bssci_config, "SENSOR_CONFIG_FILE", "endpoints.json")
BASE_STATIONS_RECOVERY_FILE = getattr(bssci_config, "BASE_STATION_CONFIG_FILE", "base_stations.json")
_ROLE_PERMISSIONS_CONFIG_KEY = "role_permissions"
_USERS_BOOTSTRAP_STATE_KEY = "bootstrap.users"
_TENANTS_BOOTSTRAP_STATE_KEY = "bootstrap.tenants"
_SENSORS_BOOTSTRAP_STATE_KEY = "bootstrap.sensors"
_BASE_STATIONS_BOOTSTRAP_STATE_KEY = "bootstrap.base_stations"


def _db_first_config_enabled():
    return bool(getattr(bssci_config, "TIMESCALE_ENABLED", False))


def _db_json_value(raw_value, fallback):
    if raw_value is None:
        return copy.deepcopy(fallback)
    if isinstance(raw_value, (dict, list)):
        return copy.deepcopy(raw_value)
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, type(fallback)):
                return parsed
        except Exception:
            return copy.deepcopy(fallback)
    return copy.deepcopy(fallback)


def _load_app_config_state_value_from_db(config_key, fallback=None):
    conn, err = _timescale_connect()
    if conn is None:
        return copy.deepcopy(fallback), err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM app_config_state WHERE config_key = %s",
                (str(config_key or "").strip(),),
            )
            row = cur.fetchone()
        if not row:
            return copy.deepcopy(fallback), None
        return _db_json_value(row[0], fallback if fallback is not None else {}), None
    except Exception as exc:
        return copy.deepcopy(fallback), str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _timescale_fetch_latest_sensor_decoded_values_map(
    tenant_id: Optional[str] = None,
    sensor_euis: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    tenant = _default_tenant_id() if tenant_id is None else tenant_id
    normalized_euis = [
        str(eui or "").strip().upper()
        for eui in (sensor_euis or [])
        if str(eui or "").strip()
    ]
    if sensor_euis is not None and not normalized_euis:
        return {}
    conn, err = _timescale_connect()
    if conn is None:
        return {}
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            if _is_global_tenant_scope(tenant):
                if normalized_euis:
                    cur.execute(
                        """
                        SELECT DISTINCT ON (sensor_eui) sensor_eui, decoded
                        FROM telemetry_uplink
                        WHERE sensor_eui = ANY(%s)
                        ORDER BY sensor_eui, received_at DESC
                        """,
                        (normalized_euis,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT DISTINCT ON (sensor_eui) sensor_eui, decoded
                        FROM telemetry_uplink
                        ORDER BY sensor_eui, received_at DESC
                        """
                    )
            else:
                tenant_key = _normalize_tenant_id(tenant, fallback=_default_tenant_id())
                if normalized_euis:
                    cur.execute(
                        """
                        SELECT DISTINCT ON (sensor_eui) sensor_eui, decoded
                        FROM telemetry_uplink
                        WHERE tenant_id = %s AND sensor_eui = ANY(%s)
                        ORDER BY sensor_eui, received_at DESC
                        """,
                        (tenant_key, normalized_euis),
                    )
                else:
                    cur.execute(
                        """
                        SELECT DISTINCT ON (sensor_eui) sensor_eui, decoded
                        FROM telemetry_uplink
                        WHERE tenant_id = %s
                        ORDER BY sensor_eui, received_at DESC
                        """,
                        (tenant_key,),
                    )
            rows = cur.fetchall() or []
        result: Dict[str, Dict[str, Any]] = {}
        for sensor_eui, decoded in rows:
            eui_upper = str(sensor_eui or "").strip().upper()
            if not eui_upper:
                continue
            decoded_payload = decoded if isinstance(decoded, dict) else {}
            result[eui_upper] = dict((decoded_payload.get("values") or {}))
        return result
    except Exception:
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _save_app_config_state_value_to_db(config_key, payload):
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_config_state (config_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (config_key) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                """,
                (
                    str(config_key or "").strip(),
                    json.dumps(payload or {}, separators=(",", ":"), ensure_ascii=True),
                ),
            )
        return True, None
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_tenant_registry_seed_payload():
    fallback_payload = {
        "tenants": [
            {
                "id": "test",
                "name": "Testovaci tenant",
                "description": "Testovaci tenant so seeded demo senzormi a telemetriou.",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        ]
    }
    try:
        with open(TENANT_REGISTRY_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
            if isinstance(payload, dict):
                return payload
    except FileNotFoundError:
        return fallback_payload
    except Exception as exc:
        logger.warning("Failed to read tenant registry seed '%s': %s", TENANT_REGISTRY_FILE, exc)
    return fallback_payload


def _load_users_file_payload():
    try:
        with open('users.json', 'r', encoding='utf-8') as f:
            payload = json.load(f)
            if isinstance(payload, dict):
                return payload
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("Failed to read users.json for DB migration seed: %s", exc)
    return {}


def _load_tenant_registry_file_payload():
    try:
        with open(TENANT_REGISTRY_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
            if isinstance(payload, dict):
                return payload
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("Failed to read %s for DB migration seed: %s", TENANT_REGISTRY_FILE, exc)
    return {}


def _bootstrap_users_store_if_needed():
    if not _db_first_config_enabled():
        return {"used": False, "source": "json", "seeded": False}
    loaded, err = _load_users_payload_from_db()
    users_map = (loaded or {}).get("users", {}) if isinstance(loaded, dict) else {}
    if isinstance(users_map, dict) and users_map:
        state, _ = _load_app_config_state_value_from_db(_USERS_BOOTSTRAP_STATE_KEY, {})
        if not isinstance(state, dict) or not state:
            state = {
                "seeded_at": datetime.now(timezone.utc).isoformat(),
                "seed_source": "preexisting_db",
                "seeded": False,
            }
            _save_app_config_state_value_to_db(_USERS_BOOTSTRAP_STATE_KEY, state)
        return {
            "used": True,
            "source": "db",
            "seeded": False,
            "state": state if isinstance(state, dict) else {},
        }

    seed_source = "default_seed"
    seed_payload = _load_users_file_payload()
    if not isinstance(seed_payload, dict) or not isinstance(seed_payload.get("users"), dict) or not seed_payload.get("users"):
        seed_payload = _load_default_user_seed_payload()
        seed_source = "users.default.json"
    else:
        seed_source = "users.json"

    ok, save_err = _save_users_payload_to_db(seed_payload)
    if ok:
        state_payload = {
            "seeded_at": datetime.now(timezone.utc).isoformat(),
            "seed_source": seed_source,
            "seeded": True,
        }
        _save_app_config_state_value_to_db(_USERS_BOOTSTRAP_STATE_KEY, state_payload)
        return {
            "used": True,
            "source": "db",
            "seeded": True,
            "state": state_payload,
        }
    return {
        "used": False,
        "source": "json-fallback",
        "seeded": False,
        "error": save_err or err,
    }


def _bootstrap_tenant_registry_store_if_needed():
    if not _db_first_config_enabled():
        return {"used": False, "source": "json", "seeded": False}
    loaded, err = _load_tenant_registry_from_db()
    tenants = (loaded or {}).get("tenants", []) if isinstance(loaded, dict) else []
    if isinstance(tenants, list) and tenants:
        state, _ = _load_app_config_state_value_from_db(_TENANTS_BOOTSTRAP_STATE_KEY, {})
        if not isinstance(state, dict) or not state:
            state = {
                "seeded_at": datetime.now(timezone.utc).isoformat(),
                "seed_source": "preexisting_db",
                "seeded": False,
            }
            _save_app_config_state_value_to_db(_TENANTS_BOOTSTRAP_STATE_KEY, state)
        return {
            "used": True,
            "source": "db",
            "seeded": False,
            "state": state if isinstance(state, dict) else {},
        }

    seed_source = TENANT_REGISTRY_FILE
    seed_payload = _load_tenant_registry_file_payload()
    if not isinstance(seed_payload, dict) or not isinstance(seed_payload.get("tenants"), list) or not seed_payload.get("tenants"):
        seed_payload = _load_tenant_registry_seed_payload()
        seed_source = "default_tenant_seed"

    ok, save_err = _save_tenant_registry_to_db(seed_payload)
    if ok:
        state_payload = {
            "seeded_at": datetime.now(timezone.utc).isoformat(),
            "seed_source": seed_source,
            "seeded": True,
        }
        _save_app_config_state_value_to_db(_TENANTS_BOOTSTRAP_STATE_KEY, state_payload)
        return {
            "used": True,
            "source": "db",
            "seeded": True,
            "state": state_payload,
        }
    return {
        "used": False,
        "source": "json-fallback",
        "seeded": False,
        "error": save_err or err,
    }


def _users_storage_backend_meta():
    if not _db_first_config_enabled():
        return {
            "backend": "json",
            "db_enabled": False,
            "bootstrap_state": {},
            "seed_defaults_file": USER_SEED_FILE,
            "recovery_file": USERS_RECOVERY_FILE,
        }
    state, err = _load_app_config_state_value_from_db(_USERS_BOOTSTRAP_STATE_KEY, {})
    return {
        "backend": "db",
        "db_enabled": True,
        "bootstrap_state": state if isinstance(state, dict) else {},
        "bootstrap_error": err or "",
        "seed_defaults_file": USER_SEED_FILE,
        "recovery_file": USERS_RECOVERY_FILE,
    }


def _tenant_storage_backend_meta():
    if not _db_first_config_enabled():
        return {
            "backend": "json",
            "db_enabled": False,
            "bootstrap_state": {},
            "seed_defaults_file": TENANT_REGISTRY_FILE,
            "recovery_file": TENANT_REGISTRY_FILE,
        }
    state, err = _load_app_config_state_value_from_db(_TENANTS_BOOTSTRAP_STATE_KEY, {})
    return {
        "backend": "db",
        "db_enabled": True,
        "bootstrap_state": state if isinstance(state, dict) else {},
        "bootstrap_error": err or "",
        "seed_defaults_file": TENANT_REGISTRY_FILE,
        "recovery_file": TENANT_REGISTRY_FILE,
    }


def _sensors_storage_backend_meta():
    if not _db_first_config_enabled():
        return {
            "backend": "json",
            "db_enabled": False,
            "bootstrap_state": {},
            "seed_defaults_file": SENSORS_RECOVERY_FILE,
            "recovery_file": SENSORS_RECOVERY_FILE,
        }
    state, err = _load_app_config_state_value_from_db(_SENSORS_BOOTSTRAP_STATE_KEY, {})
    return {
        "backend": "db",
        "db_enabled": True,
        "bootstrap_state": state if isinstance(state, dict) else {},
        "bootstrap_error": err or "",
        "seed_defaults_file": SENSORS_RECOVERY_FILE,
        "recovery_file": SENSORS_RECOVERY_FILE,
    }


def _base_stations_storage_backend_meta():
    if not _db_first_config_enabled():
        return {
            "backend": "json",
            "db_enabled": False,
            "bootstrap_state": {},
            "seed_defaults_file": BASE_STATIONS_RECOVERY_FILE,
            "recovery_file": BASE_STATIONS_RECOVERY_FILE,
        }
    state, err = _load_app_config_state_value_from_db(_BASE_STATIONS_BOOTSTRAP_STATE_KEY, {})
    return {
        "backend": "db",
        "db_enabled": True,
        "bootstrap_state": state if isinstance(state, dict) else {},
        "bootstrap_error": err or "",
        "seed_defaults_file": BASE_STATIONS_RECOVERY_FILE,
        "recovery_file": BASE_STATIONS_RECOVERY_FILE,
    }


def _load_sensors_file_payload():
    sensor_file = getattr(bssci_config, "SENSOR_CONFIG_FILE", "endpoints.json")
    try:
        with open(sensor_file, "r", encoding="utf-8") as f:
            data = json.load(f) or []
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("Failed to read %s for DB migration seed: %s", sensor_file, exc)
        return []


def _load_base_station_file_payload():
    config_file = getattr(bssci_config, "BASE_STATION_CONFIG_FILE", "base_stations.json")
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        return data if isinstance(data, dict) else {"base_stations": {}}
    except Exception as exc:
        logger.warning("Failed to read %s for DB migration seed: %s", config_file, exc)
        return {"base_stations": {}}


def _load_sensors_payload_from_db():
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT eui, payload
                FROM app_sensors
                ORDER BY eui ASC
            """)
            rows = cur.fetchall() or []
        sensors = []
        for eui, payload in rows:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}
            payload = copy.deepcopy(payload)
            payload["eui"] = _normalize_eui_upper(payload.get("eui") or eui)
            sensors.append(payload)
        return sensors, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _save_sensors_payload_to_db(sensors):
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        _ensure_timescale_schema(conn)
        normalized_rows = []
        tenant_ids = set()
        for raw_sensor in (sensors or []):
            if not isinstance(raw_sensor, dict):
                continue
            payload = copy.deepcopy(raw_sensor)
            eui = _normalize_eui_upper(payload.get("eui"))
            if not eui:
                continue
            payload["eui"] = eui
            tenant_id = _tenant_id_from_sensor(payload)
            tenant_ids.add(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()))
            normalized_rows.append((eui, tenant_id, json.dumps(payload, ensure_ascii=False)))

        conn.autocommit = False
        with conn.cursor() as cur:
            if tenant_ids:
                cur.executemany(
                    """
                    INSERT INTO tenants (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    [(tenant_id, tenant_id) for tenant_id in sorted(tenant_ids)],
                )
            cur.execute("SELECT eui FROM app_sensors")
            existing = {str((row or [None])[0] or "").strip().upper() for row in (cur.fetchall() or [])}
            incoming = {row[0] for row in normalized_rows}
            for eui in sorted(existing - incoming):
                cur.execute("DELETE FROM app_sensors WHERE eui = %s", (eui,))
            for eui, tenant_id, payload_json in normalized_rows:
                cur.execute(
                    """
                    INSERT INTO app_sensors (eui, tenant_id, payload, updated_at)
                    VALUES (%s, %s, %s::jsonb, NOW())
                    ON CONFLICT (eui) DO UPDATE SET
                        tenant_id = EXCLUDED.tenant_id,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                    """,
                    (eui, tenant_id, payload_json),
                )
        conn.commit()
        return True, None
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_base_station_payload_from_db():
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT eui, payload
                FROM app_base_stations
                ORDER BY eui ASC
            """)
            rows = cur.fetchall() or []
        base_stations = {}
        for eui, payload in rows:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            if not isinstance(payload, dict):
                payload = {}
            payload = copy.deepcopy(payload)
            canonical_eui = _normalize_eui_upper(payload.get("eui") or eui)
            payload["eui"] = canonical_eui
            base_stations[canonical_eui] = payload
        return {"base_stations": base_stations}, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _save_base_station_payload_to_db(config):
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        _ensure_timescale_schema(conn)
        base_stations = (config or {}).get("base_stations", {}) if isinstance(config, dict) else {}
        if not isinstance(base_stations, dict):
            base_stations = {}
        normalized_rows = []
        tenant_ids = set()
        for raw_eui, raw_bs in (base_stations or {}).items():
            if not isinstance(raw_bs, dict):
                continue
            payload = copy.deepcopy(raw_bs)
            eui = _normalize_eui_upper(payload.get("eui") or raw_eui)
            if not eui:
                continue
            payload["eui"] = eui
            tenant_id = _tenant_id_from_base_station(payload)
            tenant_ids.add(_normalize_tenant_id(tenant_id, fallback=_default_tenant_id()))
            normalized_rows.append((eui, tenant_id, json.dumps(payload, ensure_ascii=False)))

        conn.autocommit = False
        with conn.cursor() as cur:
            if tenant_ids:
                cur.executemany(
                    """
                    INSERT INTO tenants (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    [(tenant_id, tenant_id) for tenant_id in sorted(tenant_ids)],
                )
            cur.execute("SELECT eui FROM app_base_stations")
            existing = {str((row or [None])[0] or "").strip().upper() for row in (cur.fetchall() or [])}
            incoming = {row[0] for row in normalized_rows}
            for eui in sorted(existing - incoming):
                cur.execute("DELETE FROM app_base_stations WHERE eui = %s", (eui,))
            for eui, tenant_id, payload_json in normalized_rows:
                cur.execute(
                    """
                    INSERT INTO app_base_stations (eui, tenant_id, payload, updated_at)
                    VALUES (%s, %s, %s::jsonb, NOW())
                    ON CONFLICT (eui) DO UPDATE SET
                        tenant_id = EXCLUDED.tenant_id,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                    """,
                    (eui, tenant_id, payload_json),
                )
        conn.commit()
        return True, None
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _bootstrap_sensors_store_if_needed():
    if not _db_first_config_enabled():
        return {"used": False, "source": "json", "seeded": False}
    current_rows, err = _load_sensors_payload_from_db()
    if isinstance(current_rows, list) and current_rows:
        state, state_err = _load_app_config_state_value_from_db(_SENSORS_BOOTSTRAP_STATE_KEY, {})
        if not isinstance(state, dict) or not state:
            state_payload = {
                "seeded_at": datetime.now(timezone.utc).isoformat(),
                "seed_source": "preexisting_db",
                "seeded": False,
            }
            _save_app_config_state_value_to_db(_SENSORS_BOOTSTRAP_STATE_KEY, state_payload)
            state = state_payload
        return {"used": False, "source": "db", "seeded": False, "state": state, "error": state_err or err}
    seed_payload = _load_sensors_file_payload()
    seed_source = SENSORS_RECOVERY_FILE
    ok, save_err = _save_sensors_payload_to_db(seed_payload)
    if ok:
        state_payload = {
            "seeded_at": datetime.now(timezone.utc).isoformat(),
            "seed_source": seed_source,
            "seeded": True,
        }
        _save_app_config_state_value_to_db(_SENSORS_BOOTSTRAP_STATE_KEY, state_payload)
        return {"used": True, "source": seed_source, "seeded": True, "state": state_payload}
    return {"used": False, "source": "json-fallback", "seeded": False, "error": save_err or err}


def _bootstrap_base_stations_store_if_needed():
    if not _db_first_config_enabled():
        return {"used": False, "source": "json", "seeded": False}
    current_payload, err = _load_base_station_payload_from_db()
    current_map = current_payload.get("base_stations", {}) if isinstance(current_payload, dict) else {}
    if isinstance(current_map, dict) and current_map:
        state, state_err = _load_app_config_state_value_from_db(_BASE_STATIONS_BOOTSTRAP_STATE_KEY, {})
        if not isinstance(state, dict) or not state:
            state_payload = {
                "seeded_at": datetime.now(timezone.utc).isoformat(),
                "seed_source": "preexisting_db",
                "seeded": False,
            }
            _save_app_config_state_value_to_db(_BASE_STATIONS_BOOTSTRAP_STATE_KEY, state_payload)
            state = state_payload
        return {"used": False, "source": "db", "seeded": False, "state": state, "error": state_err or err}
    seed_payload = _load_base_station_file_payload()
    seed_source = BASE_STATIONS_RECOVERY_FILE
    ok, save_err = _save_base_station_payload_to_db(seed_payload)
    if ok:
        state_payload = {
            "seeded_at": datetime.now(timezone.utc).isoformat(),
            "seed_source": seed_source,
            "seeded": True,
        }
        _save_app_config_state_value_to_db(_BASE_STATIONS_BOOTSTRAP_STATE_KEY, state_payload)
        return {"used": True, "source": seed_source, "seeded": True, "state": state_payload}
    return {"used": False, "source": "json-fallback", "seeded": False, "error": save_err or err}


def _load_users_payload_from_db():
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT username, password, role, name, tenant_id, active, require_password_change, admin_permissions
                FROM app_users
                ORDER BY username ASC
            """)
            users = {}
            for row in (cur.fetchall() or []):
                username = str(row[0] or "").strip()
                if not username:
                    continue
                users[username] = {
                    "password": str(row[1] or ""),
                    "role": str(row[2] or "viewer"),
                    "name": str(row[3] or username),
                    "tenant_id": str(row[4] or ""),
                    "active": bool(row[5]),
                }
                if bool(row[6]):
                    users[username]["require_password_change"] = True
                admin_permissions = _db_json_value(row[7], {})
                if isinstance(admin_permissions, dict) and admin_permissions:
                    users[username]["admin_permissions"] = admin_permissions

            cur.execute(
                "SELECT payload FROM app_config_state WHERE config_key = %s",
                (_ROLE_PERMISSIONS_CONFIG_KEY,),
            )
            row = cur.fetchone()
            role_permissions = _db_json_value(row[0] if row else None, _default_role_permissions_map())
        return {
            "users": users,
            "role_permissions": role_permissions,
        }, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _save_users_payload_to_db(users_data):
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        conn.autocommit = False
        _ensure_timescale_schema(conn)
        users_map = users_data.get("users", {}) if isinstance(users_data, dict) else {}
        role_permissions = users_data.get("role_permissions", {}) if isinstance(users_data, dict) else {}
        desired_usernames = {
            str(username).strip()
            for username, user in (users_map.items() if isinstance(users_map, dict) else [])
            if str(username).strip() and isinstance(user, dict)
        }
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM app_users")
            existing_usernames = {
                str(row[0] or "").strip()
                for row in (cur.fetchall() or [])
                if str(row[0] or "").strip()
            }
            stale_usernames = sorted(existing_usernames - desired_usernames)
            if stale_usernames:
                cur.executemany(
                    "DELETE FROM app_users WHERE username = %s",
                    [(username,) for username in stale_usernames],
                )

            for username in sorted(desired_usernames):
                user = users_map.get(username) or {}
                cur.execute(
                    """
                    INSERT INTO app_users
                        (username, password, role, name, tenant_id, active, require_password_change, admin_permissions, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (username) DO UPDATE SET
                        password = EXCLUDED.password,
                        role = EXCLUDED.role,
                        name = EXCLUDED.name,
                        tenant_id = EXCLUDED.tenant_id,
                        active = EXCLUDED.active,
                        require_password_change = EXCLUDED.require_password_change,
                        admin_permissions = EXCLUDED.admin_permissions,
                        updated_at = NOW()
                    """,
                    (
                        username,
                        str(user.get("password") or ""),
                        str(user.get("role") or "viewer"),
                        str(user.get("name") or username),
                        str(user.get("tenant_id") or ""),
                        bool(user.get("active", True)),
                        bool(user.get("require_password_change", False)),
                        json.dumps(user.get("admin_permissions") or {}, separators=(",", ":"), ensure_ascii=True),
                    ),
                )

            cur.execute(
                """
                INSERT INTO app_config_state (config_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (config_key) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                """,
                (
                    _ROLE_PERMISSIONS_CONFIG_KEY,
                    json.dumps(role_permissions or {}, separators=(",", ":"), ensure_ascii=True),
                ),
            )
        conn.commit()
        return True, None
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(exc)
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _load_tenant_registry_from_db():
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, created_at
                FROM tenant_registry_meta
                ORDER BY id ASC
            """)
            tenants = []
            for row in (cur.fetchall() or []):
                tenant_id = _sanitize_tenant_id(row[0])
                if not tenant_id:
                    continue
                tenants.append({
                    "id": tenant_id,
                    "name": str(row[1] or tenant_id).strip() or tenant_id,
                    "description": str(row[2] or "").strip(),
                    "created_at": str(row[3]) if row[3] else datetime.now(timezone.utc).isoformat(),
                })
        return {"tenants": tenants}, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _save_tenant_registry_to_db(registry_data):
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        conn.autocommit = False
        _ensure_timescale_schema(conn)
        tenants_raw = registry_data.get("tenants", []) if isinstance(registry_data, dict) else []
        normalized_rows = []
        for item in tenants_raw:
            if not isinstance(item, dict):
                continue
            tenant_id = _sanitize_tenant_id(item.get("id") or item.get("tenant_id"))
            if not tenant_id or _is_reserved_default_tenant(tenant_id):
                continue
            normalized_rows.append((
                tenant_id,
                str(item.get("name") or tenant_id).strip()[:120] or tenant_id,
                str(item.get("description") or "").strip()[:240],
                str(item.get("created_at") or datetime.now(timezone.utc).isoformat()),
            ))

        desired_ids = {row[0] for row in normalized_rows}
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM tenant_registry_meta")
            existing_ids = {
                _sanitize_tenant_id(row[0])
                for row in (cur.fetchall() or [])
                if _sanitize_tenant_id(row[0])
            }
            stale_ids = sorted(existing_ids - desired_ids)
            if stale_ids:
                cur.executemany(
                    "DELETE FROM tenant_registry_meta WHERE id = %s",
                    [(tenant_id,) for tenant_id in stale_ids],
                )
            for tenant_id, name, description, created_at in normalized_rows:
                cur.execute(
                    """
                    INSERT INTO tenant_registry_meta (id, name, description, created_at, updated_at)
                    VALUES (%s, %s, %s, %s::timestamptz, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        updated_at = NOW()
                    """,
                    (tenant_id, name, description, created_at),
                )
                cur.execute(
                    """
                    INSERT INTO tenants (id, name, description, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        updated_at = NOW()
                    """,
                    (tenant_id, name, description),
                )
        conn.commit()
        return True, None
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(exc)
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _append_admin_audit_entry_to_db(entry):
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO admin_audit_log
                    (ts, action, entity, target_id, status, actor, role, actor_tenant, active_tenant, method, path, ip, user_agent, details)
                VALUES
                    (%s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    str(entry.get("timestamp") or datetime.now(timezone.utc).isoformat()),
                    str(entry.get("action") or "unknown"),
                    str(entry.get("entity") or "unknown"),
                    str(entry.get("target_id") or ""),
                    str(entry.get("status") or "success"),
                    str(entry.get("actor") or "system"),
                    str(entry.get("role") or ""),
                    str(entry.get("actor_tenant") or _default_tenant_id()),
                    str(entry.get("active_tenant") or _default_tenant_id()),
                    str(entry.get("method") or ""),
                    str(entry.get("path") or ""),
                    str(entry.get("ip") or ""),
                    str(entry.get("user_agent") or ""),
                    json.dumps(entry.get("details") or {}, separators=(",", ":"), ensure_ascii=True),
                ),
            )
        return True, None
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_admin_audit_entries_from_db(limit=None):
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        max_rows = max_admin_audit_entries if limit is None else max(1, int(limit))
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, action, entity, target_id, status, actor, role, actor_tenant, active_tenant, method, path, ip, user_agent, details
                FROM admin_audit_log
                ORDER BY ts DESC
                LIMIT %s
                """,
                (max_rows,),
            )
            rows = cur.fetchall() or []
        entries = []
        for row in reversed(rows):
            entries.append({
                "timestamp": str(row[0].isoformat(timespec="milliseconds") if hasattr(row[0], "isoformat") else row[0]),
                "action": str(row[1] or "").strip(),
                "entity": str(row[2] or "").strip(),
                "target_id": str(row[3] or "").strip(),
                "status": str(row[4] or "").strip().lower() or "success",
                "actor": str(row[5] or "").strip(),
                "role": str(row[6] or "").strip(),
                "actor_tenant": str(row[7] or "").strip(),
                "active_tenant": str(row[8] or "").strip(),
                "method": str(row[9] or "").strip(),
                "path": str(row[10] or "").strip(),
                "ip": str(row[11] or "").strip(),
                "user_agent": str(row[12] or "").strip(),
                "details": _db_json_value(row[13], {}),
            })
        return entries, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _clear_admin_audit_entries_in_db():
    conn, err = _timescale_connect()
    if conn is None:
        return False, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE admin_audit_log")
        return True, None
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_tenant_usage_meta_from_db():
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    ids.id,
                    COALESCE(NULLIF(trm.name, ''), NULLIF(t.name, ''), ids.id) AS display_name,
                    COALESCE(trm.description, t.description, '') AS description,
                    COALESCE(trm.created_at, t.created_at) AS created_at,
                    COALESCE(u.user_count, 0)::BIGINT AS user_count,
                    COALESCE(ev.event_count, 0)::BIGINT AS inventory_events,
                    COALESCE(te.telemetry_count, 0)::BIGINT AS telemetry_points
                FROM (
                    SELECT id FROM tenant_registry_meta
                    UNION
                    SELECT id FROM tenants
                    UNION
                    SELECT DISTINCT tenant_id AS id
                    FROM app_users
                    WHERE tenant_id IS NOT NULL AND tenant_id <> ''
                ) ids
                LEFT JOIN tenant_registry_meta trm ON trm.id = ids.id
                LEFT JOIN tenants t ON t.id = ids.id
                LEFT JOIN (
                    SELECT tenant_id, COUNT(*) AS user_count
                    FROM app_users
                    WHERE tenant_id IS NOT NULL AND tenant_id <> ''
                    GROUP BY tenant_id
                ) u ON u.tenant_id = ids.id
                LEFT JOIN (
                    SELECT tenant_id, COUNT(*) AS event_count
                    FROM inventory_events
                    GROUP BY tenant_id
                ) ev ON ev.tenant_id = ids.id
                LEFT JOIN (
                    SELECT tenant_id, COUNT(*) AS telemetry_count
                    FROM telemetry_uplink
                    GROUP BY tenant_id
                ) te ON te.tenant_id = ids.id
                ORDER BY ids.id ASC
            """)
            rows = cur.fetchall() or []
        tenant_meta = {}
        for row in rows:
            tenant_id = _sanitize_tenant_id(row[0])
            if not tenant_id:
                continue
            tenant_meta[tenant_id] = {
                "id": tenant_id,
                "name": str(row[1] or tenant_id).strip() or tenant_id,
                "description": str(row[2] or "").strip(),
                "created_at": str(row[3]) if row[3] else None,
                "user_count": int(row[4] or 0),
                "inventory_events": int(row[5] or 0),
                "telemetry_points": int(row[6] or 0),
            }
        return tenant_meta, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _admin_audit_where_sql(action_filter='all', entity_filter='all', actor_filter='all', status_filter='all', text_filter='', target_filter=''):
    clauses = []
    params = []
    if action_filter != 'all':
        clauses.append("LOWER(action) = %s")
        params.append(action_filter)
    if entity_filter != 'all':
        clauses.append("LOWER(entity) = %s")
        params.append(entity_filter)
    if actor_filter != 'all':
        clauses.append("LOWER(actor) = %s")
        params.append(actor_filter)
    if status_filter != 'all':
        clauses.append("LOWER(status) = %s")
        params.append(status_filter)
    if target_filter:
        clauses.append("LOWER(target_id) = %s")
        params.append(target_filter)
    if text_filter:
        clauses.append("""
            LOWER(
                COALESCE(action, '') || ' ' ||
                COALESCE(entity, '') || ' ' ||
                COALESCE(target_id, '') || ' ' ||
                COALESCE(actor, '') || ' ' ||
                COALESCE(status, '') || ' ' ||
                COALESCE(path, '') || ' ' ||
                COALESCE(details::text, '')
            ) LIKE %s
        """)
        params.append(f"%{text_filter}%")
    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(f"({clause.strip()})" for clause in clauses)
    return where_sql, params


def _fetch_admin_audit_entries_from_db(action_filter='all', entity_filter='all', actor_filter='all', status_filter='all', text_filter='', target_filter='', limit=None, newest_first=True):
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        where_sql, params = _admin_audit_where_sql(
            action_filter=action_filter,
            entity_filter=entity_filter,
            actor_filter=actor_filter,
            status_filter=status_filter,
            text_filter=text_filter,
            target_filter=target_filter,
        )
        order_sql = "DESC" if newest_first else "ASC"
        limit_sql = ""
        query_params = list(params)
        if limit is not None:
            limit_sql = " LIMIT %s"
            query_params.append(max(1, int(limit)))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts, action, entity, target_id, status, actor, role, actor_tenant, active_tenant, method, path, ip, user_agent, details
                FROM admin_audit_log
                {where_sql}
                ORDER BY ts {order_sql}
                {limit_sql}
                """,
                query_params,
            )
            rows = cur.fetchall() or []
        entries = []
        for row in rows:
            entries.append({
                "timestamp": str(row[0].isoformat(timespec="milliseconds") if hasattr(row[0], "isoformat") else row[0]),
                "action": str(row[1] or "").strip(),
                "entity": str(row[2] or "").strip(),
                "target_id": str(row[3] or "").strip(),
                "status": str(row[4] or "").strip().lower() or "success",
                "actor": str(row[5] or "").strip(),
                "role": str(row[6] or "").strip(),
                "actor_tenant": str(row[7] or "").strip(),
                "active_tenant": str(row[8] or "").strip(),
                "method": str(row[9] or "").strip(),
                "path": str(row[10] or "").strip(),
                "ip": str(row[11] or "").strip(),
                "user_agent": str(row[12] or "").strip(),
                "details": _db_json_value(row[13], {}),
            })
        return entries, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_admin_audit_summary_from_db(action_filter='all', entity_filter='all', actor_filter='all', status_filter='all', text_filter='', target_filter='', limit=200):
    conn, err = _timescale_connect()
    if conn is None:
        return None, err
    try:
        _ensure_timescale_schema(conn)
        where_sql, params = _admin_audit_where_sql(
            action_filter=action_filter,
            entity_filter=entity_filter,
            actor_filter=actor_filter,
            status_filter=status_filter,
            text_filter=text_filter,
            target_filter=target_filter,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM admin_audit_log")
            total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(f"SELECT COUNT(*) FROM admin_audit_log {where_sql}", params)
            filtered_total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                f"""
                SELECT ts, action, entity, target_id, status, actor, role, actor_tenant, active_tenant, method, path, ip, user_agent, details
                FROM admin_audit_log
                {where_sql}
                ORDER BY ts DESC
                LIMIT %s
                """,
                [*params, max(1, int(limit))],
            )
            rows = cur.fetchall() or []

            cur.execute("SELECT DISTINCT action FROM admin_audit_log WHERE action <> '' ORDER BY action ASC")
            actions = [str(row[0] or "").strip() for row in (cur.fetchall() or []) if str(row[0] or "").strip()]

            cur.execute("SELECT DISTINCT entity FROM admin_audit_log WHERE entity <> '' ORDER BY entity ASC")
            entities = [str(row[0] or "").strip() for row in (cur.fetchall() or []) if str(row[0] or "").strip()]

            cur.execute("SELECT DISTINCT actor FROM admin_audit_log WHERE actor <> '' ORDER BY actor ASC")
            actors = [str(row[0] or "").strip() for row in (cur.fetchall() or []) if str(row[0] or "").strip()]

            cur.execute(
                f"""
                SELECT LOWER(status) AS normalized_status, COUNT(*)
                FROM admin_audit_log
                {where_sql}
                GROUP BY LOWER(status)
                """,
                params,
            )
            status_counts = {'success': 0, 'warning': 0, 'error': 0}
            for row in (cur.fetchall() or []):
                key = str(row[0] or '').strip().lower()
                if not key:
                    continue
                status_counts[key] = int(row[1] or 0)
        entries = []
        for row in rows:
            entries.append({
                "timestamp": str(row[0].isoformat(timespec="milliseconds") if hasattr(row[0], "isoformat") else row[0]),
                "action": str(row[1] or "").strip(),
                "entity": str(row[2] or "").strip(),
                "target_id": str(row[3] or "").strip(),
                "status": str(row[4] or "").strip().lower() or "success",
                "actor": str(row[5] or "").strip(),
                "role": str(row[6] or "").strip(),
                "actor_tenant": str(row[7] or "").strip(),
                "active_tenant": str(row[8] or "").strip(),
                "method": str(row[9] or "").strip(),
                "path": str(row[10] or "").strip(),
                "ip": str(row[11] or "").strip(),
                "user_agent": str(row[12] or "").strip(),
                "details": _db_json_value(row[13], {}),
            })
        return {
            'entries': entries,
            'total': total,
            'filtered_total': filtered_total,
            'actions': actions,
            'entities': entities,
            'actors': actors,
            'status_counts': status_counts,
        }, None
    except Exception as exc:
        return None, str(exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def load_tenant_registry():
    """Load tenant metadata from DB-first registry with file fallback."""
    default_tenant = _default_tenant_id()
    payload = {"tenants": []}
    changed = False

    if _db_first_config_enabled():
        bootstrap_state = _bootstrap_tenant_registry_store_if_needed()
        raw, err = _load_tenant_registry_from_db()
        if isinstance(raw, dict):
            payload = raw
            if bootstrap_state.get("seeded"):
                changed = True
        else:
            logger.warning("Failed to load tenant registry from DB, falling back to file: %s", err)
            changed = True

    if not isinstance(payload, dict) or (not payload.get("tenants") and not _db_first_config_enabled()):
        try:
            if os.path.exists(TENANT_REGISTRY_FILE):
                with open(TENANT_REGISTRY_FILE, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    payload = raw
                else:
                    changed = True
            else:
                changed = True
        except Exception as exc:
            logger.warning(f"Failed to load tenant registry '{TENANT_REGISTRY_FILE}': {exc}")
            changed = True

    tenants_raw = payload.get("tenants", [])
    if not isinstance(tenants_raw, list):
        tenants_raw = []
        changed = True

    normalized_map = {}
    for item in tenants_raw:
        if not isinstance(item, dict):
            changed = True
            continue
        tenant_id = _sanitize_tenant_id(item.get("id") or item.get("tenant_id"))
        if not tenant_id:
            changed = True
            continue
        if tenant_id in normalized_map:
            changed = True
            continue
        name = str(item.get("name") or tenant_id).strip()
        name = name[:120] if name else tenant_id
        description = str(item.get("description") or "").strip()[:240]
        created_at = str(item.get("created_at") or datetime.now(timezone.utc).isoformat())
        normalized_map[tenant_id] = {
            "id": tenant_id,
            "name": name,
            "description": description,
            "created_at": created_at,
        }

    if default_tenant not in normalized_map:
        normalized_map[default_tenant] = {
            "id": default_tenant,
            "name": "Super admin",
            "description": "Reserved global scope for super admin inventory and system-owned data.",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        changed = True

    normalized_payload = {
        "tenants": [normalized_map[key] for key in sorted(normalized_map.keys())]
    }
    if changed:
        save_tenant_registry(normalized_payload)
    return normalized_payload

def save_tenant_registry(registry_data):
    """Persist tenant registry metadata to DB-first registry with file fallback."""
    if _db_first_config_enabled():
        ok, err = _save_tenant_registry_to_db(registry_data)
        if ok:
            return True
        logger.error("Failed to save tenant registry to DB, falling back to file: %s", err)
    try:
        with open(TENANT_REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(registry_data, f, indent=2, ensure_ascii=True)
        return True
    except Exception as exc:
        logger.error(f"Failed to save tenant registry '{TENANT_REGISTRY_FILE}': {exc}")
        return False

def _tenant_registry_map():
    registry = load_tenant_registry()
    tenant_map = {}
    for entry in registry.get("tenants", []):
        if not isinstance(entry, dict):
            continue
        tenant_id = _sanitize_tenant_id(entry.get("id"))
        if not tenant_id:
            continue
        tenant_map[tenant_id] = {
            "id": tenant_id,
            "name": _tenant_scope_display_name(tenant_id, entry.get("name")),
            "description": _tenant_scope_display_description(tenant_id, entry.get("description")),
            "created_at": str(entry.get("created_at") or ""),
        }
    return tenant_map

def _upsert_tenant_registry_entry(tenant_id, name=None, description=None):
    tenant_id = _sanitize_tenant_id(tenant_id)
    if not tenant_id:
        return False, "Invalid tenant id", None
    if _is_reserved_default_tenant(tenant_id):
        return False, "Reserved super admin scope cannot be modified", None

    registry = load_tenant_registry()
    tenant_map = {
        str(item.get("id")).strip().lower(): item
        for item in registry.get("tenants", [])
        if isinstance(item, dict) and _sanitize_tenant_id(item.get("id"))
    }
    existing = tenant_map.get(tenant_id)
    now_iso = datetime.now(timezone.utc).isoformat()

    if existing is None:
        existing = {
            "id": tenant_id,
            "name": str(name or tenant_id).strip() or tenant_id,
            "description": str(description or "").strip(),
            "created_at": now_iso,
        }
    else:
        if name is not None:
            existing["name"] = str(name).strip() or tenant_id
        else:
            existing["name"] = str(existing.get("name") or tenant_id).strip() or tenant_id
        if description is not None:
            existing["description"] = str(description).strip()
        else:
            existing["description"] = str(existing.get("description") or "").strip()
        existing["created_at"] = str(existing.get("created_at") or now_iso)

    existing["name"] = existing["name"][:120]
    existing["description"] = existing["description"][:240]
    tenant_map[tenant_id] = existing
    next_payload = {"tenants": [tenant_map[key] for key in sorted(tenant_map.keys())]}

    if not save_tenant_registry(next_payload):
        return False, "Failed to persist tenant registry", None
    return True, None, existing

# User management functions
def load_users():
    """Load users from DB-first store with JSON fallback."""
    try:
        data = {}
        changed = False
        if _db_first_config_enabled():
            bootstrap_state = _bootstrap_users_store_if_needed()
            loaded, err = _load_users_payload_from_db()
            if isinstance(loaded, dict):
                data = loaded
                if bootstrap_state.get("seeded"):
                    changed = True
            else:
                logger.warning("Failed to load users from DB, falling back to JSON: %s", err)
                changed = True
        if not isinstance(data, dict) or not data:
            try:
                with open('users.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except FileNotFoundError:
                data = {}
                changed = True

        if not isinstance(data, dict):
            data = {}
            changed = True

        users = data.setdefault("users", {})
        normalized_role_permissions, role_permissions_changed = _normalize_role_permissions_map(data.get("role_permissions"))
        data["role_permissions"] = normalized_role_permissions
        default_tenant = _default_tenant_id()
        changed = changed or role_permissions_changed

        bootstrap_defaults_enabled = bool(getattr(bssci_config, "AUTH_BOOTSTRAP_DEFAULT_USERS", True))
        bootstrap_demo_enabled = bool(getattr(bssci_config, "AUTH_BOOTSTRAP_DEMO_USERS", True))
        seed_payload = None
        default_admin_password = "admin123"
        if bootstrap_defaults_enabled:
            seed_payload = _load_default_user_seed_payload()
            default_admin_password = _bootstrap_admin_default_password(seed_payload)
            seed_users = seed_payload.get("users", {}) if isinstance(seed_payload, dict) else {}
            bootstrap_alias_map = {
                "customer": ("viewer",),
                "viewer": ("customer",),
            }
            for username, bootstrap_user in seed_users.items():
                if not isinstance(bootstrap_user, dict):
                    continue
                if username != "admin" and not bootstrap_demo_enabled:
                    continue
                aliases = bootstrap_alias_map.get(username, ())
                if any(alias in users and isinstance(users.get(alias), dict) for alias in aliases):
                    continue
                if username not in users or not isinstance(users.get(username), dict):
                    users[username] = copy.deepcopy(bootstrap_user)
                    changed = True

        for username, user in list(users.items()):
            if not isinstance(user, dict):
                users.pop(username, None)
                changed = True
                continue
            normalized_role = _normalize_user_role(user.get("role", "viewer"))
            if user.get("role") != normalized_role:
                user["role"] = normalized_role
                changed = True
            normalized_active = bool(user.get("active", True))
            if "active" not in user or bool(user.get("active", True)) != normalized_active:
                user["active"] = normalized_active
                changed = True
            if normalized_role == "admin":
                normalized_admin_permissions = _normalize_admin_permissions(user.get("admin_permissions"))
                if user.get("admin_permissions") != normalized_admin_permissions:
                    user["admin_permissions"] = normalized_admin_permissions
                    changed = True
                bootstrap_password_state = _resolve_bootstrap_password_state(
                    user,
                    bootstrap_admin_password=default_admin_password,
                )
                if user.get("bootstrap_password_state") != bootstrap_password_state:
                    user["bootstrap_password_state"] = bootstrap_password_state
                    changed = True
                require_password_change = bootstrap_password_state == "pending"
                if bool(user.get("require_password_change")) != require_password_change:
                    user["require_password_change"] = require_password_change
                    changed = True
            elif "admin_permissions" in user:
                user.pop("admin_permissions", None)
                changed = True
            elif "require_password_change" in user:
                user.pop("require_password_change", None)
                changed = True
            if normalized_role != "admin" and "bootstrap_password_state" in user:
                user.pop("bootstrap_password_state", None)
                changed = True
            normalized_tenant = _normalize_user_tenant_for_role(
                normalized_role,
                user.get("tenant_id"),
                fallback=default_tenant,
            )
            if user.get("tenant_id") != normalized_tenant:
                user["tenant_id"] = normalized_tenant
                changed = True

        if changed:
            save_users(data)
        return data
    except Exception as e:
        logger.error(f"Failed to load users: {e}")
        return {"users": {}, "role_permissions": {}}

def save_users(users_data):
    """Save users to DB-first store with JSON fallback."""
    _prepare_users_payload_for_persistence(users_data)
    if _db_first_config_enabled():
        ok, err = _save_users_payload_to_db(users_data)
        if ok:
            return True
        logger.error("Failed to save users to DB, falling back to JSON: %s", err)
    try:
        with open('users.json', 'w') as f:
            json.dump(users_data, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save users: {e}")
        return False

def get_current_user():
    """Get current logged in user info"""
    if 'username' not in session:
        return None
    if has_request_context() and hasattr(g, '_cached_current_user'):
        cached = getattr(g, '_cached_current_user')
        return copy.deepcopy(cached) if isinstance(cached, dict) else None
    users_data = load_users()
    username = session.get('username')
    resolved_user = None
    if username in users_data.get('users', {}):
        user = users_data['users'][username].copy()
        user['username'] = username
        role = _normalize_user_role(user.get('role', 'viewer'))
        user['role'] = role
        user['display_role'] = _display_user_role(role)
        user['is_customer_user'] = _is_customer_role(role)
        user['tenant_id'] = _normalize_user_tenant_for_role(
            role,
            user.get('tenant_id'),
            fallback=_default_tenant_id(),
        )
        if role == "admin":
            user['admin_permissions'] = _normalize_admin_permissions(user.get("admin_permissions"))
        else:
            user['admin_permissions'] = {}
        user['require_password_change'] = bool(user.get('require_password_change'))
        user['active'] = bool(user.get('active', True))
        user['permissions'] = _resolve_role_permissions(users_data.get('role_permissions', {}), role)
        user['is_super_admin'] = _is_super_admin(user)
        resolved_user = user
    if has_request_context():
        setattr(g, '_cached_current_user', copy.deepcopy(resolved_user) if isinstance(resolved_user, dict) else None)
    return resolved_user

def get_user_permissions():
    """Get permissions for current user"""
    user = get_current_user()
    if user:
        return user.get('permissions', {})
    return {}

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Login required'}), 401
            return redirect(url_for('login'))
        user = get_current_user()
        if not user:
            session.clear()
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Login required'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def internal_portal_required(f):
    """Block customer portal users from internal technical pages and APIs."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Login required'}), 401
            return redirect(url_for('login'))
        if _is_customer_role(user.get('role', 'viewer')):
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Internal portal only'}), 403
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    """Decorator to require specific role(s)"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))
            if user.get('role') not in roles:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Insufficient permissions'}), 403
                return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def admin_scope_required(*scopes, any_scope=False):
    """Decorator for fine-grained admin scopes."""
    normalized_scopes = [str(scope or "").strip() for scope in scopes if str(scope or "").strip()]

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))
            if _normalize_user_role(user.get('role', 'viewer')) != 'admin':
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Admin role required'}), 403
                return redirect(url_for('index'))

            if normalized_scopes:
                checks = [_has_admin_scope(user, scope) for scope in normalized_scopes]
                authorized = any(checks) if any_scope else all(checks)
                if not authorized:
                    if request.path.startswith('/api/'):
                        return jsonify({
                            'error': 'Insufficient admin scope',
                            'required_scopes': normalized_scopes,
                        }), 403
                    return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def permission_required(permission):
    """Decorator to require specific permission"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))

            admin_scope_by_permission = {
                'can_edit_config': 'manage_configuration',
                'can_manage_certificates': 'manage_certificates',
                'can_update_system': 'manage_system',
            }
            if _normalize_user_role(user.get('role', 'viewer')) == 'admin':
                required_scope = admin_scope_by_permission.get(str(permission or ''))
                if required_scope and not _has_admin_scope(user, required_scope):
                    if request.path.startswith('/api/'):
                        return jsonify({'error': 'Insufficient admin scope'}), 403
                    return redirect(url_for('index'))

            perms = get_user_permissions()
            if not perms.get(permission, False):
                if request.path.startswith('/api/'):
                    return jsonify({'error': 'Insufficient permissions'}), 403
                return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors and return JSON"""
    app.logger.error(f"Internal server error: {error}")
    if request.path.startswith('/api/'):
        return jsonify({
            'error': 'Internal server error',
            'running': False,
            'service_type': 'web_ui',
            'tls_server': {'active': False},
            'mqtt_broker': {'active': False},
            'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []}
        }), 500
    return error

@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors for API endpoints"""
    if request.path.startswith('/api/'):
        return jsonify({'error': 'API endpoint not found'}), 404
    return error

@app.before_request
def ensure_json_api():
    """Apply API behavior defaults + auth hardening checks."""
    endpoint = str(request.endpoint or "")
    path = str(request.path or "")
    is_api = path.startswith('/api/')

    # Keep static and login assets outside auth timeout updates.
    is_static_like = endpoint == "static" or path.startswith("/static/")

    if not is_static_like:
        _ensure_viewer_demo_telemetry_seeded()

    # Session timeout enforcement.
    if not is_static_like and path not in ("/login",):
        if 'username' in session:
            now_ts = time.time()
            try:
                last_activity = float(session.get('_last_activity_ts') or 0.0)
            except (TypeError, ValueError):
                last_activity = 0.0
            if last_activity > 0 and (now_ts - last_activity) > _session_timeout_seconds:
                username = str(session.get('username') or 'unknown')
                session.clear()
                logger.info("Session expired for user '%s' (timeout=%ss)", username, int(_session_timeout_seconds))
                if is_api:
                    return (
                        jsonify({'error': 'Session expired. Please login again.'}),
                        401,
                        {'X-Session-Expired': '1'},
                    )
                return redirect(url_for('login', reason='timeout'))
            session.permanent = True
            session['_last_activity_ts'] = now_ts

    if not is_static_like and 'username' in session:
        user = get_current_user()
        if user and not bool(user.get('active', True)):
            session.clear()
            if is_api:
                return jsonify({'error': 'Account disabled'}), 401
            return redirect(url_for('login', reason='inactive'))
        if user and is_api and _is_customer_role(user.get('role', 'viewer')) and _is_customer_blocked_api_path(path):
            return jsonify({'error': 'Internal portal only'}), 403

    if not is_static_like and 'username' in session:
        setup_path = '/auth/setup-admin-password'
        allowed_paths = {setup_path, '/logout'}
        if path not in allowed_paths and not path.startswith('/ui/language/'):
            user = get_current_user()
            if user and bool(user.get('require_password_change')):
                if is_api:
                    return (
                        jsonify({
                            'error': _ui_text('auth.password_change_required_api', 'Password change required before continuing.'),
                            'redirect': setup_path,
                        }),
                        403,
                    )
                return redirect(setup_path)

    # Login rate-limit (POST only).
    if path == '/login' and request.method == 'POST' and bool(getattr(bssci_config, "AUTH_RATE_LIMIT_ENABLED", True)):
        ip_addr = _current_request_ip()
        allowed, retry_after = _check_login_rate_limit(ip_addr)
        if not allowed:
            message = f"Too many login attempts. Try again in {int(retry_after)}s."
            response = make_response(render_template('login.html', error=message), 429)
            response.headers['Retry-After'] = str(int(retry_after))
            return response

    # API rate-limit (per actor/IP).
    if is_api and bool(getattr(bssci_config, "AUTH_RATE_LIMIT_ENABLED", True)):
        actor = str(session.get('username') or 'anonymous')
        actor_key = f"{actor}@{_current_request_ip()}"
        allowed, retry_after = _check_api_rate_limit(actor_key)
        if not allowed:
            return (
                jsonify({
                    'error': 'Rate limit exceeded',
                    'retry_after_seconds': int(retry_after),
                }),
                429,
                {'Retry-After': str(int(retry_after))},
            )

        if not request.is_json and request.method in ['POST', 'PUT', 'PATCH']:
            # For non-JSON API writes, let route handlers decide explicit validation.
            pass

# Global variables for log storage and configuration
log_entries: List[Dict[str, Any]] = []
max_log_entries = 1000
web_log_handler = None
admin_audit_entries: List[Dict[str, Any]] = []
max_admin_audit_entries = max(200, int(getattr(bssci_config, "ADMIN_AUDIT_MAX_ENTRIES", 5000) or 5000))
admin_audit_log_file = os.path.join("logs", "admin_audit.jsonl")
_admin_audit_lock = threading.Lock()
_AUDIT_SENSITIVE_KEY_MARKERS = (
    "password",
    "token",
    "secret",
    "key",
    "authorization",
    "credential",
    "bearer",
    "cookie",
)


def _audit_scrub_value(value, depth=0):
    if depth > 4:
        return "[truncated]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        compact = value.strip()
        return compact if len(compact) <= 400 else f"{compact[:400]}...<truncated>"
    if isinstance(value, dict):
        result = {}
        for idx, (raw_key, raw_val) in enumerate(value.items()):
            if idx >= 80:
                result["__truncated__"] = f"{len(value) - 80} more keys"
                break
            key = str(raw_key)
            key_lower = key.lower()
            if any(marker in key_lower for marker in _AUDIT_SENSITIVE_KEY_MARKERS):
                result[key] = "***"
            else:
                result[key] = _audit_scrub_value(raw_val, depth + 1)
        return result
    if isinstance(value, (list, tuple, set)):
        seq = list(value)
        out = [_audit_scrub_value(item, depth + 1) for item in seq[:80]]
        if len(seq) > 80:
            out.append(f"...<{len(seq) - 80} more>")
        return out
    return str(value)


def _append_admin_audit_entry(entry):
    global admin_audit_entries
    with _admin_audit_lock:
        admin_audit_entries.append(entry)
        if len(admin_audit_entries) > max_admin_audit_entries:
            admin_audit_entries = admin_audit_entries[-max_admin_audit_entries:]
    if _db_first_config_enabled():
        ok, err = _append_admin_audit_entry_to_db(entry)
        if ok:
            return
        logger.warning("Failed to persist admin audit entry to DB, falling back to file: %s", err)
    try:
        os.makedirs(os.path.dirname(admin_audit_log_file), exist_ok=True)
        with open(admin_audit_log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception as exc:
        logger.warning("Failed to persist admin audit entry: %s", exc)


def _load_admin_audit_entries():
    global admin_audit_entries
    if _db_first_config_enabled():
        loaded, err = _load_admin_audit_entries_from_db(limit=max_admin_audit_entries)
        if isinstance(loaded, list):
            if not loaded and os.path.exists(admin_audit_log_file):
                legacy_loaded = []
                try:
                    with open(admin_audit_log_file, "r", encoding="utf-8") as f:
                        for raw_line in f:
                            line = raw_line.strip()
                            if not line:
                                continue
                            try:
                                item = json.loads(line)
                            except Exception:
                                continue
                            if isinstance(item, dict):
                                legacy_loaded.append(item)
                    for item in legacy_loaded[-max_admin_audit_entries:]:
                        _append_admin_audit_entry_to_db(item)
                    loaded, _ = _load_admin_audit_entries_from_db(limit=max_admin_audit_entries)
                except Exception as legacy_exc:
                    logger.warning("Failed to migrate legacy admin audit log into DB: %s", legacy_exc)
            admin_audit_entries = loaded[-max_admin_audit_entries:]
            return
        logger.warning("Failed to load admin audit log from DB, falling back to file: %s", err)
    if not os.path.exists(admin_audit_log_file):
        admin_audit_entries = []
        return
    loaded = []
    try:
        with open(admin_audit_log_file, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except Exception:
                    continue
                if isinstance(item, dict):
                    loaded.append(item)
    except Exception as exc:
        logger.warning("Failed to load admin audit log: %s", exc)
        loaded = []
    admin_audit_entries = loaded[-max_admin_audit_entries:]


def _current_request_ip():
    if not has_request_context():
        return ""
    forwarded = str(request.headers.get("X-Forwarded-For", "")).strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "")


def _record_admin_audit(action, entity, target_id="", status="success", details=None):
    actor = "system"
    role = "system"
    actor_tenant = _default_tenant_id()
    active_tenant = _default_tenant_id()
    method = ""
    path = ""
    ip_addr = ""
    user_agent = ""

    if has_request_context():
        actor = str(session.get("username") or "anonymous")
        role = str(session.get("role") or "")
        actor_tenant = _normalize_tenant_id(session.get("tenant_id"), fallback=_default_tenant_id())
        active_tenant = _active_tenant_id()
        method = str(request.method or "")
        path = str(request.path or "")
        ip_addr = _current_request_ip()
        user_agent = str(request.headers.get("User-Agent", "") or "")[:240]

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "action": str(action or "").strip() or "unknown",
        "entity": str(entity or "").strip() or "unknown",
        "target_id": str(target_id or "").strip(),
        "status": str(status or "success").strip().lower(),
        "actor": actor,
        "role": role,
        "actor_tenant": actor_tenant,
        "active_tenant": active_tenant,
        "method": method,
        "path": path,
        "ip": ip_addr,
        "user_agent": user_agent,
        "details": _audit_scrub_value(details or {}),
    }
    _append_admin_audit_entry(entry)

# Custom log handler to capture all logs with timezone support
class WebUILogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        # Use configured timezone
        self._update_timezone()
    
    def _update_timezone(self):
        """Update timezone from config"""
        try:
            import zoneinfo
            self.tz = zoneinfo.ZoneInfo(bssci_config.TIMEZONE)
            self.use_zoneinfo = True
        except Exception:
            # Fallback to UTC+1 (CET)
            self.tz = timezone(timedelta(hours=1))
            self.use_zoneinfo = False

    def emit(self, record):
        global log_entries

        # Filter out noisy web request logs to reduce clutter
        if record.name == 'werkzeug' and any(x in record.getMessage() for x in [
            'GET /api/', 'GET /logs', 'GET /sensors', 'GET /config', 'GET /administration', 'GET /', 'GET /static/'
        ]):
            return  # Skip web request logs

        # Convert UTC timestamp to local timezone
        utc_time = datetime.fromtimestamp(record.created, tz=timezone.utc)
        local_time = utc_time.astimezone(self.tz)
        current_time = local_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        message = record.getMessage()

        # Check if this exact message was logged in the last second (duplicate detection)
        if log_entries:
            last_entry = log_entries[-1]
            try:
                last_time = datetime.strptime(last_entry['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
                time_diff = abs((local_time.replace(tzinfo=None) - last_time).total_seconds())

                if (time_diff < 1.0 and  # Within 1 second
                    last_entry['message'] == message and
                    last_entry['logger'] == record.name):
                    return  # Skip duplicate message
            except:
                pass  # If timestamp parsing fails, continue with logging

        log_entry = {
            'timestamp': current_time,
            'level': record.levelname,
            'logger': record.name,
            'message': message,
            'source': 'memory'
        }
        log_entries.append(log_entry)

        # Keep only the last max_log_entries
        if len(log_entries) > max_log_entries:
            log_entries = log_entries[-max_log_entries:]

def ensure_web_log_handler():
    """Ensure in-memory web log handler is attached even after logger reconfiguration."""
    global web_log_handler

    root_logger = logging.getLogger()
    existing_handler = next((h for h in root_logger.handlers if isinstance(h, WebUILogHandler)), None)

    if existing_handler is None:
        web_log_handler = WebUILogHandler()
        root_logger.addHandler(web_log_handler)
    else:
        web_log_handler = existing_handler

    # Keep visibility for INFO/DEBUG logs in the web logs page.
    if root_logger.level > logging.DEBUG:
        root_logger.setLevel(logging.DEBUG)

    logging.getLogger('TLSServer').setLevel(logging.DEBUG)
    logging.getLogger('mqtt_interface').setLevel(logging.DEBUG)


# Attach handler at import time; call again from runtime paths after any logging reset.
ensure_web_log_handler()
_load_admin_audit_entries()

@app.context_processor
def inject_user():
    """Inject user info into all templates"""
    user = get_current_user()
    app_language = _get_app_language()
    app_locale = _get_app_locale()
    role_label = ''
    role_slug = 'guest'
    if user:
        role_slug = 'customer' if user.get('is_customer_user') else user.get('role', 'guest')
        role_label = (
            _ui_text('auth.role_admin', 'Admin')
            if user.get('display_role') == 'admin'
            else _ui_text('auth.role_customer', 'Customer')
        )
    return {
        'current_user': user,
        'user_permissions': user.get('permissions', {}) if user else {},
        'visible_tabs': user.get('permissions', {}).get('visible_tabs', []) if user else [],
        'active_tenant_id': _active_tenant_id() if user else _default_tenant_id(),
        'is_customer_user': bool(user and user.get('is_customer_user')),
        'current_user_role_slug': role_slug,
        'current_user_role_label': role_label,
        'oms_enabled': bool(getattr(bssci_config, 'OMS_ENABLED', True)),
        'mqtt_ui_enabled': bool(getattr(bssci_config, 'MQTT_UI_ENABLED', True)),
        'app_timezone': str(getattr(bssci_config, 'TIMEZONE', 'Europe/Berlin') or 'Europe/Berlin'),
        'app_language': app_language,
        'app_locale': app_locale,
        'app_languages': {key: dict(value) for key, value in _APP_LANGUAGE_OPTIONS.items()},
        'ui_translations': dict(_UI_TRANSLATIONS.get(app_language, {})),
        'app_deployment_mode': 'production',
        'is_production_deployment': True,
        'bootstrap_login_account_hints': _bootstrap_login_account_hints(),
        't': _ui_text,
        'translate_page_title': _translate_page_title,
    }

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    requested_lang = _normalize_app_language(request.values.get('ui_language') or request.args.get('lang') or '')
    if requested_lang in _APP_LANGUAGE_OPTIONS:
        session['ui_language_override'] = requested_lang
    if request.method == 'GET' and request.args.get('reason') == 'timeout':
        error = _ui_text('login.error.timeout', 'Session expired due to inactivity. Please sign in again.')
    if request.method == 'GET' and request.args.get('reason') == 'inactive':
        error = _ui_text('login.error.account_disabled', 'This account is disabled.')
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        ip_addr = _current_request_ip()
        users_data = load_users()
        user = users_data.get('users', {}).get(username)
        if user and not bool(user.get('active', True)):
            _register_login_failure(ip_addr)
            error = _ui_text('login.error.account_disabled', 'This account is disabled.')
            return render_template('login.html', error=error)
        if user and _verify_password_value(user.get('password'), password):
            _maybe_upgrade_legacy_password_after_login(users_data, username, password)
            session['username'] = username
            session['role'] = _normalize_user_role(user.get('role', 'viewer'))
            session['tenant_id'] = _normalize_user_tenant_for_role(
                session['role'],
                user.get('tenant_id'),
                fallback=_default_tenant_id(),
            )
            session.permanent = True
            session['_last_activity_ts'] = time.time()
            _register_login_success(ip_addr)
            logger.info(f"User '{username}' logged in")
            if _should_force_initial_admin_password_change(user):
                return redirect(url_for('setup_admin_password'))
            return redirect(url_for('index'))
        _register_login_failure(ip_addr)
        error = _ui_text('login.error.invalid_credentials', 'Invalid username or password')
    return render_template('login.html', error=error)

@app.route('/auth/setup-admin-password', methods=['GET', 'POST'])
@login_required
def setup_admin_password():
    user = get_current_user()
    if not user:
        return redirect(url_for('login'))
    if not bool(user.get('require_password_change')):
        return redirect(url_for('index'))
    if _normalize_user_role(user.get('role', 'viewer')) != 'admin':
        return redirect(url_for('index'))

    error = None
    if request.method == 'POST':
        new_password = str(request.form.get('password') or '')
        confirm_password = str(request.form.get('password_confirm') or '')
        if len(new_password) < 8:
            error = _ui_text('auth.password_change_too_short', 'Nové heslo musí mať aspoň 8 znakov.')
        elif new_password != confirm_password:
            error = _ui_text('auth.password_change_mismatch', 'Zadané heslá sa nezhodujú.')
        elif new_password == _bootstrap_admin_default_password():
            error = _ui_text('auth.password_change_same_as_default', 'Nové heslo sa musí líšiť od predvoleného bootstrap hesla.')
        else:
            users_data = load_users()
            username = str(session.get('username') or '')
            user_row = users_data.get('users', {}).get(username)
            if not isinstance(user_row, dict):
                session.clear()
                return redirect(url_for('login'))
            user_row['password'] = new_password
            user_row['require_password_change'] = False
            user_row['bootstrap_password_state'] = 'rotated'
            if not save_users(users_data):
                error = _ui_text('auth.password_change_save_failed', 'Nové heslo sa nepodarilo uložiť. Skúste to znova.')
            else:
                session['_last_activity_ts'] = time.time()
                return redirect(url_for('index'))

    return render_template('force_password_change.html', error=error)


@app.route('/ui/language/<lang>')
def set_ui_language(lang):
    normalized = _normalize_app_language(lang)
    if normalized in _APP_LANGUAGE_OPTIONS:
        session['ui_language_override'] = normalized
    next_url = request.args.get('next', '').strip()
    if next_url and next_url.startswith('/') and not next_url.startswith('//'):
        return redirect(next_url)
    referrer = request.headers.get('Referer', '').strip()
    if referrer:
        try:
            parsed = urllib.parse.urlparse(referrer)
            if parsed.path:
                target = parsed.path
                if parsed.query:
                    target = f"{target}?{parsed.query}"
                return redirect(target)
        except Exception:
            pass
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    username = session.get('username', 'Unknown')
    session.clear()
    logger.info(f"User '{username}' logged out")
    return redirect(url_for('login'))

@app.route('/api/users', methods=['GET', 'POST', 'PUT', 'DELETE'])
@login_required
@admin_scope_required('manage_users')
def api_users():
    """Manage users (admin only)"""
    users_data = load_users()
    actor_user = get_current_user() or {}
    
    if request.method == 'GET':
        users_list = []
        legacy_default_user_present = False
        storage_meta = _users_storage_backend_meta()
        for username, data in users_data.get('users', {}).items():
            role = _normalize_user_role(data.get('role', 'viewer'))
            normalized_tenant = _normalize_user_tenant_for_role(
                role,
                data.get('tenant_id'),
                fallback=_default_tenant_id(),
            )
            if role != 'admin' and _is_reserved_default_tenant(normalized_tenant):
                legacy_default_user_present = True
            super_admin = role == 'admin' and _is_super_admin({
                'role': role,
                'tenant_id': data.get('tenant_id'),
            })
            users_list.append({
                'username': username,
                'name': data.get('name', ''),
                'role': _visible_role_name(role),
                'role_canonical': role,
                'active': bool(data.get('active', True)),
                'can_deactivate': username != session.get('username'),
                'can_delete': username != session.get('username'),
                'tenant_id': normalized_tenant,
                'tenant_scope_label': (
                    "Super admin"
                    if super_admin
                    else ("Rezervovaný scope" if _is_reserved_default_tenant(normalized_tenant) else normalized_tenant)
                ),
                'is_super_admin': super_admin,
                'admin_permissions': _normalize_admin_permissions(data.get('admin_permissions')) if role == 'admin' else {},
            })
        known_tenant_ids = set()
        try:
            registry = load_tenant_registry()
            for row in (registry.get('tenants', []) if isinstance(registry, dict) else []):
                tenant_id = _sanitize_tenant_id((row or {}).get('tenant_id') if isinstance(row, dict) else '')
                if tenant_id and not _is_reserved_default_tenant(tenant_id):
                    known_tenant_ids.add(tenant_id)
        except Exception:
            pass
        for _, data in users_data.get('users', {}).items():
            if not isinstance(data, dict):
                continue
            role = _normalize_user_role(data.get('role', 'viewer'))
            if role == 'admin':
                continue
            normalized_tenant = _normalize_user_tenant_for_role(role, data.get('tenant_id'), fallback=_default_tenant_id())
            if _is_reserved_default_tenant(normalized_tenant):
                legacy_default_user_present = True
            else:
                known_tenant_ids.add(normalized_tenant)
        return jsonify({
            'users': users_list,
            'roles': _visible_role_choices(users_data.get('role_permissions', {})),
            'default_tenant': _default_tenant_id(),
            'default_tenant_label': 'Super admin',
            'legacy_default_user_present': legacy_default_user_present,
            'tenant_choices': sorted(known_tenant_ids),
            'storage_backend': storage_meta.get('backend', 'json'),
            'storage_db_enabled': bool(storage_meta.get('db_enabled', False)),
            'bootstrap_state': storage_meta.get('bootstrap_state', {}),
            'seed_defaults_file': storage_meta.get('seed_defaults_file', USER_SEED_FILE),
            'recovery_file': storage_meta.get('recovery_file', USERS_RECOVERY_FILE),
            'admin_scopes': [
                {
                    'id': scope_id,
                    'label': meta.get('label', scope_id),
                    'description': meta.get('description', ''),
                    'default_enabled': bool(meta.get('default', True)),
                }
                for scope_id, meta in ADMIN_SCOPE_DEFINITIONS.items()
            ],
            'current_admin_permissions': _normalize_admin_permissions(actor_user.get('admin_permissions')),
        })
    
    elif request.method == 'POST':
        data = request.get_json()
        username = data.get('username', '').strip()
        if not username or username in users_data.get('users', {}):
            return jsonify({'success': False, 'error': 'Username is invalid or already exists'}), 400
        role = _normalize_user_role(data.get('role', 'viewer'))
        tenant_id = _normalize_user_tenant_for_role(
            role,
            data.get('tenant_id'),
            fallback=_default_tenant_id(),
        )
        if role != 'admin' and _is_reserved_default_tenant(tenant_id):
            return jsonify({'success': False, 'error': 'Reserved super admin scope is not available for tenant users'}), 400
        users_data['users'][username] = {
            'password': data.get('password', 'password123'),
            'role': role,
            'name': data.get('name', username),
            'tenant_id': tenant_id,
            'active': bool(data.get('active', True)),
        }
        if role == 'admin':
            users_data['users'][username]['admin_permissions'] = _apply_grantable_admin_permissions(
                actor_user,
                data.get('admin_permissions'),
                existing_permissions=None,
            )
        if tenant_id:
            _upsert_tenant_registry_entry(tenant_id)
        save_users(users_data)
        _record_admin_audit(
            action='user.create',
            entity='user',
            target_id=username,
            status='success',
            details={
                'role': role,
                'tenant_id': tenant_id,
                'name': data.get('name', username),
                'admin_permissions': users_data['users'][username].get('admin_permissions', {}) if role == 'admin' else {},
            },
        )
        return jsonify({'success': True})
    
    elif request.method == 'PUT':
        data = request.get_json()
        username = data.get('username')
        if username not in users_data.get('users', {}):
            return jsonify({'success': False, 'error': 'User not found'}), 404
        user_row = users_data['users'][username]
        if 'password' in data and data['password']:
            user_row['password'] = data['password']
        if 'role' in data:
            user_row['role'] = _normalize_user_role(data['role'])
        if 'name' in data:
            user_row['name'] = data['name']
        if 'active' in data:
            next_active = bool(data.get('active'))
            if username == session.get('username') and not next_active:
                return jsonify({'success': False, 'error': _ui_text('admin.cannot_deactivate_self', 'You cannot deactivate your own account.')}), 400
            user_row['active'] = next_active
        if 'tenant_id' in data or 'role' in data:
            tenant_source = data.get('tenant_id') if 'tenant_id' in data else user_row.get('tenant_id')
            tenant_id = _normalize_user_tenant_for_role(
                user_row.get('role', 'viewer'),
                tenant_source,
                fallback=_default_tenant_id(),
            )
            previous_tenant = _normalize_user_tenant_for_role(
                user_row.get('role', 'viewer'),
                user_row.get('tenant_id'),
                fallback=_default_tenant_id(),
            )
            if (
                _normalize_user_role(user_row.get('role', 'viewer')) != 'admin'
                and _is_reserved_default_tenant(tenant_id)
                and not _is_reserved_default_tenant(previous_tenant)
            ):
                return jsonify({'success': False, 'error': 'Reserved super admin scope is not available for tenant users'}), 400
            user_row['tenant_id'] = tenant_id
            if tenant_id:
                _upsert_tenant_registry_entry(tenant_id)
        if _normalize_user_role(user_row.get('role', 'viewer')) == 'admin':
            if 'admin_permissions' in data:
                user_row['admin_permissions'] = _apply_grantable_admin_permissions(
                    actor_user,
                    data.get('admin_permissions'),
                    existing_permissions=user_row.get('admin_permissions'),
                )
            else:
                user_row['admin_permissions'] = _normalize_admin_permissions(user_row.get('admin_permissions'))
            if username == session.get('username') and not bool(user_row['admin_permissions'].get('manage_users', False)):
                return jsonify({
                    'success': False,
                    'error': 'Cannot remove manage_users scope from your own admin account',
                }), 400
        else:
            user_row.pop('admin_permissions', None)
        save_users(users_data)
        if username == session.get('username'):
            session['role'] = _normalize_user_role(user_row.get('role', 'viewer'))
            session['tenant_id'] = _normalize_user_tenant_for_role(
                session['role'],
                user_row.get('tenant_id'),
                fallback=_default_tenant_id(),
            )
        _record_admin_audit(
            action='user.update',
            entity='user',
            target_id=username,
            status='success',
            details={
                'role': user_row.get('role', 'viewer'),
                'tenant_id': user_row.get('tenant_id', _default_tenant_id()),
                'name': user_row.get('name', ''),
                'active': bool(user_row.get('active', True)),
                'password_changed': bool(data.get('password')),
                'admin_permissions': user_row.get('admin_permissions', {}) if _normalize_user_role(user_row.get('role', 'viewer')) == 'admin' else {},
            },
        )
        return jsonify({'success': True})
    
    elif request.method == 'DELETE':
        data = request.get_json()
        username = data.get('username')
        if username == session.get('username'):
            return jsonify({'success': False, 'error': _ui_text('admin.cannot_delete_self', 'You cannot delete your own account.')}), 400
        if username in users_data.get('users', {}):
            del users_data['users'][username]
            save_users(users_data)
            _record_admin_audit(
                action='user.delete',
                entity='user',
                target_id=username,
                status='success',
                details={},
            )
        return jsonify({'success': True})


@app.route('/api/users/export', methods=['GET'])
@login_required
@admin_scope_required('manage_users')
def export_users_admin():
    users_data = load_users()
    payload_users = []
    for username, user in sorted((users_data.get('users', {}) or {}).items(), key=lambda item: str(item[0]).lower()):
        record = _serialize_user_for_export(username, user)
        if record:
            payload_users.append(record)
    storage_meta = _users_storage_backend_meta()
    payload = {
        "format_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "storage_backend": storage_meta.get("backend", "json"),
        "seed_defaults_file": storage_meta.get("seed_defaults_file", USER_SEED_FILE),
        "recovery_file": storage_meta.get("recovery_file", USERS_RECOVERY_FILE),
        "users": payload_users,
    }
    _record_admin_audit(
        action='user.export',
        entity='user',
        target_id='all_users',
        status='success',
        details={'count': len(payload_users)},
    )
    filename = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return Response(
        json.dumps(payload, indent=2, ensure_ascii=True),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


@app.route('/api/users/import', methods=['POST'])
@login_required
@admin_scope_required('manage_users')
def import_users_admin():
    actor_user = get_current_user() or {}
    current_username = str(session.get('username') or '').strip()
    try:
        payload = _parse_json_upload_or_payload()
    except ValueError as exc:
        _record_admin_audit(
            action='user.import',
            entity='user',
            target_id='all_users',
            status='error',
            details={'error': str(exc)},
        )
        return jsonify({'success': False, 'error': str(exc)}), 400

    imported = payload.get("data", payload) if isinstance(payload, dict) else {}
    users_in = imported.get("users", []) if isinstance(imported, dict) else []
    if not isinstance(users_in, list):
        return jsonify({'success': False, 'error': 'Import payload must contain a users list.'}), 400

    users_data = load_users()
    users_map = users_data.get('users', {}) if isinstance(users_data, dict) else {}
    created = 0
    updated = 0
    skipped = []
    generated_passwords = []

    for entry in users_in:
        if not isinstance(entry, dict):
            skipped.append({'reason': 'invalid_record'})
            continue
        username = str(entry.get('username') or '').strip()
        if not username:
            skipped.append({'reason': 'missing_username'})
            continue
        if username == current_username:
            skipped.append({'username': username, 'reason': 'self_update_blocked'})
            continue

        existing = users_map.get(username)
        role = _normalize_user_role(entry.get('role', (existing or {}).get('role', 'customer')))
        tenant_id = _normalize_user_tenant_for_role(
            role,
            entry.get('tenant_id', (existing or {}).get('tenant_id')),
            fallback=_default_tenant_id(),
        )
        if role != 'admin' and _is_reserved_default_tenant(tenant_id):
            skipped.append({'username': username, 'reason': 'reserved_tenant_scope'})
            continue

        password = str(entry.get('password') or '').strip()
        generated_password = ''
        if existing:
            password_to_store = password or str(existing.get('password') or '')
        else:
            if not password:
                generated_password = _generate_temporary_password()
                password = generated_password
            password_to_store = password

        user_row = {
            'password': password_to_store,
            'role': role,
            'name': str(entry.get('name') or (existing or {}).get('name') or username).strip() or username,
            'tenant_id': tenant_id,
            'active': bool(entry.get('active', (existing or {}).get('active', True))),
        }
        if role == 'admin':
            user_row['admin_permissions'] = _apply_grantable_admin_permissions(
                actor_user,
                entry.get('admin_permissions'),
                existing_permissions=(existing or {}).get('admin_permissions') if isinstance(existing, dict) else None,
            )
            user_row['require_password_change'] = bool((existing or {}).get('require_password_change', False))
        elif existing and 'require_password_change' in existing:
            user_row['require_password_change'] = bool(existing.get('require_password_change'))

        users_map[username] = user_row
        if tenant_id:
            _upsert_tenant_registry_entry(tenant_id)

        if existing:
            updated += 1
        else:
            created += 1
            if generated_password:
                generated_passwords.append({'username': username, 'password': generated_password})

    users_data['users'] = users_map
    if not save_users(users_data):
        return jsonify({'success': False, 'error': 'Failed to persist imported users.'}), 500

    _record_admin_audit(
        action='user.import',
        entity='user',
        target_id='all_users',
        status='success',
        details={
            'created': created,
            'updated': updated,
            'skipped': len(skipped),
            'generated_passwords': len(generated_passwords),
        },
    )
    return jsonify({
        'success': True,
        'created': created,
        'updated': updated,
        'skipped': skipped,
        'generated_passwords': generated_passwords,
    })

@app.route('/api/tenants', methods=['GET', 'POST', 'PUT', 'DELETE'])
@login_required
@admin_scope_required('manage_tenants')
def api_tenants():
    """List and manage tenant metadata."""
    tenant_ids = set()
    default_tenant = _default_tenant_id()
    tenant_ids.add(default_tenant)
    all_sensors = _load_all_sensors()
    registry_map = _tenant_registry_map()
    tenant_storage_meta = _tenant_storage_backend_meta()
    tenant_ids.update(registry_map.keys())
    db_tenant_meta = {}
    db_tenant_meta_error = None
    if _db_first_config_enabled():
        db_tenant_meta, db_tenant_meta_error = _load_tenant_usage_meta_from_db()
        if isinstance(db_tenant_meta, dict):
            tenant_ids.update(db_tenant_meta.keys())
            for tenant_id, meta in db_tenant_meta.items():
                existing = registry_map.get(tenant_id, {})
                registry_map[tenant_id] = {
                    "id": tenant_id,
                    "name": str(existing.get("name") or meta.get("name") or tenant_id).strip() or tenant_id,
                    "description": str(existing.get("description") or meta.get("description") or "").strip(),
                    "created_at": existing.get("created_at") or meta.get("created_at") or "",
                }
        else:
            db_tenant_meta = {}

    users_data = load_users()
    users_map = users_data.get("users", {}) if isinstance(users_data, dict) else {}
    bs_config = load_base_station_config().get("base_stations", {}) or {}

    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        tenant_id = _sanitize_tenant_id(data.get("tenant_id") or data.get("id"))
        if not tenant_id:
            return jsonify({"success": False, "error": "Tenant ID is required"}), 400
        if _is_reserved_default_tenant(tenant_id):
            return jsonify({"success": False, "error": "Reserved super admin scope cannot be created or modified here"}), 400

        known_ids = set(registry_map.keys())
        for user in users_map.values():
            if isinstance(user, dict):
                role = _normalize_user_role(user.get("role", "viewer"))
                if role != "admin":
                    known_ids.add(_normalize_user_tenant_for_role(role, user.get("tenant_id"), fallback=default_tenant))
        for sensor in all_sensors:
            if isinstance(sensor, dict):
                known_ids.add(_tenant_id_from_sensor(sensor))
        for bs_data in bs_config.values():
            if isinstance(bs_data, dict):
                known_ids.add(_tenant_id_from_base_station(bs_data))

        if tenant_id in known_ids:
            return jsonify({"success": False, "error": f"Tenant '{tenant_id}' already exists"}), 400

        tenant_name = str(data.get("name") or tenant_id).strip()[:120]
        tenant_description = str(data.get("description") or "").strip()[:240]
        requested_base_stations = _normalize_base_station_route_list(data.get("base_station_euis", []))
        ok, err, tenant_entry = _upsert_tenant_registry_entry(
            tenant_id=tenant_id,
            name=tenant_name,
            description=tenant_description,
        )
        if not ok:
            return jsonify({"success": False, "error": err or "Failed to create tenant"}), 500

        bs_assignment = _reassign_base_stations_for_tenant(tenant_id, requested_base_stations)

        conn, conn_err = _timescale_connect()
        timescale_result = {"synced": False, "error": conn_err}
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                        """,
                        (tenant_id, tenant_name),
                    )
                timescale_result = {"synced": True, "error": None}
            except Exception as exc:
                timescale_result = {"synced": False, "error": str(exc)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        _record_admin_audit(
            action='tenant.create',
            entity='tenant',
            target_id=tenant_id,
            status='success',
            details={
                'name': tenant_name,
                'description': tenant_description,
                'base_station_count': len(bs_assignment.get('assigned', [])),
                'base_stations_assigned': bs_assignment.get('assigned', []),
                'timescale_synced': bool(timescale_result.get('synced')),
                'timescale_error': timescale_result.get('error'),
            },
        )
        return jsonify({
            "success": True,
            "tenant": tenant_entry,
            "base_station_assignment": bs_assignment,
            "timescale": timescale_result,
        })

    if request.method == "PUT":
        data = request.get_json(silent=True) or {}
        tenant_id = _sanitize_tenant_id(data.get("tenant_id") or data.get("id"))
        if not tenant_id:
            return jsonify({"success": False, "error": "Tenant ID is required"}), 400
        if _is_reserved_default_tenant(tenant_id):
            return jsonify({"success": False, "error": "Reserved super admin scope cannot be created or modified here"}), 400

        tenant_name = str(data.get("name") or tenant_id).strip()[:120]
        tenant_description = str(data.get("description") or "").strip()[:240]
        requested_base_stations = _normalize_base_station_route_list(data.get("base_station_euis", []))
        ok, err, tenant_entry = _upsert_tenant_registry_entry(
            tenant_id=tenant_id,
            name=tenant_name,
            description=tenant_description,
        )
        if not ok:
            return jsonify({"success": False, "error": err or "Failed to update tenant"}), 500

        bs_assignment = _reassign_base_stations_for_tenant(tenant_id, requested_base_stations)

        conn, conn_err = _timescale_connect()
        timescale_result = {"synced": False, "error": conn_err}
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                        """,
                        (tenant_id, tenant_name),
                    )
                timescale_result = {"synced": True, "error": None}
            except Exception as exc:
                timescale_result = {"synced": False, "error": str(exc)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        _record_admin_audit(
            action='tenant.update',
            entity='tenant',
            target_id=tenant_id,
            status='success',
            details={
                'name': tenant_name,
                'description': tenant_description,
                'base_station_count': len(bs_assignment.get('assigned', [])),
                'base_stations_assigned': bs_assignment.get('assigned', []),
                'base_stations_released': bs_assignment.get('released', []),
                'timescale_synced': bool(timescale_result.get('synced')),
                'timescale_error': timescale_result.get('error'),
            },
        )
        return jsonify({
            "success": True,
            "tenant": tenant_entry,
            "base_station_assignment": bs_assignment,
            "timescale": timescale_result,
        })

    if request.method == "DELETE":
        data = request.get_json(silent=True) or {}
        tenant_id = _sanitize_tenant_id(
            data.get("tenant_id")
            or data.get("id")
            or request.args.get("tenant_id")
        )
        force_delete = _parse_bool_arg(data.get("force") if isinstance(data, dict) else None, default=False) or _parse_bool_arg(request.args.get("force"), default=False)
        if not tenant_id:
            return jsonify({"success": False, "error": "Tenant ID is required"}), 400
        if tenant_id == default_tenant:
            return jsonify({"success": False, "error": "Reserved super admin scope cannot be deleted"}), 400

        user_count = sum(
            1
            for _, user in users_map.items()
            if _user_belongs_to_tenant(user, tenant_id, fallback=default_tenant)
        )
        sensor_count = sum(1 for s in all_sensors if isinstance(s, dict) and _tenant_matches(_tenant_id_from_sensor(s), tenant_id))
        base_station_count = sum(
            1
            for _, bs_data in (bs_config or {}).items()
            if isinstance(bs_data, dict) and _tenant_matches(_tenant_id_from_base_station(bs_data), tenant_id)
        )

        ts_counts = {"inventory_events": 0, "telemetry_points": 0}
        conn, conn_err = _timescale_connect()
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM inventory_events WHERE tenant_id = %s", (tenant_id,))
                    ts_counts["inventory_events"] = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute("SELECT COUNT(*) FROM telemetry_uplink WHERE tenant_id = %s", (tenant_id,))
                    ts_counts["telemetry_points"] = int((cur.fetchone() or [0])[0] or 0)
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        has_usage = any([
            user_count > 0,
            sensor_count > 0,
            base_station_count > 0,
            ts_counts["inventory_events"] > 0,
            ts_counts["telemetry_points"] > 0,
        ])
        if has_usage and not force_delete:
            return jsonify({
                "success": False,
                "error": "Tenant contains assigned data. Use force=true to purge historical data.",
                "usage": {
                    "users": user_count,
                    "sensors": sensor_count,
                    "base_stations": base_station_count,
                    "inventory_events": ts_counts["inventory_events"],
                    "telemetry_points": ts_counts["telemetry_points"],
                },
            }), 400

        registry = load_tenant_registry()
        registry_entries = [
            item for item in registry.get("tenants", [])
            if isinstance(item, dict) and _sanitize_tenant_id(item.get("id")) != tenant_id
        ]
        registry["tenants"] = registry_entries
        save_tenant_registry(registry)

        conn, conn_err = _timescale_connect()
        timescale_result = {"purged": False, "error": conn_err}
        if conn is not None:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    if force_delete:
                        cur.execute("DELETE FROM telemetry_uplink WHERE tenant_id = %s", (tenant_id,))
                        cur.execute("DELETE FROM inventory_snapshot_latest WHERE tenant_id = %s", (tenant_id,))
                        cur.execute("DELETE FROM inventory_snapshot_points WHERE tenant_id = %s", (tenant_id,))
                        cur.execute("DELETE FROM inventory_events WHERE tenant_id = %s", (tenant_id,))
                    cur.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))
                timescale_result = {"purged": bool(force_delete), "error": None}
            except Exception as exc:
                timescale_result = {"purged": False, "error": str(exc)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        _record_admin_audit(
            action='tenant.delete',
            entity='tenant',
            target_id=tenant_id,
            status='success',
            details={
                'force': bool(force_delete),
                'users': user_count,
                'sensors': sensor_count,
                'base_stations': base_station_count,
                'inventory_events': ts_counts.get('inventory_events', 0),
                'telemetry_points': ts_counts.get('telemetry_points', 0),
                'timescale_purged': bool(timescale_result.get('purged')),
                'timescale_error': timescale_result.get('error'),
            },
        )
        return jsonify({
            "success": True,
            "tenant_id": tenant_id,
            "timescale": timescale_result,
        })

    for user in users_data.get("users", {}).values():
        if isinstance(user, dict):
            role = _normalize_user_role(user.get("role", "viewer"))
            if role == "admin":
                continue
            tenant_ids.add(_normalize_user_tenant_for_role(role, user.get("tenant_id"), fallback=default_tenant))

    for sensor in all_sensors:
        if isinstance(sensor, dict):
            tenant_ids.add(_tenant_id_from_sensor(sensor))

    for bs_data in bs_config.values():
        if isinstance(bs_data, dict):
            tenant_ids.add(_tenant_id_from_base_station(bs_data))

    timescale = {"enabled": bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)), "tenants": []}
    timescale_meta = {}
    if isinstance(db_tenant_meta, dict) and db_tenant_meta:
        for tenant_id, meta_entry in sorted(db_tenant_meta.items()):
            timescale_meta[tenant_id] = {
                "id": tenant_id,
                "name": str(meta_entry.get("name") or tenant_id).strip() or tenant_id,
                "created_at": meta_entry.get("created_at"),
                "inventory_events": int(meta_entry.get("inventory_events") or 0),
                "telemetry_points": int(meta_entry.get("telemetry_points") or 0),
                "user_count": int(meta_entry.get("user_count") or 0),
            }
            timescale["tenants"].append(timescale_meta[tenant_id])
    elif db_tenant_meta_error:
        timescale["error"] = db_tenant_meta_error

    tenant_summaries = []
    sorted_tenants = sorted(tenant_ids)
    for tenant_id in sorted_tenants:
        sensor_count = sum(1 for s in all_sensors if isinstance(s, dict) and _tenant_matches(_tenant_id_from_sensor(s), tenant_id))
        base_station_count = sum(
            1
            for _, bs_data in (bs_config or {}).items()
            if isinstance(bs_data, dict) and _tenant_matches(_tenant_id_from_base_station(bs_data), tenant_id)
        )
        user_count = int((timescale_meta.get(tenant_id) or {}).get("user_count") or 0)
        if not user_count:
            user_count = sum(
                1
                for _, user in users_data.get("users", {}).items()
                if _user_belongs_to_tenant(user, tenant_id, fallback=default_tenant)
            )
        registry_entry = registry_map.get(tenant_id, {})
        ts_entry = timescale_meta.get(tenant_id, {})
        tenant_name = str(
            registry_entry.get("name")
            or ts_entry.get("name")
            or tenant_id
        ).strip() or tenant_id
        tenant_name = _tenant_scope_display_name(tenant_id, tenant_name)
        tenant_description = _tenant_scope_display_description(tenant_id, registry_entry.get("description"))
        created_at = (
            registry_entry.get("created_at")
            or ts_entry.get("created_at")
            or None
        )
        tenant_summaries.append({
            "tenant_id": tenant_id,
            "display_name": tenant_name,
            "name": tenant_name,
            "description": tenant_description,
            "created_at": created_at,
            "sensor_count": sensor_count,
            "base_station_count": base_station_count,
            "assigned_base_station_euis": sorted(
                str(eui).strip().upper()
                for eui, bs_data in (bs_config or {}).items()
                if isinstance(bs_data, dict) and _tenant_matches(_tenant_id_from_base_station(bs_data), tenant_id)
            ),
            "user_count": user_count,
            "is_reserved": _is_reserved_default_tenant(tenant_id),
            "can_edit": not _is_reserved_default_tenant(tenant_id),
            "can_delete": (
                tenant_id != default_tenant
                and user_count == 0
                and sensor_count == 0
                and base_station_count == 0
                and int(ts_entry.get("inventory_events") or 0) == 0
                and int(ts_entry.get("telemetry_points") or 0) == 0
            ),
        })

    return jsonify({
        "success": True,
        "default_tenant": default_tenant,
        "default_tenant_label": "Super admin",
        "storage_backend": tenant_storage_meta.get("backend", "json"),
        "storage_db_enabled": bool(tenant_storage_meta.get("db_enabled", False)),
        "bootstrap_state": tenant_storage_meta.get("bootstrap_state", {}),
        "seed_defaults_file": tenant_storage_meta.get("seed_defaults_file", TENANT_REGISTRY_FILE),
        "recovery_file": tenant_storage_meta.get("recovery_file", TENANT_REGISTRY_FILE),
        "tenants": tenant_summaries,
        "base_stations_catalog": [
            {
                "eui": str(eui).strip().upper(),
                "name": str((bs_data or {}).get("name") or "").strip(),
                "tenant_id": _tenant_id_from_base_station(bs_data),
                "configured_ip": str((bs_data or {}).get("ip") or "").strip(),
                "tags": list((bs_data or {}).get("tags") or []),
            }
            for eui, bs_data in sorted((bs_config or {}).items(), key=lambda item: ((item[1] or {}).get("name", ""), item[0]))
            if isinstance(bs_data, dict)
        ],
        "timescale": timescale,
    })


@app.route('/api/tenants/metadata/export', methods=['GET'])
@login_required
@admin_scope_required('manage_tenants')
def export_tenant_metadata():
    registry_map = _tenant_registry_map()
    tenants = []
    for tenant_id, entry in sorted(registry_map.items()):
        if _is_reserved_default_tenant(tenant_id):
            continue
        tenants.append({
            'id': tenant_id,
            'name': str(entry.get('name') or tenant_id).strip() or tenant_id,
            'description': str(entry.get('description') or '').strip(),
            'created_at': str(entry.get('created_at') or ''),
        })
    storage_meta = _tenant_storage_backend_meta()
    payload = {
        'format_version': 1,
        'exported_at': datetime.now(timezone.utc).isoformat(),
        'storage_backend': storage_meta.get('backend', 'json'),
        'seed_defaults_file': storage_meta.get('seed_defaults_file', TENANT_REGISTRY_FILE),
        'recovery_file': storage_meta.get('recovery_file', TENANT_REGISTRY_FILE),
        'tenants': tenants,
    }
    _record_admin_audit(
        action='tenant.metadata_export',
        entity='tenant',
        target_id='all_tenants',
        status='success',
        details={'count': len(tenants)},
    )
    filename = f"tenant_metadata_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return Response(
        json.dumps(payload, indent=2, ensure_ascii=True),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


@app.route('/api/tenants/metadata/import', methods=['POST'])
@login_required
@admin_scope_required('manage_tenants')
def import_tenant_metadata():
    try:
        payload = _parse_json_upload_or_payload()
    except ValueError as exc:
        _record_admin_audit(
            action='tenant.metadata_import',
            entity='tenant',
            target_id='all_tenants',
            status='error',
            details={'error': str(exc)},
        )
        return jsonify({'success': False, 'error': str(exc)}), 400

    imported = payload.get("data", payload) if isinstance(payload, dict) else {}
    tenants_in = imported.get("tenants", []) if isinstance(imported, dict) else []
    if not isinstance(tenants_in, list):
        return jsonify({'success': False, 'error': 'Import payload must contain a tenants list.'}), 400

    existing_map = _tenant_registry_map()
    created = 0
    updated = 0
    skipped = []
    for entry in tenants_in:
        if not isinstance(entry, dict):
            skipped.append({'reason': 'invalid_record'})
            continue
        tenant_id = _sanitize_tenant_id(entry.get('id') or entry.get('tenant_id'))
        if not tenant_id:
            skipped.append({'reason': 'missing_tenant_id'})
            continue
        if _is_reserved_default_tenant(tenant_id):
            skipped.append({'tenant_id': tenant_id, 'reason': 'reserved_scope'})
            continue
        ok, err, _ = _upsert_tenant_registry_entry(
            tenant_id=tenant_id,
            name=str(entry.get('name') or tenant_id).strip(),
            description=str(entry.get('description') or '').strip(),
        )
        if not ok:
            skipped.append({'tenant_id': tenant_id, 'reason': err or 'save_failed'})
            continue
        if tenant_id in existing_map:
            updated += 1
        else:
            created += 1

    _record_admin_audit(
        action='tenant.metadata_import',
        entity='tenant',
        target_id='all_tenants',
        status='success',
        details={'created': created, 'updated': updated, 'skipped': len(skipped)},
    )
    return jsonify({'success': True, 'created': created, 'updated': updated, 'skipped': skipped})

@app.route('/api/tenants/export', methods=['GET'])
@login_required
@admin_scope_required('manage_tenants')
def export_tenant_data():
    tenant_id = _resolve_read_tenant_id(request.args.get("tenant_id"), fallback=_active_tenant_id())
    include_timescale = _parse_bool_arg(request.args.get("include_timescale"), default=True)
    telemetry_limit = max(1, min(int(request.args.get("telemetry_limit", 50000)), 250000))
    events_limit = max(1, min(int(request.args.get("events_limit", 50000)), 250000))

    sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=tenant_id)
    config = load_base_station_config()
    bs_map = _filter_base_stations_for_tenant(config.get("base_stations", {}), tenant_id=tenant_id)

    state = _load_coverage_positions_state()
    positions = state.get("positions", {}) if isinstance(state, dict) else {}
    positions = positions if isinstance(positions, dict) else {}
    sensor_keys = {f"sensor_{str(s.get('eui', '')).strip().upper()}" for s in sensors}
    bs_keys = {f"bs_{str(eui).strip().upper()}" for eui in bs_map.keys()}
    include_keys = sensor_keys | bs_keys
    export_positions = {key: value for key, value in positions.items() if key in include_keys}

    users_data = load_users()
    tenant_users = []
    for username, user in users_data.get("users", {}).items():
        if not isinstance(user, dict):
            continue
        if _user_belongs_to_tenant(user, tenant_id, fallback=_default_tenant_id()):
            tenant_users.append({
                "username": username,
                "name": user.get("name", ""),
                "role": user.get("role", "viewer"),
                "tenant_id": tenant_id,
            })

    tenant_registry_entry = _tenant_registry_map().get(tenant_id, {})
    payload = {
        "format_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "tenant_id": tenant_id,
        "tenant_name": tenant_registry_entry.get("name", tenant_id),
        "tenant_description": tenant_registry_entry.get("description", ""),
        "data": {
            "sensors": sensors,
            "base_stations": bs_map,
            "coverage_positions": {"positions": export_positions},
            "users": tenant_users,
        },
    }

    if include_timescale and bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)):
        ok, err, ts_dump = _timescale_fetch_tenant_dump(
            tenant_id=tenant_id,
            telemetry_limit=telemetry_limit,
            events_limit=events_limit,
        )
        payload["timescale"] = {
            "included": ok,
            "error": err,
            "limits": {"telemetry_limit": telemetry_limit, "events_limit": events_limit},
            "data": ts_dump if ok else {},
        }
    else:
        payload["timescale"] = {"included": False, "error": "disabled or not requested", "data": {}}

    _record_admin_audit(
        action="tenant.export",
        entity="tenant",
        target_id=tenant_id,
        status="success",
        details={
            "include_timescale": include_timescale,
            "telemetry_limit": telemetry_limit,
            "events_limit": events_limit,
            "sensor_count": len(sensors),
            "base_station_count": len(bs_map),
            "position_count": len(export_positions),
            "user_count": len(tenant_users),
            "timescale_included": bool(payload.get("timescale", {}).get("included")),
            "timescale_error": payload.get("timescale", {}).get("error"),
        },
    )

    content = json.dumps(payload, indent=2, ensure_ascii=True)
    filename = f"tenant_export_{tenant_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return Response(
        content,
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )

@app.route('/api/tenants/import', methods=['POST'])
@login_required
@admin_scope_required('manage_tenants')
def import_tenant_data():
    target_tenant = _resolve_read_tenant_id(
        request.args.get("tenant_id") or (request.form.get("tenant_id") if request.form else None),
        fallback=_active_tenant_id()
    )
    include_timescale = _parse_bool_arg(request.args.get("include_timescale"), default=True)
    merge_mode = _parse_bool_arg(request.args.get("merge"), default=True)

    try:
        if "file" in request.files and request.files["file"].filename:
            raw = request.files["file"].read().decode("utf-8")
            payload = json.loads(raw)
        else:
            payload = request.get_json(silent=True) or {}
    except Exception as exc:
        _record_admin_audit(
            action="tenant.import",
            entity="tenant",
            target_id=target_tenant,
            status="error",
            details={
                "error": f"Invalid import payload: {exc}",
                "include_timescale": include_timescale,
                "merge_mode": merge_mode,
            },
        )
        return jsonify({"success": False, "error": f"Invalid import payload: {exc}"}), 400

    imported = payload.get("data", payload)
    sensors_in = imported.get("sensors", []) if isinstance(imported, dict) else []
    bs_in = imported.get("base_stations", {}) if isinstance(imported, dict) else {}
    coverage_in = imported.get("coverage_positions", {}) if isinstance(imported, dict) else {}
    ts_in = payload.get("timescale", {}).get("data", {}) if isinstance(payload, dict) else {}
    incoming_tenant_name = ""
    incoming_tenant_description = ""
    if isinstance(payload, dict):
        incoming_tenant_name = str(payload.get("tenant_name") or payload.get("name") or "").strip()
        incoming_tenant_description = str(payload.get("tenant_description") or "").strip()
    _upsert_tenant_registry_entry(
        tenant_id=target_tenant,
        name=incoming_tenant_name or target_tenant,
        description=incoming_tenant_description,
    )

    sensors_all = _load_all_sensors()
    if not merge_mode:
        sensors_all = [s for s in sensors_all if not _tenant_matches(_tenant_id_from_sensor(s), target_tenant)]

    sensor_index = {}
    for idx, sensor in enumerate(sensors_all):
        if not isinstance(sensor, dict):
            continue
        sensor_index[(_tenant_id_from_sensor(sensor), str(sensor.get("eui", "")).strip().upper())] = idx

    sensor_created = 0
    sensor_updated = 0
    for sensor in (sensors_in or []):
        if not isinstance(sensor, dict):
            continue
        normalized = _normalize_sensor_payload(sensor)
        if not normalized.get("eui"):
            continue
        normalized["tenant_id"] = target_tenant
        key = (target_tenant, normalized["eui"])
        if key in sensor_index:
            sensors_all[sensor_index[key]].update(normalized)
            sensor_updated += 1
        else:
            sensors_all.append(normalized)
            sensor_index[key] = len(sensors_all) - 1
            sensor_created += 1
    _save_all_sensors(sensors_all)

    config = load_base_station_config()
    bs_map = config.setdefault("base_stations", {})
    if not merge_mode:
        keys_to_delete = [
            key for key, value in bs_map.items()
            if isinstance(value, dict) and _tenant_matches(_tenant_id_from_base_station(value), target_tenant)
        ]
        for key in keys_to_delete:
            bs_map.pop(key, None)

    bs_created = 0
    bs_updated = 0
    if isinstance(bs_in, dict):
        source_bs_items = bs_in.items()
    elif isinstance(bs_in, list):
        source_bs_items = [
            (str(item.get("eui", "")).strip().lower(), item)
            for item in bs_in
            if isinstance(item, dict)
        ]
    else:
        source_bs_items = []
    for key, bs_data in source_bs_items:
        if not isinstance(bs_data, dict):
            continue
        eui = str((bs_data.get("eui") or key or "")).strip().lower()
        if not eui or not _validate_eui(eui):
            continue
        payload_bs = {
            "name": str(bs_data.get("name", "") or "").strip(),
            "tags": bs_data.get("tags", []) if isinstance(bs_data.get("tags", []), list) else [],
            "ip": str(bs_data.get("ip", "") or "").strip(),
            "tenant_id": target_tenant,
        }
        gps_lat, gps_lng = _normalize_gps_coordinates(bs_data.get("gps_lat"), bs_data.get("gps_lng"))
        payload_bs["gps_lat"] = gps_lat
        payload_bs["gps_lng"] = gps_lng
        if eui in bs_map:
            bs_map[eui].update(payload_bs)
            bs_updated += 1
        else:
            bs_map[eui] = payload_bs
            bs_created += 1
    save_base_station_config(config)

    state = _load_coverage_positions_state()
    positions = state.setdefault("positions", {})
    if not isinstance(positions, dict):
        positions = {}
        state["positions"] = positions
    incoming_positions = coverage_in.get("positions", {}) if isinstance(coverage_in, dict) else {}
    for key, value in (incoming_positions or {}).items():
        if not isinstance(value, dict):
            continue
        device_type = str(value.get("deviceType", "")).strip().lower()
        if device_type == "sensor":
            eui = str(value.get("eui", "")).strip().upper()
            if not any(
                isinstance(s, dict)
                and str(s.get("eui", "")).strip().upper() == eui
                and _tenant_matches(_tenant_id_from_sensor(s), target_tenant)
                for s in sensors_all
            ):
                continue
        elif device_type == "bs":
            eui = str(value.get("eui", "")).strip().lower()
            bs_row = bs_map.get(eui, {})
            if not isinstance(bs_row, dict) or not _tenant_matches(_tenant_id_from_base_station(bs_row), target_tenant):
                continue
        positions[key] = value
    _save_coverage_positions_state(state)

    ts_result = {"imported": False, "error": "not requested", "counts": {}}
    if include_timescale and bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)) and isinstance(ts_in, dict):
        conn, err = _timescale_connect()
        if conn is None:
            ts_result = {"imported": False, "error": err, "counts": {}}
        else:
            try:
                _ensure_timescale_schema(conn)
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO NOTHING
                    """, (target_tenant, target_tenant))

                    if not merge_mode:
                        cur.execute("DELETE FROM telemetry_uplink WHERE tenant_id = %s", (target_tenant,))
                        cur.execute("DELETE FROM inventory_snapshot_points WHERE tenant_id = %s", (target_tenant,))
                        cur.execute("DELETE FROM inventory_snapshot_latest WHERE tenant_id = %s", (target_tenant,))
                        cur.execute("DELETE FROM inventory_events WHERE tenant_id = %s", (target_tenant,))

                    events_rows = ts_in.get("inventory_events", []) or []
                    snapshot_points_rows = ts_in.get("inventory_snapshot_points", []) or []
                    snapshot_latest_rows = ts_in.get("inventory_snapshot_latest", []) or []
                    telemetry_rows = ts_in.get("telemetry_uplink", []) or []

                    if events_rows:
                        cur.executemany("""
                            INSERT INTO inventory_events
                                (ts, tenant_id, entity_type, action, eui, actor, event, has_payload, payload_size, source, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, [
                            (
                                row.get("ts"),
                                target_tenant,
                                row.get("entity_type"),
                                row.get("action"),
                                row.get("eui"),
                                row.get("actor") or "import",
                                row.get("event") or "imported",
                                bool(row.get("has_payload", False)),
                                int(row.get("payload_size", 0) or 0),
                                row.get("source") or "tenant_import",
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in events_rows if isinstance(row, dict)
                        ])

                    if snapshot_points_rows:
                        cur.executemany("""
                            INSERT INTO inventory_snapshot_points
                                (ts, tenant_id, entity_type, eui, status, trigger, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, [
                            (
                                row.get("ts"),
                                target_tenant,
                                row.get("entity_type"),
                                row.get("eui"),
                                row.get("status"),
                                row.get("trigger") or "import",
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in snapshot_points_rows if isinstance(row, dict)
                        ])

                    for row in snapshot_latest_rows:
                        if not isinstance(row, dict):
                            continue
                        cur.execute("""
                            INSERT INTO inventory_snapshot_latest
                                (tenant_id, entity_type, eui, status, trigger, payload, updated_at)
                            VALUES
                                (%s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s::timestamptz, NOW()))
                            ON CONFLICT (tenant_id, entity_type, eui)
                            DO UPDATE SET
                                status = EXCLUDED.status,
                                trigger = EXCLUDED.trigger,
                                payload = EXCLUDED.payload,
                                updated_at = EXCLUDED.updated_at
                        """, (
                            target_tenant,
                            row.get("entity_type"),
                            row.get("eui"),
                            row.get("status"),
                            row.get("trigger") or "import",
                            json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            row.get("updated_at"),
                        ))

                    if telemetry_rows:
                        cur.executemany("""
                            INSERT INTO telemetry_uplink
                                (ts, tenant_id, sensor_eui, base_station_eui, snr, rssi, packet_loss_pct, packet_cnt, msg_type, payload)
                            VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, [
                            (
                                row.get("ts"),
                                target_tenant,
                                str(row.get("sensor_eui") or "").strip().lower(),
                                (str(row.get("base_station_eui") or "").strip().lower() or None),
                                float(row.get("snr")) if row.get("snr") is not None else None,
                                float(row.get("rssi")) if row.get("rssi") is not None else None,
                                float(row.get("packet_loss_pct")) if row.get("packet_loss_pct") is not None else None,
                                int(row.get("packet_cnt")) if row.get("packet_cnt") is not None else None,
                                str(row.get("msg_type") or "ul")[:16],
                                json.dumps(row.get("payload") or {}, separators=(",", ":"), ensure_ascii=True),
                            )
                            for row in telemetry_rows if isinstance(row, dict)
                        ])

                ts_result = {
                    "imported": True,
                    "error": None,
                    "counts": {
                        "inventory_events": len(events_rows),
                        "inventory_snapshot_points": len(snapshot_points_rows),
                        "inventory_snapshot_latest": len(snapshot_latest_rows),
                        "telemetry_uplink": len(telemetry_rows),
                    },
                }
            except Exception as exc:
                ts_result = {"imported": False, "error": str(exc), "counts": {}}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    _try_record_inventory_event(
        "tenant",
        "imported",
        target_tenant,
        {
            "sensor_created": sensor_created,
            "sensor_updated": sensor_updated,
            "base_station_created": bs_created,
            "base_station_updated": bs_updated,
            "merge_mode": merge_mode,
            "timescale_imported": bool(ts_result.get("imported")),
        }
    )

    import_status = "success"
    if include_timescale and not bool(ts_result.get("imported")):
        import_status = "warning"

    _record_admin_audit(
        action="tenant.import",
        entity="tenant",
        target_id=target_tenant,
        status=import_status,
        details={
            "merge_mode": merge_mode,
            "include_timescale": include_timescale,
            "sensors_created": sensor_created,
            "sensors_updated": sensor_updated,
            "base_stations_created": bs_created,
            "base_stations_updated": bs_updated,
            "timescale": ts_result,
        },
    )

    return jsonify({
        "success": True,
        "tenant_id": target_tenant,
        "merge_mode": merge_mode,
        "sensors": {"created": sensor_created, "updated": sensor_updated},
        "base_stations": {"created": bs_created, "updated": bs_updated},
        "timescale": ts_result,
    })

def _safe_positive_int(value, default=1, min_value=1, max_value=10_000_000):
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    if parsed < min_value:
        return min_value
    if parsed > max_value:
        return max_value
    return parsed

def _safe_refresh_seconds(value, default=10):
    return _safe_positive_int(value, default=default, min_value=5, max_value=300)

def _grafana_base_url():
    base = str(getattr(bssci_config, "GRAFANA_URL", "http://localhost:3000") or "").strip()
    return (base or "http://localhost:3000").rstrip("/")

def _grafana_internal_base_url():
    internal = str(getattr(bssci_config, "GRAFANA_INTERNAL_URL", "") or "").strip()
    if internal:
        return internal.rstrip("/")
    return _grafana_base_url()

def _grafana_dashboard_uid():
    uid = str(getattr(bssci_config, "GRAFANA_DASHBOARD_UID", "service-center-overview") or "").strip()
    return uid or "service-center-overview"

def _grafana_dashboard_slug():
    raw_slug = str(getattr(bssci_config, "GRAFANA_DASHBOARD_SLUG", "service-center-overview") or "").strip().lower()
    raw_slug = re.sub(r"[^a-z0-9-]+", "-", raw_slug)
    raw_slug = raw_slug.strip("-")
    return raw_slug or "service-center-overview"

def _grafana_org_id():
    return _safe_positive_int(getattr(bssci_config, "GRAFANA_ORG_ID", 1), default=1, min_value=1, max_value=100_000)

def _grafana_embed_enabled():
    return bool(getattr(bssci_config, "GRAFANA_EMBED_ENABLED", True))

def _grafana_proxy_enabled():
    return bool(getattr(bssci_config, "GRAFANA_PROXY_ENABLED", True))

def _grafana_proxy_timeout_seconds():
    return _safe_positive_int(
        getattr(bssci_config, "GRAFANA_PROXY_TIMEOUT_SECONDS", 20),
        default=20,
        min_value=2,
        max_value=120,
    )

def _grafana_proxy_auth_header():
    bearer_token = str(getattr(bssci_config, "GRAFANA_PROXY_BEARER_TOKEN", "") or "").strip()
    if bearer_token:
        return f"Bearer {bearer_token}"

    username = str(getattr(bssci_config, "GRAFANA_PROXY_BASIC_USER", "") or "").strip()
    password = str(getattr(bssci_config, "GRAFANA_PROXY_BASIC_PASSWORD", "") or "")
    if username and password:
        raw = f"{username}:{password}".encode("utf-8")
        encoded = base64.b64encode(raw).decode("ascii")
        return f"Basic {encoded}"

    return ""

def _grafana_is_configured():
    base_url = _grafana_internal_base_url() if _grafana_proxy_enabled() else _grafana_base_url()
    return bool(str(base_url or "").strip()) and bool(str(_grafana_dashboard_uid() or "").strip())

def _grafana_requested_tenant_id():
    return _resolve_read_tenant_id(request.args.get("tenant_id"), fallback=_active_tenant_id())

def _default_health_panel_map():
    return {
        "throughput": 1,
        "signal": 2,
        "active_sensors": 3,
        "active_base_stations": 4,
        "top_sensors": 5,
        "recent_messages": 6,
    }

def _grafana_health_panel_map():
    raw_map = str(getattr(bssci_config, "GRAFANA_HEALTH_PANEL_MAP", "") or "").strip()
    if not raw_map:
        return _default_health_panel_map()

    parsed_map = {}
    for token in raw_map.split(","):
        if ":" not in token:
            continue
        key_raw, value_raw = token.split(":", 1)
        key = str(key_raw or "").strip().lower()
        if not key:
            continue
        panel_id = _safe_positive_int(value_raw, default=0, min_value=0, max_value=1000)
        if panel_id > 0:
            parsed_map[key] = panel_id

    if not parsed_map:
        return _default_health_panel_map()

    defaults = _default_health_panel_map()
    for key, panel_id in defaults.items():
        parsed_map.setdefault(key, panel_id)
    return parsed_map

def _build_grafana_dashboard_params(tenant_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = {
        "orgId": _grafana_org_id(),
        "var-tenant": tenant_id,
    }
    if from_ms is not None and to_ms is not None:
        params["from"] = int(from_ms)
        params["to"] = int(to_ms)
    elif minutes is not None:
        minutes = _safe_positive_int(minutes, default=360, min_value=5, max_value=7 * 24 * 60)
        params["from"] = f"now-{minutes}m"
        params["to"] = "now"
    if refresh_seconds is not None:
        params["refresh"] = f"{_safe_refresh_seconds(refresh_seconds)}s"
    return params

def _build_grafana_dashboard_url(tenant_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_dashboard_params(
        tenant_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    return (
        f"{_grafana_base_url()}/d/{urllib.parse.quote(_grafana_dashboard_uid())}/"
        f"{urllib.parse.quote(_grafana_dashboard_slug())}?{urllib.parse.urlencode(params)}"
    )

def _build_grafana_panel_params(tenant_id, panel_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = {
        "orgId": _grafana_org_id(),
        "panelId": int(panel_id),
        "var-tenant": tenant_id,
        "theme": "light",
    }
    if from_ms is not None and to_ms is not None:
        params["from"] = int(from_ms)
        params["to"] = int(to_ms)
    elif minutes is not None:
        minutes = _safe_positive_int(minutes, default=360, min_value=5, max_value=7 * 24 * 60)
        params["from"] = f"now-{minutes}m"
        params["to"] = "now"
    if refresh_seconds is not None:
        params["refresh"] = f"{_safe_refresh_seconds(refresh_seconds)}s"
    return params

def _build_grafana_panel_url(tenant_id, panel_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_panel_params(
        tenant_id,
        panel_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    return (
        f"{_grafana_base_url()}/d-solo/{urllib.parse.quote(_grafana_dashboard_uid())}/"
        f"{urllib.parse.quote(_grafana_dashboard_slug())}?{urllib.parse.urlencode(params)}"
    )

def _build_grafana_proxy_url(proxy_path, params=None):
    clean_path = str(proxy_path or "").strip().lstrip("/")
    query = urllib.parse.urlencode(params or {}, doseq=True)
    base = f"/grafana-proxy/{clean_path}" if clean_path else "/grafana-proxy/"
    if query:
        return f"{base}?{query}"
    return base

def _build_grafana_dashboard_proxy_url(tenant_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_dashboard_params(
        tenant_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    params["tenant_id"] = tenant_id
    params.pop("var-tenant", None)
    return _build_grafana_proxy_url(
        f"d/{urllib.parse.quote(_grafana_dashboard_uid())}/{urllib.parse.quote(_grafana_dashboard_slug())}",
        params=params,
    )

def _build_grafana_panel_proxy_url(tenant_id, panel_id, from_ms=None, to_ms=None, minutes=None, refresh_seconds=None):
    params = _build_grafana_panel_params(
        tenant_id,
        panel_id,
        from_ms=from_ms,
        to_ms=to_ms,
        minutes=minutes,
        refresh_seconds=refresh_seconds,
    )
    params["tenant_id"] = tenant_id
    params.pop("var-tenant", None)
    return _build_grafana_proxy_url(
        f"d-solo/{urllib.parse.quote(_grafana_dashboard_uid())}/{urllib.parse.quote(_grafana_dashboard_slug())}",
        params=params,
    )

def _should_enforce_grafana_tenant(proxy_path):
    normalized = str(proxy_path or "").strip().lstrip("/").lower()
    return (
        normalized.startswith("d/")
        or normalized.startswith("d-solo/")
        or normalized.startswith("render/d/")
        or normalized.startswith("render/d-solo/")
    )

def _inject_tenant_scoped_var(payload, tenant_id):
    if not isinstance(payload, dict):
        return payload

    tenant_scope = {"text": tenant_id, "value": tenant_id, "selected": True}
    scoped_vars = payload.get("scopedVars")
    if isinstance(scoped_vars, dict):
        scoped_vars["tenant"] = tenant_scope

    queries = payload.get("queries")
    if isinstance(queries, list):
        for query in queries:
            if not isinstance(query, dict):
                continue
            query_scoped = query.get("scopedVars")
            if isinstance(query_scoped, dict):
                query_scoped["tenant"] = tenant_scope

    return payload

def _build_grafana_proxy_upstream_url(proxy_path):
    internal_base = _grafana_internal_base_url()
    parsed_base = urllib.parse.urlsplit(internal_base)
    base_path = parsed_base.path.rstrip("/")
    normalized_proxy_path = str(proxy_path or "").lstrip("/")
    upstream_path = f"{base_path}/{normalized_proxy_path}" if normalized_proxy_path else (base_path or "/")

    query_pairs = []
    for key in request.args:
        if key.lower() in {"var-tenant", "tenant_id"}:
            continue
        for value in request.args.getlist(key):
            query_pairs.append((key, value))

    if _should_enforce_grafana_tenant(proxy_path):
        query_pairs.append(("var-tenant", _grafana_requested_tenant_id()))

    upstream_query = urllib.parse.urlencode(query_pairs, doseq=True)
    return urllib.parse.urlunsplit(
        (
            parsed_base.scheme or "http",
            parsed_base.netloc,
            upstream_path,
            upstream_query,
            "",
        )
    )

def _rewrite_grafana_location_header(location):
    raw_location = str(location or "").strip()
    if not raw_location:
        return raw_location

    proxy_prefix = "/grafana-proxy"
    if raw_location.startswith(proxy_prefix):
        return raw_location

    parsed_base = urllib.parse.urlsplit(_grafana_internal_base_url())
    parsed_location = urllib.parse.urlsplit(raw_location)

    if parsed_location.scheme and parsed_location.netloc:
        if parsed_location.netloc == parsed_base.netloc:
            return urllib.parse.urlunsplit(
                (
                    "",
                    "",
                    f"{proxy_prefix}{parsed_location.path}",
                    parsed_location.query,
                    parsed_location.fragment,
                )
            )
        return raw_location

    if raw_location.startswith("/"):
        return f"{proxy_prefix}{raw_location}"
    return raw_location

def _rewrite_grafana_text_body(body_text):
    proxy_prefix = "/grafana-proxy"
    text = str(body_text or "")
    replacements = (
        ('href="/', f'href="{proxy_prefix}/'),
        ('src="/', f'src="{proxy_prefix}/'),
        ('action="/', f'action="{proxy_prefix}/'),
        ("url(/", f"url({proxy_prefix}/"),
        ('"/public/', f'"{proxy_prefix}/public/'),
        ("'/public/", f"'{proxy_prefix}/public/"),
        ('"/api/', f'"{proxy_prefix}/api/'),
        ("'/api/", f"'{proxy_prefix}/api/"),
        ('"/avatar/', f'"{proxy_prefix}/avatar/'),
        ("'/avatar/", f"'{proxy_prefix}/avatar/"),
        ('"/login', f'"{proxy_prefix}/login'),
        ("'/login", f"'{proxy_prefix}/login"),
        ('"/logout', f'"{proxy_prefix}/logout'),
        ("'/logout", f"'{proxy_prefix}/logout"),
    )
    for source, target in replacements:
        text = text.replace(source, target)

    text = re.sub(r'("appSubUrl"\s*:\s*")([^"]*)(")', r'\1/grafana-proxy\3', text)
    while "/grafana-proxy/grafana-proxy/" in text:
        text = text.replace("/grafana-proxy/grafana-proxy/", "/grafana-proxy/")
    return text

@app.route('/grafana-proxy', defaults={'proxy_path': ''}, methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
@app.route('/grafana-proxy/<path:proxy_path>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])
@login_required
def grafana_proxy(proxy_path):
    if not _grafana_proxy_enabled():
        return jsonify({"success": False, "error": "Grafana proxy is disabled."}), 403
    if not _grafana_is_configured():
        return jsonify({"success": False, "error": "Grafana is not configured."}), 503

    upstream_url = _build_grafana_proxy_upstream_url(proxy_path)
    request_body = request.get_data(cache=False, as_text=False) or b""
    normalized_path = str(proxy_path or "").strip().lstrip("/").lower()
    tenant_id = _grafana_requested_tenant_id()

    if normalized_path.startswith("api/ds/query") and request_body:
        try:
            parsed_payload = json.loads(request_body.decode("utf-8"))
            parsed_payload = _inject_tenant_scoped_var(parsed_payload, tenant_id)
            request_body = json.dumps(parsed_payload, separators=(",", ":")).encode("utf-8")
        except Exception:
            pass

    forwarded_headers = {}
    for header_name, header_value in request.headers.items():
        lower_name = header_name.lower()
        if lower_name in {"host", "content-length", "accept-encoding", "cookie"}:
            continue
        if lower_name in {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}:
            continue
        forwarded_headers[header_name] = header_value
    forwarded_headers["Accept-Encoding"] = "identity"
    forwarded_headers["X-Forwarded-Host"] = request.host
    forwarded_headers["X-Forwarded-Proto"] = request.scheme
    forwarded_headers["X-Forwarded-Prefix"] = "/grafana-proxy"
    upstream_auth = _grafana_proxy_auth_header()
    if upstream_auth:
        forwarded_headers["Authorization"] = upstream_auth

    upstream_request = urllib.request.Request(
        upstream_url,
        data=request_body if request.method in {"POST", "PUT", "PATCH", "DELETE"} else None,
        headers=forwarded_headers,
        method=request.method,
    )

    def _build_proxy_response(status_code, upstream_headers, body_bytes):
        response_headers = {}
        for header_name, header_value in list(upstream_headers.items()):
            lower_name = header_name.lower()
            if lower_name in {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length"}:
                continue
            if lower_name == "location":
                response_headers[header_name] = _rewrite_grafana_location_header(header_value)
                continue
            response_headers[header_name] = header_value

        content_type = str(response_headers.get("Content-Type", "") or "").lower()
        if any(token in content_type for token in ("text/html", "text/css", "application/javascript", "text/javascript")):
            try:
                rewritten = _rewrite_grafana_text_body(body_bytes.decode("utf-8"))
                body_bytes = rewritten.encode("utf-8")
            except Exception:
                pass

        response = Response(body_bytes, status=status_code)
        for header_name, header_value in response_headers.items():
            response.headers[header_name] = header_value
        response.headers.pop("X-Frame-Options", None)
        return response

    try:
        with urllib.request.urlopen(
            upstream_request,
            timeout=_grafana_proxy_timeout_seconds(),
            context=ssl._create_unverified_context(),
        ) as upstream_response:
            upstream_body = upstream_response.read()
            return _build_proxy_response(upstream_response.status, upstream_response.headers, upstream_body)
    except urllib.error.HTTPError as error:
        error_body = error.read() if hasattr(error, "read") else b""
        return _build_proxy_response(error.code, error.headers, error_body)
    except Exception as error:
        return jsonify({"success": False, "error": f"Grafana proxy failed: {error}"}), 502

@app.route('/api/grafana/dashboard-url', methods=['GET'])
@login_required
def grafana_dashboard_url():
    tenant_id = _grafana_requested_tenant_id()
    use_proxy = _grafana_proxy_enabled()
    return jsonify({
        "success": True,
        "tenant_id": tenant_id,
        "proxy_enabled": use_proxy,
        "url": (
            _build_grafana_dashboard_proxy_url(tenant_id)
            if use_proxy
            else _build_grafana_dashboard_url(tenant_id)
        ),
    })

@app.route('/api/health/grafana', methods=['GET'])
@login_required
@internal_portal_required
def health_grafana_urls():
    minutes = _safe_positive_int(request.args.get("minutes"), default=360, min_value=5, max_value=7 * 24 * 60)
    raw_refresh_seconds = str(request.args.get("refresh_seconds", "") or "").strip().lower()
    if raw_refresh_seconds in {"", "0", "false", "off", "no"}:
        refresh_seconds = None
    else:
        refresh_seconds = _safe_refresh_seconds(raw_refresh_seconds, default=10)

    tenant_id = _grafana_requested_tenant_id()
    panel_map = _grafana_health_panel_map()
    use_proxy = _grafana_proxy_enabled()
    panels = {
        key: {
            "panel_id": int(panel_id),
            "url": (
                _build_grafana_panel_proxy_url(
                    tenant_id,
                    panel_id,
                    minutes=minutes,
                    refresh_seconds=refresh_seconds,
                )
                if use_proxy
                else _build_grafana_panel_url(
                    tenant_id,
                    panel_id,
                    minutes=minutes,
                    refresh_seconds=refresh_seconds,
                )
            ),
        }
        for key, panel_id in panel_map.items()
    }

    return jsonify({
        "success": True,
        "configured": _grafana_is_configured(),
        "embed_enabled": _grafana_embed_enabled(),
        "proxy_enabled": use_proxy,
        "anonymous_enabled": bool(getattr(bssci_config, "GRAFANA_ANONYMOUS_ENABLED", True)),
        "anonymous_org_role": str(getattr(bssci_config, "GRAFANA_ANONYMOUS_ORG_ROLE", "Viewer") or "Viewer"),
        "tenant_id": tenant_id,
        "minutes": minutes,
        "refresh_seconds": refresh_seconds,
        "from": f"now-{minutes}m",
        "to": "now",
        "dashboard_url": (
            _build_grafana_dashboard_proxy_url(
                tenant_id,
                minutes=minutes,
                refresh_seconds=refresh_seconds,
            )
            if use_proxy
            else _build_grafana_dashboard_url(
                tenant_id,
                minutes=minutes,
                refresh_seconds=refresh_seconds,
            )
        ),
        "panels": panels,
    })

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/sensors')
@login_required
def sensors():
    sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=_active_tenant_id())
    return render_template('sensors.html', sensors=sensors, sensor_profiles=_sensor_profile_options())


@app.route('/sensors/<eui>')
@login_required
def sensor_detail_page(eui):
    tenant_ids_set = set(_tenant_registry_map().keys())
    tenant_ids_set.add(_default_tenant_id())
    users_data = load_users()
    for user in (users_data.get("users", {}) or {}).values():
        if isinstance(user, dict):
            role = _normalize_user_role(user.get("role", "viewer"))
            if role != "admin":
                t = str(user.get("tenant_id") or "").strip().lower()
                if t:
                    tenant_ids_set.add(t)
    all_tenant_ids = sorted(t for t in tenant_ids_set if t and not _is_global_tenant_scope(t))
    return render_template(
        'sensor_detail.html',
        sensor_eui=str(eui or '').strip().upper(),
        sensor_profiles=_sensor_profile_options(),
        all_tenant_ids=all_tenant_ids,
    )


@app.route('/alerts')
@login_required
def alerts_page():
    return render_template('alerts.html')


@app.route('/sensor-telemetry')
@login_required
@internal_portal_required
def sensor_telemetry():
    return render_template('sensor_telemetry.html', telemetry_timezone=str(getattr(bssci_config, 'TIMEZONE', 'Europe/Berlin') or 'Europe/Berlin'))

@app.route('/api/sensors', methods=['GET'])
@login_required
def get_sensors():
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        active_tenant = _active_tenant_id()
        current_role = _normalize_user_role(session.get('role', 'viewer'))
        force_refresh = str(request.args.get('refresh', '') or '').strip().lower() in {'1', 'true', 'yes'}
        if _is_customer_role(current_role) and not force_refresh:
            cached_payload = _get_cached_viewer_sensor_list(active_tenant)
            if cached_payload is not None:
                return jsonify(cached_payload)
        
        # Load configured sensors from the DB-first store (with recovery-file fallback)
        sensor_status = {}
        try:
            sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
            print(f"Loaded {len(sensors)} configured sensors for tenant '{active_tenant}'")
                
            # Initialize sensor status from configured inventory
            for sensor in sensors:
                eui = str(sensor.get('eui', '')).upper()
                if not eui:
                    continue
                sensor_status[eui] = {
                    'eui': eui,
                    'nwKey': sensor.get('nwKey', ''),
                    'shortAddr': sensor.get('shortAddr', '0000'),
                    'bidi': bool(sensor.get('bidi', False)),
                    'name': sensor.get('name', ''),
                    'tags': _normalize_sensor_tags(sensor.get('tags', [])),
                    'sensor_profile': _infer_sensor_profile(sensor),
                    'sensor_profile_label': _sensor_profile_label(_infer_sensor_profile(sensor)),
                    'payload_decoder': str(sensor.get('payload_decoder', 'auto') or 'auto'),
                    'environment_context': str(sensor.get('environment_context', 'auto') or 'auto'),
                    'configured_reporting_mode': _normalize_reporting_mode(sensor.get('reporting_mode')),
                    'reporting_mode': _normalize_reporting_mode(sensor.get('reporting_mode')),
                    'expected_interval_seconds': sensor.get('expected_interval_seconds'),
                    'configured_expected_interval_seconds': _normalize_expected_interval_seconds(sensor.get('expected_interval_seconds')),
                    'stale_after_hours': sensor.get('stale_after_hours'),
                    'configured_stale_after_hours': _normalize_stale_after_hours(sensor.get('stale_after_hours')),
                    'gps_lat': sensor.get('gps_lat'),
                    'gps_lng': sensor.get('gps_lng'),
                    'marker_color': sensor.get('marker_color'),
                    'tenant_id': sensor.get('tenant_id', active_tenant),
                    'registered': False,
                    'registration_info': {},
                    'base_stations': [],
                    'runtime_base_stations': [],
                    'attached_base_stations': _normalize_base_station_route_list(
                        sensor.get('attached_base_stations', [])
                    ),
                    'missing_registrations': [],
                    'total_registrations': 0,
                    'total_available_bases': 0,
                    'preferredDownlinkPath': sensor.get('preferredDownlinkPath', None),
                    'activity_status': 'no_data',
                    'hours_since_last_seen': 0,
                    'last_seen_timestamp': 0,
                    'packets_received': 0,
                    'packets_lost': 0,
                    'avg_snr': None,
                    'avg_rssi': None,
                }
                
            # Get connected base stations list for missing registration tracking
            connected_bases = []
            if tls_server and hasattr(tls_server, 'connected_base_stations'):
                connected_bases = _normalize_base_station_route_list(
                    list(tls_server.connected_base_stations.values())
                )
                # Update total available bases for all sensors
                for sensor_eui in sensor_status:
                    sensor_status[sensor_eui]['total_available_bases'] = len(connected_bases)
            
            # Now safely get real registration data from TLS server
            if tls_server and hasattr(tls_server, 'registered_sensors'):
                try:
                    # Thread-safe access to registered sensors data
                    registered_dict = getattr(tls_server, 'registered_sensors', {})
                    print(f"Accessing registration data for {len(registered_dict)} registered sensors")
                    
                    for sensor_eui, reg_data in list(registered_dict.items()):
                        if sensor_eui in sensor_status:
                            try:
                                # Get base stations list safely
                                base_stations_list = _normalize_base_station_route_list(
                                    reg_data.get('base_stations', [])
                                )
                                registrations_list = reg_data.get('registrations', [])
                                
                                # Calculate missing registrations
                                missing_bases = [bs for bs in connected_bases if bs not in base_stations_list]
                                
                                sensor_status[sensor_eui].update({
                                    'registered': reg_data.get('status') == 'registered',
                                    'base_stations': base_stations_list,
                                    'runtime_base_stations': base_stations_list,
                                    'missing_registrations': missing_bases,
                                    'total_registrations': len(base_stations_list),
                                    'registration_info': {
                                        'status': reg_data.get('status', 'unknown'),
                                        'last_update': reg_data.get('registration_time', 'Unknown'),
                                        'registrations': registrations_list
                                    }
                                })
                                
                                print(f"Sensor {sensor_eui}: {len(base_stations_list)} base stations - {base_stations_list}")
                                
                            except Exception as e:
                                print(f"Error processing registration data for sensor {sensor_eui}: {e}")
                                
                except Exception as e:
                    print(f"Error accessing TLS server registration data: {e}")
            
            # For sensors without registration data, mark all connected bases as missing
            for sensor_eui in sensor_status:
                if not sensor_status[sensor_eui]['base_stations']:
                    sensor_status[sensor_eui]['missing_registrations'] = connected_bases.copy()

            # Merge runtime status + packet telemetry so UI does not stay stuck on "No data".
            runtime_status = {}
            if tls_server and hasattr(tls_server, 'get_sensor_registration_status'):
                try:
                    runtime_status = tls_server.get_sensor_registration_status() or {}
                except Exception as e:
                    print(f"Error getting runtime sensor registration status: {e}")

            packet_stats = {}
            if tls_server and hasattr(tls_server, 'sensor_packet_stats'):
                try:
                    packet_stats = getattr(tls_server, 'sensor_packet_stats', {}) or {}
                except Exception as e:
                    print(f"Error accessing sensor packet stats: {e}")

            snapshot_sensor_map = _timescale_fetch_latest_sensor_snapshots(active_tenant)

            now_ts = datetime.now(timezone.utc).timestamp()
            warning_timeout = float(getattr(bssci_config, 'AUTO_DETACH_WARNING_TIMEOUT', 129600) or 129600)
            detach_timeout = float(getattr(bssci_config, 'AUTO_DETACH_TIMEOUT', 259200) or 259200)

            for sensor_eui, sensor_data in sensor_status.items():
                runtime_entry = (
                    runtime_status.get(sensor_eui)
                    or runtime_status.get(sensor_eui.upper())
                    or runtime_status.get(sensor_eui.lower())
                    or {}
                )

                if runtime_entry:
                    if runtime_entry.get('preferredDownlinkPath') is not None:
                        sensor_data['preferredDownlinkPath'] = runtime_entry.get('preferredDownlinkPath')
                    if runtime_entry.get('activity_status'):
                        sensor_data['activity_status'] = runtime_entry.get('activity_status')
                    if runtime_entry.get('last_seen_timestamp'):
                        last_seen_runtime = float(runtime_entry.get('last_seen_timestamp') or 0)
                        sensor_data['last_seen_timestamp'] = last_seen_runtime
                        sensor_data['hours_since_last_seen'] = (
                            float(runtime_entry.get('hours_since_last_seen'))
                            if runtime_entry.get('hours_since_last_seen') is not None
                            else round(max(0.0, now_ts - last_seen_runtime) / 3600.0, 2)
                        )

                stats = (
                    packet_stats.get(sensor_eui)
                    or packet_stats.get(sensor_eui.upper())
                    or packet_stats.get(sensor_eui.lower())
                    or {}
                )
                snapshot_entry = snapshot_sensor_map.get(sensor_eui) or snapshot_sensor_map.get(sensor_eui.upper()) or {}
                snapshot_payload = snapshot_entry.get("payload") if isinstance(snapshot_entry, dict) else {}
                if stats:
                    packets_received = int(stats.get('packets_received', 0) or 0)
                    packets_lost = int(stats.get('packets_lost', 0) or 0)
                    snr_count = int(stats.get('snr_count', 0) or 0)
                    rssi_count = int(stats.get('rssi_count', 0) or 0)
                    avg_snr = (float(stats.get('snr_sum', 0.0) or 0.0) / snr_count) if snr_count > 0 else None
                    avg_rssi = (float(stats.get('rssi_sum', 0.0) or 0.0) / rssi_count) if rssi_count > 0 else None
                    last_seen = float(stats.get('last_seen', 0) or 0)

                    sensor_data['packets_received'] = packets_received
                    sensor_data['packets_lost'] = packets_lost
                    sensor_data['avg_snr'] = round(avg_snr, 2) if avg_snr is not None else None
                    sensor_data['avg_rssi'] = round(avg_rssi, 2) if avg_rssi is not None else None

                    if last_seen > 0:
                        sensor_data['last_seen_timestamp'] = last_seen
                        sensor_data['hours_since_last_seen'] = round(max(0.0, now_ts - last_seen) / 3600.0, 2)
                        interval_meta = _resolve_sensor_expected_interval(sensor_data, stats.get('avg_interval_seconds'))
                        sensor_data.update(interval_meta)
                        if sensor_data.get('activity_status') not in {'auto_detached', 'auto_detach_pending'}:
                            inactive_seconds = max(0.0, now_ts - last_seen)
                            if inactive_seconds > detach_timeout:
                                sensor_data['activity_status'] = 'auto_detach_pending'
                            elif interval_meta.get('reporting_mode') == 'event':
                                if inactive_seconds > interval_meta['stale_threshold_seconds']:
                                    sensor_data['activity_status'] = 'stale'
                                elif inactive_seconds <= 3600:
                                    sensor_data['activity_status'] = 'active'
                                else:
                                    sensor_data['activity_status'] = 'quiet'
                            elif inactive_seconds > interval_meta['offline_threshold_seconds']:
                                sensor_data['activity_status'] = 'warning'
                            else:
                                sensor_data['activity_status'] = 'active'
                    else:
                        sensor_data.update(_resolve_sensor_expected_interval(sensor_data, stats.get('avg_interval_seconds')))
                elif snapshot_payload:
                    packets_received = int(snapshot_payload.get('packets_received', 0) or 0)
                    packets_lost = int(snapshot_payload.get('packets_lost', 0) or 0)
                    avg_snr = snapshot_payload.get('avg_snr')
                    avg_rssi = snapshot_payload.get('avg_rssi')
                    last_seen = float(snapshot_payload.get('last_seen_ts', 0) or 0)

                    sensor_data['packets_received'] = packets_received
                    sensor_data['packets_lost'] = packets_lost
                    sensor_data['avg_snr'] = round(float(avg_snr), 2) if avg_snr is not None else None
                    sensor_data['avg_rssi'] = round(float(avg_rssi), 2) if avg_rssi is not None else None
                    sensor_data['packet_loss_source'] = 'snapshot'

                    if last_seen > 0:
                        sensor_data['last_seen_timestamp'] = last_seen
                        sensor_data['hours_since_last_seen'] = round(max(0.0, now_ts - last_seen) / 3600.0, 2)
                        interval_meta = _resolve_sensor_expected_interval(sensor_data, None)
                        sensor_data.update(interval_meta)
                        if sensor_data.get('activity_status') not in {'auto_detached', 'auto_detach_pending'}:
                            inactive_seconds = max(0.0, now_ts - last_seen)
                            if interval_meta.get('reporting_mode') == 'event':
                                if inactive_seconds > interval_meta['stale_threshold_seconds']:
                                    sensor_data['activity_status'] = 'stale'
                                elif inactive_seconds <= 3600:
                                    sensor_data['activity_status'] = 'active'
                                else:
                                    sensor_data['activity_status'] = 'quiet'
                            elif inactive_seconds > interval_meta['offline_threshold_seconds']:
                                sensor_data['activity_status'] = 'warning'
                            else:
                                sensor_data['activity_status'] = 'active'
                    else:
                        sensor_data.update(_resolve_sensor_expected_interval(sensor_data, None))
                else:
                    sensor_data.update(_resolve_sensor_expected_interval(sensor_data, None))
                availability = _sensor_availability_snapshot(
                    sensor_data,
                    active_tenant,
                    runtime_status=runtime_status,
                    packet_stats=packet_stats,
                    snapshot_sensor_map=snapshot_sensor_map,
                )
                sensor_data.update({
                    'activity_status': availability.get('activity_status', sensor_data.get('activity_status', 'no_data')),
                    'last_seen_timestamp': availability.get('last_seen_timestamp', sensor_data.get('last_seen_timestamp', 0)),
                    'hours_since_last_seen': availability.get('hours_since_last_seen', sensor_data.get('hours_since_last_seen', 0)),
                    'reporting_mode': availability.get('reporting_mode', sensor_data.get('reporting_mode')),
                    'reporting_mode_source': availability.get('reporting_mode_source', sensor_data.get('reporting_mode_source')),
                    'stale_after_hours': availability.get('stale_after_hours', sensor_data.get('stale_after_hours')),
                    'stale_after_source': availability.get('stale_after_source', sensor_data.get('stale_after_source')),
                    'expected_interval_seconds': availability.get('expected_interval_seconds', sensor_data.get('expected_interval_seconds')),
                    'expected_interval_source': availability.get('expected_interval_source', sensor_data.get('expected_interval_source')),
                    'delay_threshold_seconds': availability.get('delay_threshold_seconds', sensor_data.get('delay_threshold_seconds')),
                    'offline_threshold_seconds': availability.get('offline_threshold_seconds', sensor_data.get('offline_threshold_seconds')),
                    'stale_threshold_seconds': availability.get('stale_threshold_seconds', sensor_data.get('stale_threshold_seconds')),
                    'ui_tier': availability.get('ui_tier', sensor_data.get('ui_tier')),
                    'status_incident': availability.get('status_incident', sensor_data.get('status_incident')),
                    'status_incident_severity': availability.get('status_incident_severity', sensor_data.get('status_incident_severity')),
                })
                    
            print(f"Processed sensor status for {len(sensor_status)} sensors with registration data")
            if _is_customer_role(current_role):
                _store_cached_viewer_sensor_list(active_tenant, sensor_status)
            return jsonify(sensor_status)
        except FileNotFoundError:
            sensor_file = getattr(bssci_config, 'SENSOR_CONFIG_FILE', 'endpoints.json')
            print(f"Sensor recovery file not found: {sensor_file}")
            return jsonify({})
        except json.JSONDecodeError as e:
            sensor_file = getattr(bssci_config, 'SENSOR_CONFIG_FILE', 'endpoints.json')
            print(f"Invalid JSON in sensor recovery file {sensor_file}: {e}")
            return jsonify({})
            
    except Exception as e:
        print(f"Error in get_sensors: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/sensors', methods=['POST'])
@login_required
@permission_required('can_add_sensors')
def add_sensor():
    try:
        data = _normalize_sensor_payload(request.json or {})
    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400
    
    try:
        if not data.get('eui'):
            return jsonify({'success': False, 'message': 'EUI is required'})
        
        # Step 1: Persist into the DB-first sensor store
        sensors = _load_all_sensors()
        active_tenant = _resolve_write_tenant_id(data.get("tenant_id"))
        data["tenant_id"] = active_tenant

        # Check if sensor already exists
        sensor_updated = False
        changed_fields = []
        attach_targets_after_save = []
        previous_snapshot = {}
        for sensor in sensors:
            if str(sensor.get('eui', '')).upper() != data['eui'].upper():
                continue
            existing_tenant = _tenant_id_from_sensor(sensor)
            if not _tenant_matches(existing_tenant, active_tenant):
                return jsonify({
                    'success': False,
                    'message': f"Sensor EUI already exists in tenant '{existing_tenant}'. Reassign first if needed."
                }), 409
            # Update existing sensor
            previous_snapshot = _sensor_audit_snapshot(sensor)
            compare_fields = [
                'nwKey',
                'shortAddr',
                'bidi',
                'name',
                'tags',
                'sensor_profile',
                'gps_lat',
                'gps_lng',
                'payload_decoder',
                'environment_context',
                'reporting_mode',
                'expected_interval_seconds',
                'stale_after_hours',
            ]
            for field in compare_fields:
                if sensor.get(field) != data.get(field):
                    changed_fields.append(field)
            sensor.update(data)
            attach_targets_after_save = _normalize_base_station_route_list(
                sensor.get("attached_base_stations", [])
            )
            sensor_updated = True
            break

        if not sensor_updated:
            # Add new sensor
            data["created_at"] = str(data.get("created_at") or datetime.now(timezone.utc).isoformat())
            sensors.append(data)
            attach_targets_after_save = _normalize_base_station_route_list(
                data.get("attached_base_stations", [])
            )
        else:
            for sensor in sensors:
                if str(sensor.get('eui', '')).upper() == data['eui'].upper():
                    if sensor.get("created_at"):
                        data["created_at"] = sensor.get("created_at")
                    break

        # Save to file
        _save_all_sensors(sensors)

        _try_record_inventory_event(
            "sensor",
            "updated" if sensor_updated else "created",
            data.get("eui", ""),
            {
                "short_addr": data.get("shortAddr", ""),
                "bidi": bool(data.get("bidi", False)),
                "nwkey_present": bool(data.get("nwKey")),
                "name": data.get("name", ""),
                "tags_count": len(data.get("tags", [])),
                "sensor_profile": data.get("sensor_profile", "auto"),
                "payload_decoder": data.get("payload_decoder", "auto"),
                "environment_context": data.get("environment_context", "auto"),
                "reporting_mode": data.get("reporting_mode", "auto"),
                "expected_interval_seconds": data.get("expected_interval_seconds"),
                "stale_after_hours": data.get("stale_after_hours"),
                "gps_lat": data.get("gps_lat"),
                "gps_lng": data.get("gps_lng"),
                "tenant_id": active_tenant,
            }
        )

        _upsert_device_gps_position("sensor", data.get("eui", ""), data.get("gps_lat"), data.get("gps_lng"))
        new_snapshot = _sensor_audit_snapshot(data)
        _record_admin_audit(
            action='sensor.update' if sensor_updated else 'sensor.create',
            entity='sensor',
            target_id=data.get("eui", ""),
            status='success',
            details={
                "tenant_id": active_tenant,
                "changed_fields": changed_fields if sensor_updated else list(new_snapshot.keys()),
                "before": previous_snapshot if sensor_updated else {},
                "after": new_snapshot,
            },
        )
        
        # Step 2: Notify TLS server and apply runtime attach only when needed
        global tls_server_instance
        tls_server = tls_server_instance
        should_trigger_runtime_attach = (
            not sensor_updated
            or any(field in {'nwKey', 'shortAddr', 'bidi'} for field in changed_fields)
        )

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                # Reload the sensor configuration in TLS server
                tls_server.reload_sensor_config()

                if not should_trigger_runtime_attach:
                    return jsonify({'success': True, 'message': 'Senzor bol uložený. Nastavenia prepojenia zostali bez zmeny.'})

                # Force attach only for new sensor or when protocol fields changed
                if hasattr(tls_server, 'connected_base_stations') and tls_server.connected_base_stations:
                    print(
                        f"Triggering runtime attach for sensor {data['eui']} "
                        f"(targets={attach_targets_after_save or 'all connected'})"
                    )
                    if hasattr(tls_server, 'attach_sensor_sync'):
                        attached_count = tls_server.attach_sensor_sync(
                            data['eui'],
                            attach_targets_after_save or None,
                        )
                        if attached_count > 0:
                            return jsonify({'success': True, 'message': f'Senzor bol uložený a prepojený s {attached_count} základňovými stanicami.'})
                        return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí, keď budú základňové stanice dostupné.'})
                    return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí po obnovení spojenia.'})

                return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí po opätovnom pripojení základňových staníc.'})
            except Exception as e:
                print(f"Error notifying TLS server: {e}")
                return jsonify({'success': True, 'message': 'Senzor bol uložený. Dokončenie prepojenia sa oneskorilo.'})
        else:
            return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí neskôr.'})
                
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})

@app.route('/api/sensors/<eui>', methods=['PUT'])
@login_required
@permission_required('can_add_sensors')
def update_sensor(eui):
    eui_upper = str(eui or '').strip().upper()
    patch = request.get_json(silent=True) or {}
    if not eui_upper:
        return jsonify({'success': False, 'message': 'EUI is required'}), 400

    try:
        sensors = _load_all_sensors()
        active_tenant = _active_tenant_id()
        target_index = None
        existing_sensor = None
        for idx, sensor in enumerate(sensors):
            if str(sensor.get('eui', '')).upper() != eui_upper:
                continue
            if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                continue
            target_index = idx
            existing_sensor = dict(sensor)
            break

        if target_index is None or not existing_sensor:
            return jsonify({'success': False, 'message': 'Sensor not found'}), 404

        previous_snapshot = _sensor_audit_snapshot(existing_sensor)
        merged = dict(existing_sensor)
        merged.update(dict(patch or {}))
        merged['eui'] = eui_upper
        merged['tenant_id'] = _tenant_id_from_sensor(existing_sensor)
        if existing_sensor.get('created_at'):
            merged['created_at'] = existing_sensor.get('created_at')
        if 'shared_tenants' not in merged and existing_sensor.get('shared_tenants') is not None:
            merged['shared_tenants'] = existing_sensor.get('shared_tenants')

        data = _normalize_sensor_payload(merged)

        compare_fields = [
            'nwKey',
            'shortAddr',
            'bidi',
            'name',
            'tags',
            'sensor_profile',
            'gps_lat',
            'gps_lng',
            'payload_decoder',
            'environment_context',
            'reporting_mode',
            'expected_interval_seconds',
            'stale_after_hours',
        ]
        changed_fields = [field for field in compare_fields if existing_sensor.get(field) != data.get(field)]

        sensors[target_index] = data
        _save_all_sensors(sensors)

        _try_record_inventory_event(
            "sensor",
            "updated",
            data.get("eui", ""),
            {
                "short_addr": data.get("shortAddr", ""),
                "bidi": bool(data.get("bidi", False)),
                "nwkey_present": bool(data.get("nwKey")),
                "name": data.get("name", ""),
                "tags_count": len(data.get("tags", [])),
                "sensor_profile": data.get("sensor_profile", "auto"),
                "payload_decoder": data.get("payload_decoder", "auto"),
                "environment_context": data.get("environment_context", "auto"),
                "reporting_mode": data.get("reporting_mode", "auto"),
                "expected_interval_seconds": data.get("expected_interval_seconds"),
                "stale_after_hours": data.get("stale_after_hours"),
                "gps_lat": data.get("gps_lat"),
                "gps_lng": data.get("gps_lng"),
                "tenant_id": active_tenant,
            }
        )

        _upsert_device_gps_position("sensor", data.get("eui", ""), data.get("gps_lat"), data.get("gps_lng"))
        new_snapshot = _sensor_audit_snapshot(data)
        _record_admin_audit(
            action='sensor.update',
            entity='sensor',
            target_id=data.get("eui", ""),
            status='success',
            details={
                "tenant_id": active_tenant,
                "changed_fields": _audit_changed_fields(previous_snapshot, new_snapshot),
                "before": previous_snapshot,
                "after": new_snapshot,
            },
        )

        global tls_server_instance
        tls_server = tls_server_instance
        should_trigger_runtime_attach = any(field in {'nwKey', 'shortAddr', 'bidi'} for field in changed_fields)
        attach_targets_after_save = _normalize_base_station_route_list(data.get("attached_base_stations", []))

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()

                if not should_trigger_runtime_attach:
                    return jsonify({'success': True, 'message': 'Senzor bol uložený. Nastavenia prepojenia zostali bez zmeny.'})

                if hasattr(tls_server, 'connected_base_stations') and tls_server.connected_base_stations:
                    if hasattr(tls_server, 'attach_sensor_sync'):
                        attached_count = tls_server.attach_sensor_sync(
                            data['eui'],
                            attach_targets_after_save or None,
                        )
                        if attached_count > 0:
                            return jsonify({'success': True, 'message': f'Senzor bol uložený a prepojený s {attached_count} základňovými stanicami.'})
                        return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí, keď budú základňové stanice dostupné.'})
                    return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí po obnovení spojenia.'})

                return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí po opätovnom pripojení základňových staníc.'})
            except Exception as e:
                print(f"Error notifying TLS server: {e}")
                return jsonify({'success': True, 'message': 'Senzor bol uložený. Dokončenie prepojenia sa oneskorilo.'})

        return jsonify({'success': True, 'message': 'Senzor bol uložený. Prepojenie sa dokončí neskôr.'})
    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500

@app.route('/api/sensors/<eui>', methods=['DELETE'])
@login_required
@permission_required('can_add_sensors')
def delete_sensor(eui):
    sensors = _load_all_sensors()
    active_tenant = _active_tenant_id()
    deleted_sensor = None
    kept = []
    for sensor in sensors:
        same_eui = str(sensor.get('eui', '')).upper() == eui.upper()
        same_tenant = _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant)
        if same_eui and same_tenant and deleted_sensor is None:
            deleted_sensor = sensor
            continue
        kept.append(sensor)

    try:
        _save_all_sensors(kept)
        _try_record_inventory_event(
            "sensor",
            "deleted",
            eui,
            {
                "short_addr": (deleted_sensor or {}).get("shortAddr", ""),
                "bidi": bool((deleted_sensor or {}).get("bidi", False)),
                "nwkey_present": bool((deleted_sensor or {}).get("nwKey")),
                "name": (deleted_sensor or {}).get("name", ""),
                "tags_count": len(_normalize_sensor_tags((deleted_sensor or {}).get("tags", []))),
                "gps_lat": (deleted_sensor or {}).get("gps_lat"),
                "gps_lng": (deleted_sensor or {}).get("gps_lng"),
                "tenant_id": active_tenant,
            }
        )
        _remove_device_position("sensor", eui)
        _record_admin_audit(
            action='sensor.delete',
            entity='sensor',
            target_id=eui,
            status='success',
            details={
                "tenant_id": active_tenant,
                "before": _sensor_audit_snapshot(deleted_sensor),
            },
        )
        return jsonify({'success': True, 'message': 'Sensor deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/<eui>/share', methods=['GET', 'POST'])
@login_required
@permission_required('can_edit_sensors')
def manage_sensor_share(eui):
    """Get or update the shared_tenants list for a sensor (owner only)."""
    eui_upper = str(eui or "").strip().upper()
    sensors = _load_all_sensors()
    active_tenant = _active_tenant_id()

    sensor = next((s for s in sensors if str(s.get("eui", "")).upper() == eui_upper), None)
    if not sensor:
        return jsonify({"success": False, "message": "Sensor not found"}), 404

    owner_tenant = _tenant_id_from_sensor(sensor)
    if not _tenant_matches(owner_tenant, active_tenant):
        return jsonify({"success": False, "message": "Only the owner tenant can manage sharing"}), 403

    if request.method == "GET":
        shared = [_normalize_tenant_id(t, fallback=_default_tenant_id())
                  for t in (sensor.get("shared_tenants") or [])]
        return jsonify({"success": True, "owner_tenant": owner_tenant, "shared_tenants": shared})

    data = request.json or {}
    action = str(data.get("action") or "").strip().lower()
    tenant = _normalize_tenant_id(data.get("tenant"), fallback=None)

    if action not in ("add", "remove"):
        return jsonify({"success": False, "message": "action must be 'add' or 'remove'"}), 400
    if not tenant:
        return jsonify({"success": False, "message": "tenant is required"}), 400
    if tenant == owner_tenant:
        return jsonify({"success": False, "message": "Cannot share with the owner tenant"}), 400

    previous_shared = [_normalize_tenant_id(t, fallback=_default_tenant_id())
              for t in (sensor.get("shared_tenants") or [])]
    shared = list(previous_shared)
    if action == "add":
        if tenant not in shared:
            shared.append(tenant)
    else:
        shared = [t for t in shared if t != tenant]

    sensor["shared_tenants"] = shared
    _save_all_sensors(sensors)
    _record_admin_audit(
        action='sensor.share_update',
        entity='sensor',
        target_id=eui_upper,
        status='success',
        details={
            "tenant_id": active_tenant,
            "share_action": action,
            "shared_tenant": tenant,
            "before": {"shared_tenants": previous_shared},
            "after": {"shared_tenants": shared},
            "changed_fields": ["shared_tenants"] if previous_shared != shared else [],
        },
    )
    logger.info(f"Sensor {eui_upper} shared_tenants updated by {session.get('username')}: {shared}")
    return jsonify({"success": True, "shared_tenants": shared})


@app.route('/api/sensors/<eui>/attach', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def attach_sensor(eui):
    """Set sensor attach mapping (attach + detach in one endpoint)."""
    try:
        eui_upper = str(eui or "").strip().upper()
        active_tenant = _active_tenant_id()
        target_sensor = next((
            s for s in _load_all_sensors()
            if str(s.get("eui", "")).strip().upper() == eui_upper
            and _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
        ), None)
        if not target_sensor:
            return jsonify({'success': False, 'message': 'Senzor sa nenašiel v aktuálnom priestore.'}), 404
        previous_targets = _normalize_base_station_route_list(target_sensor.get("attached_base_stations", []))

        payload = request.get_json(silent=True) or {}
        requested_bases = payload.get('base_stations')
        if requested_bases is None:
            return jsonify({'success': False, 'message': 'Vyberte jednu alebo viac základňových staníc na prepojenie.'}), 400

        if isinstance(requested_bases, str):
            requested_bases = [part.strip() for part in requested_bases.split(',') if part and part.strip()]
        if not isinstance(requested_bases, list):
            return jsonify({'success': False, 'message': 'Zoznam základňových staníc musí byť vo forme poľa EUI identifikátorov.'}), 400

        selected_base_stations = _normalize_base_station_route_list(requested_bases)
        if requested_bases and not selected_base_stations:
            return jsonify({'success': False, 'message': 'Nebola vybraná žiadna platná základňová stanica.'}), 400

        global tls_server_instance
        tls_server = tls_server_instance

        # Empty selection means explicit detach / clear mapping.
        if not selected_base_stations:
            _update_sensor_attached_base_stations(eui_upper, [], tenant_id=active_tenant)

            runtime_detached = None
            runtime_error = None
            if tls_server and hasattr(tls_server, 'detach_sensor_sync'):
                try:
                    runtime_detached = bool(tls_server.detach_sensor_sync(eui))
                except Exception as exc:
                    runtime_error = str(exc)

            if tls_server and hasattr(tls_server, 'reload_sensor_config'):
                try:
                    tls_server.reload_sensor_config()
                except Exception:
                    pass

            message = f'Senzor {eui} bol odpojený od všetkých základňových staníc.'
            if runtime_detached is True:
                message = f'{message} Zmena sa prejavila aj na aktuálne pripojených staniciach.'
            elif runtime_detached is False:
                message = f'{message} Dokončenie odpojenia prebehne po obnovení spojenia so stanicami.'
            if runtime_error:
                message = f'{message} Poznámka: {runtime_error}'

            _record_admin_audit(
                action='sensor.detach',
                entity='sensor',
                target_id=eui_upper,
                status='success',
                details={
                    'tenant_id': active_tenant,
                    'mapping_cleared': True,
                    'before': {'attached_base_stations': previous_targets},
                    'after': {'attached_base_stations': []},
                    'changed_fields': ['attached_base_stations'] if previous_targets else [],
                    'runtime_detached': bool(runtime_detached),
                    'runtime_error': runtime_error,
                },
            )

            return jsonify({
                'success': True,
                'message': message,
                'attached_count': 0,
                'pending_count': 0,
                'requested_base_stations': [],
                'online_base_stations': [],
                'pending_base_stations': [],
                'mapping_cleared': True,
            })

        # Validate that selected targets belong to inventory scope (tenant + currently connected).
        bs_config = _filter_base_stations_for_tenant(
            load_base_station_config().get("base_stations", {}),
            tenant_id=active_tenant,
        )
        configured_targets = {
            _normalize_eui_upper(bs_eui)
            for bs_eui in (bs_config or {}).keys()
            if _normalize_eui_upper(bs_eui)
        }

        connected_targets = set()
        if tls_server and hasattr(tls_server, 'connected_base_stations'):
            connected_targets = {
                _normalize_eui_upper(bs_eui)
                for bs_eui in (tls_server.connected_base_stations or {}).values()
                if _normalize_eui_upper(bs_eui)
            }

        allowed_targets = configured_targets | connected_targets
        unknown_targets = [bs for bs in selected_base_stations if bs not in allowed_targets]
        if unknown_targets:
            return jsonify({
                'success': False,
                'message': f"Niektoré vybrané základňové stanice nie sú dostupné v tomto priestore: {', '.join(unknown_targets)}"
            }), 400

        # Always persist desired attach mapping, even if target BS is offline.
        _update_sensor_attached_base_stations(eui_upper, selected_base_stations, tenant_id=active_tenant)
        _record_admin_audit(
            action='sensor.attach',
            entity='sensor',
            target_id=eui_upper,
            status='success',
            details={
                'tenant_id': active_tenant,
                'before': {'attached_base_stations': previous_targets},
                'after': {'attached_base_stations': selected_base_stations},
                'changed_fields': ['attached_base_stations'] if previous_targets != selected_base_stations else [],
                'requested_base_stations': selected_base_stations,
                'requested_count': len(selected_base_stations),
                'connected_count_now': len(connected_targets),
            },
        )

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()
            except Exception:
                pass

        if tls_server and hasattr(tls_server, 'attach_sensor_sync'):
            attached_count = int(tls_server.attach_sensor_sync(eui, selected_base_stations) or 0)
            total_targets = len(selected_base_stations)
            pending_count = max(0, total_targets - attached_count)
            pending_targets = [bs for bs in selected_base_stations if bs not in connected_targets]
            online_targets = [bs for bs in selected_base_stations if bs in connected_targets]

            if attached_count > 0 and pending_count > 0:
                return jsonify({
                    'success': True,
                    'message': f'Senzor {eui} bol uložený. {attached_count} základňových staníc je už prepojených, ďalších {pending_count} sa doplní po obnovení spojenia.',
                    'attached_count': attached_count,
                    'pending_count': pending_count,
                    'requested_base_stations': selected_base_stations,
                    'online_base_stations': online_targets,
                    'pending_base_stations': pending_targets
                })
            if attached_count > 0:
                return jsonify({
                    'success': True,
                    'message': f'Senzor {eui} je prepojený s {attached_count} základňovými stanicami.',
                    'attached_count': attached_count,
                    'pending_count': 0,
                    'requested_base_stations': selected_base_stations,
                    'online_base_stations': online_targets,
                    'pending_base_stations': []
                })

            return jsonify({
                'success': True,
                'message': f'Prepojenie senzora {eui} bolo uložené. Dokončí sa po opätovnom pripojení základňových staníc.',
                'attached_count': 0,
                'pending_count': len(selected_base_stations),
                'requested_base_stations': selected_base_stations,
                'online_base_stations': [],
                'pending_base_stations': selected_base_stations
            })

        return jsonify({
            'success': True,
            'message': f'Prepojenie senzora {eui} bolo uložené. Dokončí sa, keď budú základňové stanice dostupné.',
            'attached_count': 0,
            'pending_count': len(selected_base_stations),
            'requested_base_stations': selected_base_stations,
            'online_base_stations': [],
            'pending_base_stations': selected_base_stations
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/sensors/telemetry/history', methods=['GET'])
@login_required
def get_sensor_telemetry_history():
    try:
        active_tenant = _active_tenant_id()
        sensor_eui_arg = request.args.get('sensor_eui')
        query_tenant = _resolve_query_tenant_for_sensor(sensor_eui_arg, active_tenant) if sensor_eui_arg else active_tenant
        result = _timescale_fetch_sensor_telemetry_history(
            tenant_id=query_tenant,
            sensor_eui=sensor_eui_arg,
            base_station_eui=request.args.get('base_station_eui'),
            profile=request.args.get('profile'),
            minutes=request.args.get('minutes', 1440),
            limit=request.args.get('limit', 100),
            offset=request.args.get('offset', 0),
        )
        return jsonify(result), (200 if result.get("success", False) else 503)
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route('/api/sensors/telemetry/history/export', methods=['GET'])
@login_required
@internal_portal_required
def export_sensor_telemetry_history():
    try:
        active_tenant = _active_tenant_id()
        sensor_eui_arg = request.args.get('sensor_eui')
        query_tenant = _resolve_query_tenant_for_sensor(sensor_eui_arg, active_tenant) if sensor_eui_arg else active_tenant
        result = _timescale_fetch_sensor_telemetry_history(
            tenant_id=query_tenant,
            sensor_eui=sensor_eui_arg,
            base_station_eui=request.args.get('base_station_eui'),
            profile=request.args.get('profile'),
            minutes=request.args.get('minutes', 1440),
            limit=request.args.get('limit', 2000),
            offset=0,
        )
        if not result.get("success"):
            return jsonify(result), 503

        sensor_suffix = str(request.args.get('sensor_eui') or 'all-sensors').strip().lower()
        filename = f"sensor-telemetry-{sensor_suffix}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
        return _telemetry_csv_response(result.get("rows") or [], filename=filename)
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


def _sanitize_sensor_detail_for_customer(response: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = dict(response or {})

    latest = sanitized.get("latest_uplink")
    if isinstance(latest, dict):
        latest_copy = dict(latest)
        latest_copy.pop("raw_hex", None)
        latest_copy.pop("raw_dec", None)
        decoded = latest_copy.get("decoded")
        if isinstance(decoded, dict):
            decoded_copy = dict(decoded)
            decoded_copy.pop("model_hint", None)
            latest_copy["decoded"] = decoded_copy
        sanitized["latest_uplink"] = latest_copy

    history = []
    for row in sanitized.get("uplink_history") or []:
        if not isinstance(row, dict):
            history.append(row)
            continue
        row_copy = dict(row)
        row_copy.pop("raw_hex", None)
        row_copy.pop("raw_dec", None)
        decoded = row_copy.get("decoded")
        if isinstance(decoded, dict):
            decoded_copy = dict(decoded)
            decoded_copy.pop("model_hint", None)
            row_copy["decoded"] = decoded_copy
        history.append(row_copy)
    sanitized["uplink_history"] = history

    return sanitized


# ── Alert CRUD routes ────────────────────────────────────────────────────────

import uuid as _uuid_mod

@app.route('/api/alerts', methods=['GET'])
@login_required
def api_alerts_list():
    """List alerts, optionally filtered by ?sensor_eui=<eui>."""
    active_tenant = _active_tenant_id()
    sensor_eui = request.args.get('sensor_eui', '').strip().upper()
    visible_sensors = _build_visible_sensor_lookup(active_tenant)
    alerts = [
        {
            **a,
            "kind": _normalize_alert_kind(a.get("kind")),
            "enabled": bool(a.get("enabled", True)),
        }
        for a in _load_alerts()
        if str(a.get('sensor_eui', '')).strip().upper() in visible_sensors
    ]
    if sensor_eui:
        alerts = [a for a in alerts if str(a.get('sensor_eui', '')).upper() == sensor_eui]
    return jsonify({"success": True, "alerts": alerts})


@app.route('/api/alerts/options', methods=['GET'])
@login_required
def api_alert_options():
    """Return alert-capable metrics for a tenant-visible sensor."""
    active_tenant = _active_tenant_id()
    sensor_eui = request.args.get('sensor_eui', '').strip().upper()
    if not sensor_eui:
        return jsonify({"success": False, "message": "Sensor EUI is required.", "metrics": []}), 400

    visible_sensors = _build_visible_sensor_lookup(active_tenant)
    sensor_config = visible_sensors.get(sensor_eui)
    if not sensor_config:
        return jsonify({"success": False, "message": "Sensor not found.", "metrics": []}), 404

    metrics = _build_sensor_alert_metric_options(sensor_config, active_tenant)
    availability = _sensor_availability_snapshot(sensor_config, active_tenant)
    return jsonify({
        "success": True,
        "sensor_eui": sensor_eui,
        "metrics": metrics,
        "sensor": {
            "eui": sensor_eui,
            "name": str(sensor_config.get("name") or "").strip(),
            "payload_decoder": str(sensor_config.get("payload_decoder") or "auto"),
            "activity_status": availability.get("activity_status", "no_data"),
        },
    })


@app.route('/api/alerts', methods=['POST'])
@login_required
@permission_required('can_manage_alerts')
def api_alerts_create():
    """Create a new alert."""
    try:
        active_tenant = _active_tenant_id()
        body = request.get_json(force=True) or {}
        sensor_eui = str(body.get('sensor_eui') or '').strip().upper()
        kind = _normalize_alert_kind(body.get('kind'))
        metric = str(body.get('metric') or '').strip()
        condition = str(body.get('condition') or '').strip()
        threshold = body.get('threshold')
        name = str(body.get('name') or '').strip()
        severity = str(body.get('severity') or 'warning').strip()
        enabled = bool(body.get('enabled', True))
        visible_sensors = _build_visible_sensor_lookup(active_tenant)
        sensor_config = visible_sensors.get(sensor_eui)

        if not sensor_config:
            return jsonify({"success": False, "message": "Senzor sa nenašiel alebo k nemu nemáte prístup."}), 404
        if severity not in ('warning', 'critical'):
            severity = 'warning'
        if kind != 'threshold':
            metric = ''
            condition = ''
            threshold = None

        if kind == 'threshold':
            if not metric or condition not in ('gt', 'gte', 'lt', 'lte', 'eq'):
                return jsonify({"success": False, "message": "Missing threshold fields or invalid condition."}), 400
            allowed_metrics = {
                str(item.get("value") or "").strip()
                for item in _build_sensor_alert_metric_options(sensor_config, active_tenant)
            }
            if metric not in allowed_metrics:
                return jsonify({"success": False, "message": "Selected metric is not available for this sensor."}), 400
            try:
                threshold = float(threshold)
            except (TypeError, ValueError):
                return jsonify({"success": False, "message": "Invalid threshold value."}), 400
        else:
            metric = '__sensor_offline__'
            condition = 'gt'
            threshold = 0.0

        if kind != 'threshold':
            metric = ''
            condition = ''
            threshold = None

        alert = {
            "id": str(_uuid_mod.uuid4()),
            "sensor_eui": sensor_eui,
            "kind": kind,
            "name": name or (f"{metric} {condition} {threshold}" if kind == "threshold" else f"{sensor_eui} offline"),
            "metric": metric,
            "condition": condition,
            "threshold": threshold,
            "severity": severity,
            "enabled": enabled,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        alerts = _load_alerts()
        alerts.append(alert)
        _save_alerts(alerts)
        return jsonify({"success": True, "alert": alert}), 201
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route('/api/alerts/<alert_id>', methods=['PUT'])
@login_required
@permission_required('can_manage_alerts')
def api_alerts_update(alert_id):
    """Update an existing alert."""
    try:
        active_tenant = _active_tenant_id()
        visible_sensors = _build_visible_sensor_lookup(active_tenant)
        alerts = _load_alerts()
        target = next((a for a in alerts if a.get('id') == alert_id), None)
        if target and str(target.get('sensor_eui', '')).strip().upper() not in visible_sensors:
            return jsonify({"success": False, "message": "K tomuto upozorneniu nemáte prístup."}), 404
        if not target:
            return jsonify({"success": False, "message": "Upozornenie sa nenašlo."}), 404
        body = request.get_json(force=True) or {}
        target['kind'] = _normalize_alert_kind(target.get('kind'))
        for field in ('name', 'severity'):
            if field in body and str(body[field]).strip():
                target[field] = str(body[field]).strip()
        if target['kind'] == 'threshold' and 'metric' in body and str(body['metric']).strip():
            sensor_config = visible_sensors.get(str(target.get('sensor_eui', '')).strip().upper())
            proposed_metric = str(body['metric']).strip()
            allowed_metrics = {
                str(item.get("value") or "").strip()
                for item in _build_sensor_alert_metric_options(sensor_config, active_tenant)
            }
            if proposed_metric in allowed_metrics:
                target['metric'] = proposed_metric
        if target['kind'] == 'threshold' and 'condition' in body and body['condition'] in ('gt', 'gte', 'lt', 'lte', 'eq'):
            target['condition'] = body['condition']
        if target['kind'] == 'threshold' and 'threshold' in body:
            try:
                target['threshold'] = float(body['threshold'])
            except (TypeError, ValueError):
                pass
        if target['kind'] != 'threshold':
            target['metric'] = ''
            target['condition'] = ''
            target['threshold'] = None
        if 'enabled' in body:
            target['enabled'] = bool(body['enabled'])
        _save_alerts(alerts)
        return jsonify({"success": True, "alert": target})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route('/api/alerts/<alert_id>', methods=['DELETE'])
@login_required
@permission_required('can_manage_alerts')
def api_alerts_delete(alert_id):
    """Delete an alert."""
    active_tenant = _active_tenant_id()
    visible_sensors = _build_visible_sensor_lookup(active_tenant)
    alerts = _load_alerts()
    target = next((a for a in alerts if a.get('id') == alert_id), None)
    if not target:
        return jsonify({"success": False, "message": "Upozornenie sa nenašlo."}), 404
    if str(target.get('sensor_eui', '')).strip().upper() not in visible_sensors:
        return jsonify({"success": False, "message": "K tomuto upozorneniu nemáte prístup."}), 404
    new_list = [a for a in alerts if a.get('id') != alert_id]
    if len(new_list) == len(alerts):
        return jsonify({"success": False, "message": "Upozornenie sa nenašlo."}), 404
    _save_alerts(new_list)
    return jsonify({"success": True})


@app.route('/api/alerts/triggered', methods=['GET'])
@login_required
@internal_portal_required
def api_alerts_triggered():
    """Evaluate all enabled alerts against latest sensor values and return triggered ones."""
    try:
        active_tenant = _active_tenant_id()
        triggered = _evaluate_triggered_alerts(active_tenant, persist_state=True)
        return jsonify({"success": True, "triggered": triggered})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc), "triggered": []}), 500


@app.route('/api/incidents', methods=['GET'])
@login_required
def api_incidents():
    """Return current operational incidents from sensor state and alert rules."""
    try:
        active_tenant = _active_tenant_id()
        cached_payload = _get_cached_incident_feed_payload(active_tenant)
        if cached_payload:
            return jsonify(cached_payload)
        incidents = _build_current_incidents(active_tenant)
        payload = {
            "success": True,
            "incidents": incidents,
            "summary": {
                "total": len(incidents),
                "critical": sum(1 for item in incidents if item.get("tier") == "error"),
                "warning": sum(1 for item in incidents if item.get("tier") != "error"),
            },
        }
        _store_cached_incident_feed_payload(active_tenant, payload)
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc), "incidents": [], "summary": {"total": 0, "critical": 0, "warning": 0}}), 500

@app.route('/api/alerts/history', methods=['GET'])
@login_required
def api_alerts_history():
    """Return alert event history (triggered/resolved), newest first."""
    try:
        active_tenant = _active_tenant_id()
        limit = min(int(request.args.get('limit', 200)), 500)
        fetch_limit = 500
        sensor_filter = str(request.args.get('sensor_eui') or '').strip().upper()
        event_filter = str(request.args.get('event_type') or '').strip().lower()
        severity_filter = str(request.args.get('severity') or '').strip().lower()
        source_filter = str(request.args.get('source') or '').strip().lower()
        search_filter = str(request.args.get('q') or '').strip().lower()

        # Build sensor name lookup
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), active_tenant)
        sensor_names = {str(s.get('eui', '')).upper(): s.get('name') or str(s.get('eui', '')) for s in sensors}

        # Build alert rule name lookup
        alert_rules = {str(a.get('id', '')): a for a in _load_alerts()}

        def _history_event_from_payload(ts_value: Any, alert_id: Any, sensor_eui: Any, event_type: Any, severity: Any, payload: Any) -> Dict[str, Any]:
            p = payload if isinstance(payload, dict) else {}
            eui = str(sensor_eui or p.get('sensor_eui') or '').upper()
            rule = alert_rules.get(str(alert_id) or '') or {}
            source = str(
                p.get('source')
                or ('offline_rule' if _normalize_alert_kind(rule.get('kind') or p.get('kind')) == 'sensor_offline' else 'threshold')
            ).strip().lower()
            kind = str(rule.get('kind') or p.get('kind') or ('activity' if source == 'activity' else 'threshold')).strip().lower()
            return {
                "ts": ts_value.isoformat() if hasattr(ts_value, 'isoformat') else str(ts_value or ''),
                "alert_id": str(alert_id or p.get('id') or ''),
                "sensor_eui": eui,
                "sensor_name": sensor_names.get(eui, p.get('sensor_name') or eui),
                "event_type": str(event_type or '').strip().lower(),
                "severity": "critical" if str(severity or p.get('severity') or '').strip().lower() == 'critical' else "warning",
                "source": source,
                "kind": kind,
                "name": str(rule.get('name') or p.get('name') or p.get('sensor_name') or sensor_names.get(eui, eui) or '').strip(),
                "metric": str(rule.get('metric') or p.get('metric') or '').strip(),
                "condition": str(rule.get('condition') or p.get('condition') or '').strip(),
                "threshold": p.get('threshold'),
                "reason": str(p.get('reason') or p.get('desc') or '').strip(),
                "desc": str(p.get('desc') or p.get('reason') or '').strip(),
                "trigger_value": p.get('current_value') if p.get('current_value') is not None else p.get('trigger_value'),
                "val_str": p.get('val_str') if p.get('val_str') not in (None, '') else p.get('valStr'),
                "trigger_status": p.get('current_status') or p.get('trigger_status'),
                "triggered_at": p.get('triggered_at'),
                "resolved_at": p.get('resolved_at'),
                "hours_since_last_seen": p.get('hours_since_last_seen'),
                "link_url": p.get('link_url'),
                "rule_id": p.get('rule_id'),
            }

        def _history_event_matches(event: Dict[str, Any]) -> bool:
            if sensor_filter and str(event.get('sensor_eui') or '').upper() != sensor_filter:
                return False
            if event_filter and str(event.get('event_type') or '').lower() != event_filter:
                return False
            if severity_filter and str(event.get('severity') or '').lower() != severity_filter:
                return False
            if source_filter:
                source = str(event.get('source') or '').lower()
                if source_filter == 'rules':
                    if source not in {'threshold', 'offline_rule'}:
                        return False
                elif source != source_filter:
                    return False
            if search_filter:
                haystack = " ".join([
                    str(event.get('name') or ''),
                    str(event.get('sensor_name') or ''),
                    str(event.get('sensor_eui') or ''),
                    str(event.get('reason') or ''),
                    str(event.get('desc') or ''),
                    str(event.get('metric') or ''),
                    str(event.get('source') or ''),
                    str(event.get('kind') or ''),
                    str(event.get('trigger_status') or ''),
                ]).lower()
                if search_filter not in haystack:
                    return False
            return True

        events = []

        # Try TimescaleDB first
        conn = None
        try:
            conn, err = _timescale_connect()
            if conn is not None:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT ts, alert_id, sensor_eui, event_type, severity, payload
                        FROM alert_events
                        WHERE tenant_id = %s
                        ORDER BY ts DESC
                        LIMIT %s
                    """, (_normalize_tenant_id(active_tenant, fallback=_default_tenant_id()), fetch_limit))
                    for ts, alert_id, sensor_eui, event_type, severity, payload in (cur.fetchall() or []):
                        events.append(_history_event_from_payload(ts, alert_id, sensor_eui, event_type, severity, payload))
        except Exception:
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

        # File-based fallback if DB had no results
        if not events:
            for e in sorted(_load_alert_events_file(), key=lambda x: x.get('ts', ''), reverse=True)[:fetch_limit]:
                events.append(_history_event_from_payload(
                    e.get('ts', ''),
                    e.get('alert_id', ''),
                    e.get('sensor_eui', ''),
                    e.get('event_type', ''),
                    e.get('severity', 'warning'),
                    e,
                ))

        events = [event for event in events if _history_event_matches(event)]
        return jsonify({"success": True, "events": events[:limit]})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc), "events": []}), 500


# ── End alert routes ─────────────────────────────────────────────────────────


@app.route('/api/sensors/<eui>/gps', methods=['POST'])
@login_required
def api_sensor_gps_update(eui):
    """Update GPS coordinates for a sensor (viewer-accessible)."""
    try:
        body = request.get_json(force=True) or {}
        try:
            gps_lat = float(body['gps_lat'])
            gps_lng = float(body['gps_lng'])
        except (KeyError, TypeError, ValueError):
            return jsonify({"success": False, "message": "Neplatné súradnice"}), 400
        if not (-90 <= gps_lat <= 90) or not (-180 <= gps_lng <= 180):
            return jsonify({"success": False, "message": "Súradnice mimo rozsahu"}), 400
        eui_upper = _normalize_eui_upper(eui)
        active_tenant = _active_tenant_id()
        sensors = _load_all_sensors()
        found = False
        for s in sensors:
            if (_normalize_eui_upper(s.get('eui', '')) == eui_upper
                    and (_tenant_matches(_tenant_id_from_sensor(s), active_tenant)
                         or _sensor_shared_with_tenant(s, active_tenant))):
                before_snapshot = _sensor_audit_snapshot(s)
                s['gps_lat'] = round(gps_lat, 8)
                s['gps_lng'] = round(gps_lng, 8)
                after_snapshot = _sensor_audit_snapshot(s)
                found = True
                break
        if not found:
            return jsonify({"success": False, "message": "Senzor nenájdený"}), 404
        _save_all_sensors(sensors)
        _upsert_device_gps_position("sensor", eui_upper, round(gps_lat, 8), round(gps_lng, 8))
        _record_admin_audit(
            action='sensor.update_gps',
            entity='sensor',
            target_id=eui_upper,
            status='success',
            details={
                'tenant_id': active_tenant,
                'before': before_snapshot,
                'after': after_snapshot,
                'changed_fields': _audit_changed_fields(before_snapshot, after_snapshot),
            },
        )
        return jsonify({"success": True, "eui": eui_upper, "gps_lat": gps_lat, "gps_lng": gps_lng})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route('/api/sensors/<eui>/marker-color', methods=['POST'])
@login_required
def api_sensor_marker_color(eui):
    """Set or clear custom marker color for a sensor."""
    try:
        body = request.get_json(force=True) or {}
        color = str(body.get('color') or '').strip()
        if color and not re.match(r'^#[0-9a-fA-F]{6}$', color):
            return jsonify({"success": False, "message": "Neplatná farba (očakáva sa #RRGGBB)"}), 400
        active_tenant = _active_tenant_id()
        eui_upper = _normalize_eui_upper(eui)
        sensors = _load_all_sensors()
        found = False
        for s in sensors:
            if (_normalize_eui_upper(s.get('eui', '')) == eui_upper
                    and (_tenant_matches(_tenant_id_from_sensor(s), active_tenant)
                         or _sensor_shared_with_tenant(s, active_tenant))):
                before_snapshot = _sensor_audit_snapshot(s)
                if color:
                    s['marker_color'] = color
                else:
                    s.pop('marker_color', None)
                after_snapshot = _sensor_audit_snapshot(s)
                found = True
                break
        if not found:
            return jsonify({"success": False, "message": "Senzor nenájdený"}), 404
        _save_all_sensors(sensors)
        _record_admin_audit(
            action='sensor.update_marker_color',
            entity='sensor',
            target_id=eui_upper,
            status='success',
            details={
                'tenant_id': active_tenant,
                'before': before_snapshot,
                'after': after_snapshot,
                'changed_fields': _audit_changed_fields(before_snapshot, after_snapshot),
            },
        )
        return jsonify({"success": True, "marker_color": color or None})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route('/api/sensors/<eui>/detach', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def detach_sensor(eui):
    """Detach a specific sensor from all base stations (desired-state first, runtime best-effort)."""
    try:
        active_tenant = _active_tenant_id()
        eui_upper = str(eui or "").strip().upper()
        target_sensor = next((
            s for s in _load_all_sensors()
            if str(s.get("eui", "")).strip().upper() == eui_upper
            and _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
        ), None)
        if not target_sensor:
            return jsonify({'success': False, 'message': 'Senzor sa nenašiel v aktuálnom priestore.'}), 404
        previous_targets = _normalize_base_station_route_list(target_sensor.get("attached_base_stations", []))

        # Always clear desired attach mapping first (works even if no online BS).
        _update_sensor_attached_base_stations(eui_upper, [], tenant_id=active_tenant)

        global tls_server_instance
        tls_server = tls_server_instance
        runtime_detached = None
        runtime_error = None

        if tls_server and hasattr(tls_server, 'detach_sensor_sync'):
            try:
                runtime_detached = bool(tls_server.detach_sensor_sync(eui))
            except Exception as exc:
                runtime_error = str(exc)

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()
            except Exception:
                pass

        if runtime_detached is True:
            message = f'Senzor {eui} bol odpojený a prepojenie bolo vymazané.'
        elif runtime_detached is False:
            message = f'Prepojenie senzora {eui} bolo vymazané. Dokončenie odpojenia prebehne po obnovení spojenia so stanicami.'
        else:
            message = f'Prepojenie senzora {eui} bolo vymazané.'

        if runtime_error:
            message = f'{message} Poznámka: {runtime_error}'

        _record_admin_audit(
            action='sensor.detach',
            entity='sensor',
            target_id=eui_upper,
            status='success',
            details={
                'tenant_id': active_tenant,
                'mapping_cleared': True,
                'before': {'attached_base_stations': previous_targets},
                'after': {'attached_base_stations': []},
                'changed_fields': ['attached_base_stations'] if previous_targets else [],
                'runtime_detached': bool(runtime_detached),
                'runtime_error': runtime_error,
            },
        )

        return jsonify({
            'success': True,
            'message': message,
            'runtime_detached': bool(runtime_detached),
            'mapping_cleared': True,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

def _convert_topology_timestamps(receiving_bases, sensor_last_seen):
    """Use the sensor's real last_seen timestamp for all base stations"""
    if not receiving_bases:
        return {}
    result = {}
    for bs_eui, data in receiving_bases.items():
        result[bs_eui] = data.copy()
        if sensor_last_seen and sensor_last_seen > 0:
            result[bs_eui]['last_seen'] = sensor_last_seen
    return result

@app.route('/api/sensors/<eui>/details', methods=['GET'])
@login_required
def get_sensor_details(eui):
    """Get detailed statistics for a specific sensor"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        eui_upper = eui.upper()
        active_tenant = _active_tenant_id()
        
        # Get base sensor config
        sensor_config = None
        try:
            sensors = _load_all_sensors()
            for s in sensors:
                if str(s.get('eui', '')).upper() == eui_upper:
                    if (
                        _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
                        or _sensor_shared_with_tenant(s, active_tenant)
                    ):
                        sensor_config = dict(s)
                        sensor_config["tenant_id"] = _tenant_id_from_sensor(s)
                        break
        except:
            pass
        
        if not sensor_config:
            return jsonify({'success': False, 'message': 'Sensor not found'})
        sensor_config = _normalize_sensor_payload(sensor_config)
        sensor_profile = _infer_sensor_profile(sensor_config)
        
        # Get packet statistics
        stats = {}
        if tls_server and hasattr(tls_server, 'sensor_packet_stats'):
            stats = tls_server.sensor_packet_stats.get(eui_upper, {})
        stats_source = 'runtime' if stats else 'none'

        snapshot_payload = {}
        try:
            query_tenant = _resolve_query_tenant_for_sensor(eui_upper, active_tenant)
            snapshot_map = _timescale_fetch_latest_sensor_snapshots(query_tenant)
            snapshot_entry = snapshot_map.get(eui_upper) or {}
            snapshot_payload = snapshot_entry.get("payload") if isinstance(snapshot_entry, dict) else {}
        except Exception:
            snapshot_payload = {}

        if not stats and snapshot_payload:
            stats = {
                'packets_received': int(snapshot_payload.get('packets_received', 0) or 0),
                'packets_lost': int(snapshot_payload.get('packets_lost', 0) or 0),
                'last_seen': float(snapshot_payload.get('last_seen_ts', 0) or 0),
                'frame_counter': int(snapshot_payload.get('frame_counter', 0) or 0),
            }
            avg_snr_snapshot = snapshot_payload.get('avg_snr')
            avg_rssi_snapshot = snapshot_payload.get('avg_rssi')
            if avg_snr_snapshot is not None:
                stats['avg_snr'] = float(avg_snr_snapshot)
            if avg_rssi_snapshot is not None:
                stats['avg_rssi'] = float(avg_rssi_snapshot)
            stats_source = 'snapshot'
        
        # Get topology info
        topology = {}
        if tls_server and hasattr(tls_server, 'sensor_topology'):
            topology = tls_server.sensor_topology.get(eui_upper, {})
        
        # Get registration info
        registration = {}
        if tls_server and hasattr(tls_server, 'registered_sensors'):
            registration = tls_server.registered_sensors.get(eui_upper, {})
        
        # Get preferred downlink path
        downlink_path = {}
        if tls_server and hasattr(tls_server, 'preferred_downlink_paths'):
            downlink_path = tls_server.preferred_downlink_paths.get(eui_upper, {})

        latest_uplink = None
        uplink_history = []
        if tls_server and hasattr(tls_server, 'get_sensor_latest_uplink'):
            try:
                latest_uplink = tls_server.get_sensor_latest_uplink(eui_upper)
            except Exception:
                latest_uplink = None
        if tls_server and hasattr(tls_server, 'get_sensor_uplink_history'):
            try:
                uplink_history = tls_server.get_sensor_uplink_history(eui_upper, limit=10) or []
            except Exception:
                uplink_history = []

        # Fallback for restarts/cold runtime: recover latest payload from Timescale telemetry.
        # This keeps sensor payload visible even when in-memory runtime history is empty.
        if not latest_uplink:
            try:
                sensor_tenant = _normalize_tenant_id(
                    sensor_config.get("tenant_id"),
                    fallback=active_tenant,
                )
                ts_history = _timescale_fetch_sensor_payload_history(
                    eui_upper,
                    tenant_id=sensor_tenant,
                    limit=10,
                ) or []
                if ts_history:
                    latest_uplink = ts_history[0]
                    uplink_history = ts_history
            except Exception:
                pass
        
        # Calculate derived metrics
        if stats.get('snr_count', 0) > 0:
            avg_snr = stats.get('snr_sum', 0) / stats.get('snr_count', 1)
        else:
            avg_snr = float(stats.get('avg_snr', 0) or 0)
        if stats.get('rssi_count', 0) > 0:
            avg_rssi = stats.get('rssi_sum', 0) / stats.get('rssi_count', 1)
        else:
            avg_rssi = float(stats.get('avg_rssi', 0) or 0)
        packets_received = stats.get('packets_received', 0)
        packets_lost = stats.get('packets_lost', 0)
        packet_loss_rate = (packets_lost / (packets_received + packets_lost) * 100) if (packets_received + packets_lost) > 0 else 0
        
        # Calculate observed send interval from recent runtime or telemetry history
        send_interval = 0
        interval_candidates = []
        if 'snr_history' in stats and len(stats['snr_history']) >= 2:
            history = stats['snr_history'][-10:]
            for i in range(1, len(history)):
                delta = float(history[i]['ts']) - float(history[i - 1]['ts'])
                if delta > 0:
                    interval_candidates.append(delta)
        if not interval_candidates and uplink_history and len(uplink_history) >= 2:
            sorted_history = []
            for row in uplink_history[:10]:
                ts_value = row.get("ts")
                if isinstance(ts_value, str):
                    try:
                        ts_value = datetime.fromisoformat(ts_value.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        ts_value = None
                try:
                    ts_value = float(ts_value) if ts_value is not None else None
                except (TypeError, ValueError):
                    ts_value = None
                if ts_value:
                    sorted_history.append(ts_value)
            sorted_history = sorted(sorted_history)
            for i in range(1, len(sorted_history)):
                delta = sorted_history[i] - sorted_history[i - 1]
                if delta > 0:
                    interval_candidates.append(delta)
        if interval_candidates:
            send_interval = sum(interval_candidates) / len(interval_candidates)

        interval_meta = _resolve_sensor_expected_interval(sensor_config, send_interval or None)
        
        # Calculate device health scores
        signal_score = 5.0
        if avg_snr < -5:
            signal_score = 1.0
        elif avg_snr < 0:
            signal_score = 2.0
        elif avg_snr < 5:
            signal_score = 3.0
        elif avg_snr < 10:
            signal_score = 4.0
        
        energy_score = 3.0  # Default fair
        if stats.get('spreading_factor', 7) <= 7:
            energy_score = 5.0
        elif stats.get('spreading_factor', 7) <= 9:
            energy_score = 4.0
        elif stats.get('spreading_factor', 7) <= 10:
            energy_score = 3.0
        elif stats.get('spreading_factor', 7) <= 11:
            energy_score = 2.0
        else:
            energy_score = 1.0
        
        # Get gateway count from topology
        gateway_count = len(topology.get('receiving_bases', {})) if topology else 0
        
        # Calculate duty cycle
        total_airtime_ms = stats.get('total_airtime_ms', 0)
        first_seen = stats.get('first_seen', 0)
        last_seen = stats.get('last_seen', 0)
        observation_time_s = (last_seen - first_seen) if first_seen and last_seen else 1
        duty_cycle = (total_airtime_ms / 1000 / observation_time_s * 100) if observation_time_s > 0 else 0
        
        response = {
            'success': True,
            'eui': eui_upper,
            'config': sensor_config,
            'sensor_profile': sensor_profile,
            'sensor_profile_label': _sensor_profile_label(sensor_profile),
            'first_seen': stats.get('first_seen'),
            'last_seen': stats.get('last_seen'),
            'packets_received': packets_received,
            'packets_lost': packets_lost,
            'packet_loss_rate': round(packet_loss_rate, 2),
            'packet_loss_source': stats_source,
            'avg_snr': round(avg_snr, 2),
            'avg_rssi': round(avg_rssi, 2),
            'min_snr': stats.get('min_snr', 0),
            'max_snr': stats.get('max_snr', 0),
            'min_rssi': stats.get('min_rssi', 0),
            'max_rssi': stats.get('max_rssi', 0),
            'current_rssi': (
                stats.get('rssi_sum', 0) / stats.get('rssi_count', 1)
                if stats.get('rssi_count', 0) > 0
                else avg_rssi
            ),
            'send_interval': round(send_interval, 1),
            'expected_interval_seconds': interval_meta['expected_interval_seconds'],
            'expected_interval_source': interval_meta['expected_interval_source'],
            'delay_threshold_seconds': interval_meta['delay_threshold_seconds'],
            'offline_threshold_seconds': interval_meta['offline_threshold_seconds'],
            'gateway_count': gateway_count,
            'primary_gateway': topology.get('primary_bs', ''),
            'receiving_gateways': list(topology.get('receiving_bases', {}).keys()) if topology else [],
            'receiving_bases_details': _convert_topology_timestamps(topology.get('receiving_bases', {}), last_seen) if topology else {},
            'signal_score': round(signal_score, 1),
            'energy_score': round(energy_score, 1),
            'spreading_factor': stats.get('spreading_factor', 7),
            'data_rate': stats.get('data_rate', 'SF7BW125'),
            'frequency_mhz': round(stats.get('frequency_mhz', 0), 3),
            'frame_counter': stats.get('frame_counter', 0),
            'last_airtime_ms': round(stats.get('last_airtime_ms', 0), 2),
            'avg_airtime_ms': round(total_airtime_ms / packets_received, 2) if packets_received > 0 else 0,
            'total_airtime_ms': round(total_airtime_ms, 2),
            'duty_cycle': round(duty_cycle, 4),
            'snr_history': stats.get('snr_history', [])[-50:],
            'rssi_history': stats.get('rssi_history', [])[-50:],
            'registration': registration,
            'downlink_path': downlink_path,
            'latest_uplink': latest_uplink,
            'uplink_history': uplink_history,
        }

        if _is_customer_role(session.get('role', 'viewer')):
            response = _sanitize_sensor_detail_for_customer(response)

        return jsonify(response)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/attach-all', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def attach_all_sensors():
    """Attach all configured sensors to base stations"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        active_tenant = _active_tenant_id()
        
        if not tls_server:
            _record_admin_audit(
                action='sensor.attach_all',
                entity='sensor',
                target_id='tenant',
                status='error',
                details={'tenant_id': active_tenant, 'error': 'tls_server_not_available'},
            )
            return jsonify({'success': False, 'message': 'TLS server not available'})
        
        if not hasattr(tls_server, 'connected_base_stations') or not tls_server.connected_base_stations:
            _record_admin_audit(
                action='sensor.attach_all',
                entity='sensor',
                target_id='tenant',
                status='error',
                details={'tenant_id': active_tenant, 'error': 'no_base_stations_connected'},
            )
            return jsonify({'success': False, 'message': 'No base stations connected'})
        
        # Get all configured sensors from the DB-first store
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
        
        if not sensors:
            _record_admin_audit(
                action='sensor.attach_all',
                entity='sensor',
                target_id='tenant',
                status='error',
                details={'tenant_id': active_tenant, 'error': 'no_sensors_configured'},
            )
            return jsonify({'success': False, 'message': 'No sensors configured to attach'})
        
        # Force reload sensor config to ensure all sensors are loaded
        tls_server.reload_sensor_config()
        
        # Send attach requests for all sensors to all connected base stations
        if hasattr(tls_server, 'attach_all_sensors_sync'):
            attached_count = tls_server.attach_all_sensors_sync()
            bs_count = len(tls_server.connected_base_stations)
            if attached_count > 0:
                connected_targets = [
                    _normalize_eui_upper(bs_eui)
                    for bs_eui in (tls_server.connected_base_stations or {}).values()
                    if _normalize_eui_upper(bs_eui)
                ]
                all_sensors = _load_all_sensors()
                changed = False
                changed_sensor_euis = []
                for sensor in all_sensors:
                    if not isinstance(sensor, dict):
                        continue
                    if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                        continue
                    sensor_eui = str(sensor.get("eui", "")).strip().upper()
                    if not sensor_eui:
                        continue
                    current_targets = _normalize_base_station_route_list(sensor.get("attached_base_stations", []))
                    if current_targets != connected_targets:
                        sensor["attached_base_stations"] = list(connected_targets)
                        changed = True
                        changed_sensor_euis.append(sensor_eui)
                if changed:
                    _save_all_sensors(all_sensors)
            message = f'Sent attach requests for {attached_count} sensors to {bs_count} base stations'
            _record_admin_audit(
                action='sensor.attach_all',
                entity='sensor',
                target_id='tenant',
                status='success',
                details={
                    'tenant_id': active_tenant,
                    'attached_count': attached_count,
                    'base_station_count': bs_count,
                    'changed_sensor_count': len(changed_sensor_euis) if attached_count > 0 else 0,
                    'sample_sensor_euis': changed_sensor_euis[:20] if attached_count > 0 else [],
                    'attached_base_stations': connected_targets if attached_count > 0 else [],
                },
            )
            return jsonify({'success': True, 'message': message})
        else:
            _record_admin_audit(
                action='sensor.attach_all',
                entity='sensor',
                target_id='tenant',
                status='error',
                details={'tenant_id': active_tenant, 'error': 'tls_attach_all_unavailable'},
            )
            return jsonify({'success': False, 'message': 'Attach all function not available in TLS server'})
    except Exception as e:
        _record_admin_audit(
            action='sensor.attach_all',
            entity='sensor',
            target_id='tenant',
            status='error',
            details={'tenant_id': _active_tenant_id(), 'error': str(e)},
        )
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/detach-all', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def detach_all_sensors():
    """Detach all sensors from base stations (desired-state first, runtime best-effort)."""
    try:
        global tls_server_instance
        tls_server = tls_server_instance

        # Always clear desired mappings for current tenant.
        active_tenant = _active_tenant_id()
        sensors = _load_all_sensors()
        changed = False
        cleared_sensor_euis = []
        for sensor in sensors:
            if not isinstance(sensor, dict):
                continue
            if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                continue
            if "attached_base_stations" in sensor:
                sensor.pop("attached_base_stations", None)
                changed = True
                sensor_eui = str(sensor.get("eui", "")).strip().upper()
                if sensor_eui:
                    cleared_sensor_euis.append(sensor_eui)
        if changed:
            _save_all_sensors(sensors)

        detached_count = 0
        runtime_error = None
        if tls_server and hasattr(tls_server, 'detach_all_sensors_sync'):
            try:
                detached_count = int(tls_server.detach_all_sensors_sync() or 0)
            except Exception as exc:
                runtime_error = str(exc)

        if tls_server and hasattr(tls_server, 'reload_sensor_config'):
            try:
                tls_server.reload_sensor_config()
            except Exception:
                pass

        message = f'Prepojenie so základňovými stanicami bolo vymazané pre všetky senzory v priestore "{active_tenant}".'
        if detached_count > 0:
            message = f'{message} Zmena sa už prejavila pri {detached_count} senzoroch.'
        elif tls_server and hasattr(tls_server, 'detach_all_sensors_sync'):
            message = f'{message} Dokončenie odpojenia prebehne po obnovení spojenia so stanicami.'
        if runtime_error:
            message = f'{message} Poznámka: {runtime_error}'

        _record_admin_audit(
            action='sensor.detach_all',
            entity='sensor',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'detached_count': detached_count,
                'mapping_cleared': True,
                'changed_sensor_count': len(cleared_sensor_euis),
                'sample_sensor_euis': cleared_sensor_euis[:20],
                'runtime_error': runtime_error,
            },
        )

        return jsonify({
            'success': True,
            'message': message,
            'detached_count': detached_count,
            'mapping_cleared': True,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/clear', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def clear_all_sensors():
    """Clear all sensor configurations and detach all sensors"""
    try:
        detached_count = 0
        active_tenant = _active_tenant_id()

        # First detach all sensors from base stations
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server and hasattr(tls_server, 'detach_all_sensors_sync'):
            detached_count = tls_server.detach_all_sensors_sync()

        # Get count before clear for telemetry record
        existing_sensors = _load_all_sensors()
        sensors_to_remove = _filter_sensors_for_tenant(existing_sensors, tenant_id=active_tenant)
        kept_sensors = [
            s for s in existing_sensors
            if not _tenant_matches(_tenant_id_from_sensor(s), active_tenant)
        ]
        _save_all_sensors(kept_sensors)

        # Remove sensor placements from coverage positions (keep base stations)
        state = _load_coverage_positions_state()
        positions = state.get("positions", {})
        if isinstance(positions, dict):
            target_euis = {
                str((sensor or {}).get("eui", "")).strip().upper()
                for sensor in sensors_to_remove
            }
            keys_to_remove = [
                key for key in positions.keys()
                if str(key).startswith("sensor_") and str(key).split("_", 1)[-1].strip().upper() in target_euis
            ]
            for key in keys_to_remove:
                positions.pop(key, None)
            if keys_to_remove:
                _save_coverage_positions_state(state)

        # Also clear from TLS server if available
        if tls_server and hasattr(tls_server, 'clear_all_sensors'):
            tls_server.clear_all_sensors()

        _try_record_inventory_event(
            "sensor",
            "cleared",
            "all",
            {
                "count": len(sensors_to_remove),
                "detached_count": detached_count,
                "tenant_id": active_tenant,
            }
        )

        message = f'Tenant sensors cleared successfully ({len(sensors_to_remove)} removed). Detached {detached_count} sensors from base stations.'
        _record_admin_audit(
            action='sensor.clear_all',
            entity='sensor',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'removed_count': len(sensors_to_remove),
                'detached_count': detached_count,
                'sample_sensor_euis': [
                    _normalize_eui_upper((sensor or {}).get("eui", ""))
                    for sensor in sensors_to_remove[:20]
                    if _normalize_eui_upper((sensor or {}).get("eui", ""))
                ],
            },
        )
        return jsonify({'success': True, 'message': message})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sensors/reload', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def reload_sensors():
    """Force reload sensor configuration in TLS server"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance
        if tls_server:
            tls_server.reload_sensor_config()
            _record_admin_audit(
                action='sensor.reload_config',
                entity='sensor',
                target_id='tenant',
                status='success',
                details={'tenant_id': _active_tenant_id()},
            )
            return jsonify({'success': True, 'message': 'Sensor configuration reloaded successfully'})
        else:
            return jsonify({'success': False, 'message': 'TLS server not available'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/sensors/storage-meta', methods=['GET'])
@login_required
@permission_required('can_edit_sensors')
def get_sensors_storage_meta():
    sensors = _load_all_sensors()
    storage_meta = _sensors_storage_backend_meta()
    return jsonify({
        'success': True,
        'storage_backend': storage_meta.get('backend', 'json'),
        'storage_db_enabled': bool(storage_meta.get('db_enabled', False)),
        'bootstrap_state': storage_meta.get('bootstrap_state', {}),
        'seed_defaults_file': storage_meta.get('seed_defaults_file', SENSORS_RECOVERY_FILE),
        'recovery_file': storage_meta.get('recovery_file', SENSORS_RECOVERY_FILE),
        'sensor_count': len(sensors or []),
    })


@app.route('/api/sensors/export', methods=['GET'])
@login_required
@permission_required('can_edit_sensors')
def export_sensors():
    """Export all sensors as CSV file"""
    try:
        # Load sensors from the DB-first inventory store
        active_tenant = _active_tenant_id()
        sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
        
        if not sensors:
            return jsonify({'success': False, 'message': 'No sensors to export'}), 404
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        storage_meta = _sensors_storage_backend_meta()
        
        # Write header
        writer.writerow(['eui', 'nwKey', 'shortAddr', 'bidi', 'name', 'tags', 'sensor_profile', 'payload_decoder', 'gps_lat', 'gps_lng', 'tenant_id'])
        
        # Write sensor data
        for sensor in sensors:
            writer.writerow([
                sensor.get('eui', ''),
                sensor.get('nwKey', ''),
                sensor.get('shortAddr', ''),
                'true' if sensor.get('bidi', False) else 'false',
                sensor.get('name', ''),
                '|'.join(_normalize_sensor_tags(sensor.get('tags', []))),
                _infer_sensor_profile(sensor),
                sensor.get('payload_decoder', 'auto'),
                sensor.get('gps_lat', ''),
                sensor.get('gps_lng', ''),
                sensor.get('tenant_id', active_tenant),
            ])
        
        # Create response with CSV file
        output.seek(0)
        _record_admin_audit(
            action='sensor.export',
            entity='sensor',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'count': len(sensors),
                'storage_backend': storage_meta.get('backend', 'json'),
                'recovery_file': storage_meta.get('recovery_file', SENSORS_RECOVERY_FILE),
            },
        )
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename=sensors_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                'X-Storage-Backend': str(storage_meta.get('backend', 'json')),
            }
        )
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/sensors/import', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def import_sensors():
    """Import sensors from CSV/TXT file"""
    try:
        active_tenant = _active_tenant_id()
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'}), 400
        
        # Read file content
        content = file.read().decode('utf-8')
        lines = content.strip().split('\n')
        
        if len(lines) < 1:
            return jsonify({'success': False, 'message': 'File is empty'}), 400
        
        # Detect delimiter (comma, semicolon, or tab)
        first_line = lines[0]
        if ';' in first_line:
            delimiter = ';'
        elif '\t' in first_line:
            delimiter = '\t'
        else:
            delimiter = ','
        
        # Parse CSV
        reader = csv.reader(io.StringIO(content), delimiter=delimiter)
        rows = list(reader)
        
        if len(rows) < 1:
            return jsonify({'success': False, 'message': 'No data in file'}), 400
        
        # Check if first row is header
        header = rows[0]
        has_header = any(h.lower() in ['eui', 'nwkey', 'shortaddr', 'bidi', 'network_key', 'short_addr', 'name', 'tags', 'label', 'sensor_profile', 'profile', 'payload_decoder', 'decoder', 'gps_lat', 'gps_lng', 'latitude', 'longitude', 'lat', 'lng', 'tenant', 'tenant_id'] for h in header)
        
        if has_header:
            # Map header columns
            header_lower = [h.lower().strip() for h in header]
            eui_idx = next((i for i, h in enumerate(header_lower) if h in ['eui', 'mac', 'address']), 0)
            nwkey_idx = next((i for i, h in enumerate(header_lower) if h in ['nwkey', 'network_key', 'key', 'networkkey']), 1)
            shortaddr_idx = next((i for i, h in enumerate(header_lower) if h in ['shortaddr', 'short_addr', 'shortaddress', 'addr']), 2)
            bidi_idx = next((i for i, h in enumerate(header_lower) if h in ['bidi', 'bidirectional', 'bidir']), 3)
            name_idx = next((i for i, h in enumerate(header_lower) if h in ['name', 'label', 'title']), None)
            tags_idx = next((i for i, h in enumerate(header_lower) if h in ['tags', 'tag', 'labels']), None)
            profile_idx = next((i for i, h in enumerate(header_lower) if h in ['sensor_profile', 'profile']), None)
            decoder_idx = next((i for i, h in enumerate(header_lower) if h in ['payload_decoder', 'decoder', 'payload_profile']), None)
            lat_idx = next((i for i, h in enumerate(header_lower) if h in ['gps_lat', 'latitude', 'lat']), None)
            lng_idx = next((i for i, h in enumerate(header_lower) if h in ['gps_lng', 'longitude', 'lng', 'lon']), None)
            tenant_idx = next((i for i, h in enumerate(header_lower) if h in ['tenant', 'tenant_id']), None)
            data_rows = rows[1:]
        else:
            # Assume order: eui, nwKey, shortAddr, bidi, name, tags, gps_lat, gps_lng
            eui_idx, nwkey_idx, shortaddr_idx, bidi_idx = 0, 1, 2, 3
            name_idx, tags_idx = 4, 5
            profile_idx = None
            decoder_idx = None
            lat_idx, lng_idx = 6, 7
            tenant_idx = None
            data_rows = rows
        
        # Load existing sensors
        existing_sensors = _load_all_sensors()
        storage_meta = _sensors_storage_backend_meta()
        existing_by_eui = {
            str(s.get('eui', '')).strip().upper(): s
            for s in existing_sensors
            if isinstance(s, dict) and str(s.get('eui', '')).strip()
        }
        
        imported_count = 0
        updated_count = 0
        errors = []
        
        for row_idx, row in enumerate(data_rows):
            try:
                if len(row) < 3:  # At least eui, nwKey, shortAddr required
                    errors.append(f"Row {row_idx + 1}: Not enough columns")
                    continue
                
                eui = row[eui_idx].strip().upper() if eui_idx < len(row) else ''
                nwkey = row[nwkey_idx].strip() if nwkey_idx < len(row) else ''
                shortaddr = row[shortaddr_idx].strip() if shortaddr_idx < len(row) else '0000'
                bidi_val = row[bidi_idx].strip().lower() if bidi_idx < len(row) else 'false'
                bidi = bidi_val in ['true', '1', 'yes', 'on']
                row_decoder_idx = decoder_idx
                row_lat_idx = lat_idx
                row_lng_idx = lng_idx
                if not has_header and len(row) >= 9:
                    # Backward compatible: no-header files may include payload_decoder before GPS columns.
                    row_decoder_idx = 6
                    row_lat_idx = 7
                    row_lng_idx = 8

                name = row[name_idx].strip() if (name_idx is not None and name_idx < len(row)) else ''
                tags_raw = row[tags_idx].strip() if (tags_idx is not None and tags_idx < len(row)) else ''
                tags = _normalize_sensor_tags(tags_raw)
                sensor_profile = row[profile_idx].strip() if (profile_idx is not None and profile_idx < len(row)) else 'auto'
                payload_decoder = row[row_decoder_idx].strip() if (row_decoder_idx is not None and row_decoder_idx < len(row)) else 'auto'
                lat_raw = row[row_lat_idx].strip() if (row_lat_idx is not None and row_lat_idx < len(row)) else ''
                lng_raw = row[row_lng_idx].strip() if (row_lng_idx is not None and row_lng_idx < len(row)) else ''
                tenant_raw = row[tenant_idx].strip() if (tenant_idx is not None and tenant_idx < len(row)) else active_tenant
                if str(session.get("role", "")).strip().lower() != "admin":
                    tenant_raw = active_tenant
                tenant_id = _normalize_tenant_id(tenant_raw, fallback=active_tenant)
                gps_lat, gps_lng = _normalize_gps_coordinates(lat_raw, lng_raw)
                
                # Validate EUI
                if not eui or len(eui) < 8:
                    errors.append(f"Row {row_idx + 1}: Invalid EUI '{eui}'")
                    continue
                
                # Validate nwKey
                if not nwkey or len(nwkey) < 16:
                    errors.append(f"Row {row_idx + 1}: Invalid network key")
                    continue
                
                sensor_data = {
                    'eui': eui,
                    'nwKey': nwkey,
                    'shortAddr': shortaddr if shortaddr else '0000',
                    'bidi': bidi,
                    'name': name,
                    'tags': tags,
                    'sensor_profile': sensor_profile,
                    'payload_decoder': payload_decoder,
                    'gps_lat': gps_lat,
                    'gps_lng': gps_lng,
                    'tenant_id': tenant_id,
                }
                sensor_data = _normalize_sensor_payload(sensor_data)
                
                existing_sensor = existing_by_eui.get(eui)
                if existing_sensor is not None:
                    existing_tenant = _tenant_id_from_sensor(existing_sensor)
                    if not _tenant_matches(existing_tenant, tenant_id):
                        errors.append(
                            f"Row {row_idx + 1}: EUI '{eui}' already belongs to tenant '{existing_tenant}'"
                        )
                        continue
                    # Update existing sensor
                    for s in existing_sensors:
                        if (
                            str(s.get('eui', '')).upper() == eui
                            and _tenant_matches(_tenant_id_from_sensor(s), tenant_id)
                        ):
                            s.update(sensor_data)
                            break
                    updated_count += 1
                else:
                    # Add new sensor
                    existing_sensors.append(sensor_data)
                    existing_by_eui[eui] = sensor_data
                    imported_count += 1
                    
            except Exception as e:
                errors.append(f"Row {row_idx + 1}: {str(e)}")
        
        # Save to file
        _save_all_sensors(existing_sensors)

        # Keep coverage map positions in sync with imported sensor coordinates
        for sensor in existing_sensors:
            eui_value = str(sensor.get("eui", "")).strip().upper()
            if not eui_value:
                continue
            try:
                gps_lat, gps_lng = _normalize_gps_coordinates(sensor.get("gps_lat"), sensor.get("gps_lng"))
            except ValueError:
                gps_lat, gps_lng = None, None
            _upsert_device_gps_position("sensor", eui_value, gps_lat, gps_lng)

        _try_record_inventory_event(
            "sensor",
            "imported",
            "batch",
            {
                "imported_count": imported_count,
                "updated_count": updated_count,
                "error_count": len(errors),
                "total_after": len(existing_sensors),
                "named_count": sum(1 for s in existing_sensors if str(s.get("name", "")).strip()),
                "tagged_count": sum(1 for s in existing_sensors if len(_normalize_sensor_tags(s.get("tags", []))) > 0),
                "filename": file.filename or "unknown",
                "tenant_id": active_tenant,
            }
        )
        
        # Reload TLS server config
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'reload_sensor_config'):
            tls_server_instance.reload_sensor_config()
        
        message = f'Import complete: {imported_count} new sensors, {updated_count} updated'
        if errors:
            message += f', {len(errors)} errors'

        _record_admin_audit(
            action='sensor.import',
            entity='sensor',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'imported_count': imported_count,
                'updated_count': updated_count,
                'error_count': len(errors),
                'filename': file.filename or 'unknown',
                'storage_backend': storage_meta.get('backend', 'json'),
                'recovery_file': storage_meta.get('recovery_file', SENSORS_RECOVERY_FILE),
            },
        )
        
        return jsonify({
            'success': True,
            'message': message,
            'imported': imported_count,
            'updated': updated_count,
            'errors': errors[:10] if errors else [],  # Limit error messages
            'storage_backend': storage_meta.get('backend', 'json'),
            'seed_defaults_file': storage_meta.get('seed_defaults_file', SENSORS_RECOVERY_FILE),
            'recovery_file': storage_meta.get('recovery_file', SENSORS_RECOVERY_FILE),
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/config')
@login_required
@permission_required('can_edit_config')
def config():
    try:
        # Force reload the config module to get latest values
        import importlib
        import sys
        if 'bssci_config' in sys.modules:
            importlib.reload(sys.modules['bssci_config'])
        
        import bssci_config
        
        config_data = {
            'LISTEN_HOST': getattr(bssci_config, 'LISTEN_HOST', '0.0.0.0'),
            'LISTEN_PORT': getattr(bssci_config, 'LISTEN_PORT', 16018),
            'MQTT_ENABLED': getattr(bssci_config, 'MQTT_ENABLED', True),
            'MQTT_BROKER': getattr(bssci_config, 'MQTT_BROKER', 'localhost'),
            'MQTT_PORT': getattr(bssci_config, 'MQTT_PORT', 1883),
            'MQTT_USERNAME': getattr(bssci_config, 'MQTT_USERNAME', ''),
            'MQTT_PASSWORD': getattr(bssci_config, 'MQTT_PASSWORD', ''),
            'BASE_TOPIC': getattr(bssci_config, 'BASE_TOPIC', 'bssci/'),
            'STATUS_INTERVAL': getattr(bssci_config, 'STATUS_INTERVAL', 30),
            'DEDUPLICATION_DELAY': getattr(bssci_config, 'DEDUPLICATION_DELAY', 2.0),
            'AUTO_DETACH_ENABLED': getattr(bssci_config, 'AUTO_DETACH_ENABLED', True),
            'AUTO_DETACH_TIMEOUT': getattr(bssci_config, 'AUTO_DETACH_TIMEOUT', 259200),
            'AUTO_DETACH_WARNING_TIMEOUT': getattr(bssci_config, 'AUTO_DETACH_WARNING_TIMEOUT', 129600),
            'AUTO_DETACH_CHECK_INTERVAL': getattr(bssci_config, 'AUTO_DETACH_CHECK_INTERVAL', 3600),
            'TIMEZONE': getattr(bssci_config, 'TIMEZONE', 'Europe/Berlin'),
            'APP_LANGUAGE': _normalize_app_language(getattr(bssci_config, 'APP_LANGUAGE', 'sk')),
            'MQTT_UI_ENABLED': getattr(bssci_config, 'MQTT_UI_ENABLED', True),
            'TELEMETRY_SOURCE': getattr(bssci_config, 'TELEMETRY_SOURCE', 'auto'),
            'INFLUX_ENABLED': getattr(bssci_config, 'INFLUX_ENABLED', False),
            'INFLUXDB_URL': getattr(bssci_config, 'INFLUXDB_URL', ''),
            'INFLUXDB_ORG': getattr(bssci_config, 'INFLUXDB_ORG', ''),
            'INFLUXDB_BUCKET': getattr(bssci_config, 'INFLUXDB_BUCKET', ''),
            'INFLUXDB_TOKEN': getattr(bssci_config, 'INFLUXDB_TOKEN', ''),
            'INFLUXDB_VERIFY_SSL': getattr(bssci_config, 'INFLUXDB_VERIFY_SSL', True),
            'INFLUX_UPTIME_MEASUREMENT': getattr(bssci_config, 'INFLUX_UPTIME_MEASUREMENT', 'bssci_bs_uptime'),
            'INFLUX_UPTIME_FIELD': getattr(bssci_config, 'INFLUX_UPTIME_FIELD', 'status'),
            'INFLUX_UPTIME_EUI_TAG': getattr(bssci_config, 'INFLUX_UPTIME_EUI_TAG', 'eui'),
            'INFLUX_UPTIME_QUERY': getattr(bssci_config, 'INFLUX_UPTIME_QUERY', ''),
            'INFLUX_INVENTORY_WRITE_ENABLED': getattr(bssci_config, 'INFLUX_INVENTORY_WRITE_ENABLED', True),
            'INFLUX_INVENTORY_MEASUREMENT': getattr(bssci_config, 'INFLUX_INVENTORY_MEASUREMENT', 'bssci_inventory_events'),
            'INFLUX_SNAPSHOT_ENABLED': getattr(bssci_config, 'INFLUX_SNAPSHOT_ENABLED', True),
            'INFLUX_SNAPSHOT_INTERVAL_SECONDS': getattr(bssci_config, 'INFLUX_SNAPSHOT_INTERVAL_SECONDS', 60),
            'INFLUX_SNAPSHOT_MEASUREMENT': getattr(bssci_config, 'INFLUX_SNAPSHOT_MEASUREMENT', 'bssci_inventory_snapshot'),
            'TIMESCALE_ENABLED': getattr(bssci_config, 'TIMESCALE_ENABLED', False),
            'TIMESCALE_HOST': getattr(bssci_config, 'TIMESCALE_HOST', 'timescaledb'),
            'TIMESCALE_PORT': getattr(bssci_config, 'TIMESCALE_PORT', 5432),
            'TIMESCALE_DB': getattr(bssci_config, 'TIMESCALE_DB', 'bssci'),
            'TIMESCALE_USER': getattr(bssci_config, 'TIMESCALE_USER', 'bssci_user'),
            'TIMESCALE_PASSWORD': getattr(bssci_config, 'TIMESCALE_PASSWORD', ''),
            'TIMESCALE_SSLMODE': getattr(bssci_config, 'TIMESCALE_SSLMODE', 'disable'),
            'TIMESCALE_DEFAULT_TENANT': getattr(bssci_config, 'TIMESCALE_DEFAULT_TENANT', 'default'),
            'TIMESCALE_INVENTORY_WRITE_ENABLED': getattr(bssci_config, 'TIMESCALE_INVENTORY_WRITE_ENABLED', True),
            'TIMESCALE_TELEMETRY_WRITE_ENABLED': getattr(bssci_config, 'TIMESCALE_TELEMETRY_WRITE_ENABLED', True),
            'TIMESCALE_SNAPSHOT_ENABLED': getattr(bssci_config, 'TIMESCALE_SNAPSHOT_ENABLED', True),
            'TIMESCALE_SNAPSHOT_INTERVAL_SECONDS': getattr(bssci_config, 'TIMESCALE_SNAPSHOT_INTERVAL_SECONDS', 60),
            'TIMESCALE_RETENTION_ENABLED': getattr(bssci_config, 'TIMESCALE_RETENTION_ENABLED', True),
            'TIMESCALE_TELEMETRY_RETENTION_DAYS': getattr(bssci_config, 'TIMESCALE_TELEMETRY_RETENTION_DAYS', 90),
            'TIMESCALE_INVENTORY_RETENTION_DAYS': getattr(bssci_config, 'TIMESCALE_INVENTORY_RETENTION_DAYS', 365),
            'TIMESCALE_COMPRESSION_ENABLED': getattr(bssci_config, 'TIMESCALE_COMPRESSION_ENABLED', True),
            'TIMESCALE_COMPRESSION_AFTER_DAYS': getattr(bssci_config, 'TIMESCALE_COMPRESSION_AFTER_DAYS', 7),
            'GRAFANA_URL': getattr(bssci_config, 'GRAFANA_URL', 'http://localhost:3000'),
            'GRAFANA_INTERNAL_URL': getattr(bssci_config, 'GRAFANA_INTERNAL_URL', ''),
            'GRAFANA_DASHBOARD_UID': getattr(bssci_config, 'GRAFANA_DASHBOARD_UID', 'service-center-overview'),
            'GRAFANA_DASHBOARD_SLUG': getattr(bssci_config, 'GRAFANA_DASHBOARD_SLUG', 'service-center-overview'),
            'GRAFANA_ORG_ID': getattr(bssci_config, 'GRAFANA_ORG_ID', 1),
            'GRAFANA_EMBED_ENABLED': getattr(bssci_config, 'GRAFANA_EMBED_ENABLED', True),
            'GRAFANA_ANONYMOUS_ENABLED': getattr(bssci_config, 'GRAFANA_ANONYMOUS_ENABLED', True),
            'GRAFANA_ANONYMOUS_ORG_ROLE': getattr(bssci_config, 'GRAFANA_ANONYMOUS_ORG_ROLE', 'Viewer'),
            'GRAFANA_PROXY_ENABLED': getattr(bssci_config, 'GRAFANA_PROXY_ENABLED', True),
            'GRAFANA_PROXY_TIMEOUT_SECONDS': getattr(bssci_config, 'GRAFANA_PROXY_TIMEOUT_SECONDS', 20),
            'GRAFANA_HEALTH_PANEL_MAP': getattr(
                bssci_config,
                'GRAFANA_HEALTH_PANEL_MAP',
                'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6'
            ),
            'MONITOR_QUEUE_WARN_PCT': getattr(bssci_config, 'MONITOR_QUEUE_WARN_PCT', 65.0),
            'MONITOR_QUEUE_CRIT_PCT': getattr(bssci_config, 'MONITOR_QUEUE_CRIT_PCT', 85.0),
            'MONITOR_RETRY_FAIL_WARN_PCT': getattr(bssci_config, 'MONITOR_RETRY_FAIL_WARN_PCT', 5.0),
            'MONITOR_RETRY_FAIL_CRIT_PCT': getattr(bssci_config, 'MONITOR_RETRY_FAIL_CRIT_PCT', 20.0),
            'MONITOR_DB_WRITE_LATENCY_WARN_MS': getattr(bssci_config, 'MONITOR_DB_WRITE_LATENCY_WARN_MS', 500.0),
            'MONITOR_DB_WRITE_LATENCY_CRIT_MS': getattr(bssci_config, 'MONITOR_DB_WRITE_LATENCY_CRIT_MS', 1500.0),
            'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR': getattr(bssci_config, 'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR', 3.0),
            'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR': getattr(bssci_config, 'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR', 8.0),
            'OMS_ENABLED': getattr(bssci_config, 'OMS_ENABLED', True),
            'MQTT_UI_ENABLED': getattr(bssci_config, 'MQTT_UI_ENABLED', True),
            'BS_UPTIME_PANEL_ENABLED': getattr(bssci_config, 'BS_UPTIME_PANEL_ENABLED', False),
        }
        return render_template('config.html', config=config_data)
    except Exception as e:
        print(f"Error loading config page: {e}")
        # Return default config if there's an error
        default_config = {
            'LISTEN_HOST': '0.0.0.0',
            'LISTEN_PORT': 16018,
            'MQTT_ENABLED': True,
            'MQTT_BROKER': 'localhost',
            'MQTT_PORT': 1883,
            'MQTT_USERNAME': '',
            'MQTT_PASSWORD': '',
            'BASE_TOPIC': 'bssci/',
            'STATUS_INTERVAL': 30,
            'DEDUPLICATION_DELAY': 2.0,
            'AUTO_DETACH_ENABLED': True,
            'AUTO_DETACH_TIMEOUT': 259200,
            'AUTO_DETACH_WARNING_TIMEOUT': 129600,
            'AUTO_DETACH_CHECK_INTERVAL': 3600,
            'TIMEZONE': 'Europe/Berlin',
            'APP_LANGUAGE': 'sk',
            'MQTT_UI_ENABLED': True,
            'TELEMETRY_SOURCE': 'auto',
            'INFLUX_ENABLED': False,
            'INFLUXDB_URL': '',
            'INFLUXDB_ORG': '',
            'INFLUXDB_BUCKET': '',
            'INFLUXDB_TOKEN': '',
            'INFLUXDB_VERIFY_SSL': True,
            'INFLUX_UPTIME_MEASUREMENT': 'bssci_bs_uptime',
            'INFLUX_UPTIME_FIELD': 'status',
            'INFLUX_UPTIME_EUI_TAG': 'eui',
            'INFLUX_UPTIME_QUERY': '',
            'INFLUX_INVENTORY_WRITE_ENABLED': True,
            'INFLUX_INVENTORY_MEASUREMENT': 'bssci_inventory_events',
            'INFLUX_SNAPSHOT_ENABLED': True,
            'INFLUX_SNAPSHOT_INTERVAL_SECONDS': 60,
            'INFLUX_SNAPSHOT_MEASUREMENT': 'bssci_inventory_snapshot',
            'TIMESCALE_ENABLED': False,
            'TIMESCALE_HOST': 'timescaledb',
            'TIMESCALE_PORT': 5432,
            'TIMESCALE_DB': 'bssci',
            'TIMESCALE_USER': 'bssci_user',
            'TIMESCALE_PASSWORD': '',
            'TIMESCALE_SSLMODE': 'disable',
            'TIMESCALE_DEFAULT_TENANT': 'default',
            'TIMESCALE_INVENTORY_WRITE_ENABLED': True,
            'TIMESCALE_TELEMETRY_WRITE_ENABLED': True,
            'TIMESCALE_SNAPSHOT_ENABLED': True,
            'TIMESCALE_SNAPSHOT_INTERVAL_SECONDS': 60,
            'TIMESCALE_RETENTION_ENABLED': True,
            'TIMESCALE_TELEMETRY_RETENTION_DAYS': 90,
            'TIMESCALE_INVENTORY_RETENTION_DAYS': 365,
            'TIMESCALE_COMPRESSION_ENABLED': True,
            'TIMESCALE_COMPRESSION_AFTER_DAYS': 7,
            'GRAFANA_URL': 'http://localhost:3000',
            'GRAFANA_INTERNAL_URL': '',
            'GRAFANA_DASHBOARD_UID': 'service-center-overview',
            'GRAFANA_DASHBOARD_SLUG': 'service-center-overview',
            'GRAFANA_ORG_ID': 1,
            'GRAFANA_EMBED_ENABLED': True,
            'GRAFANA_ANONYMOUS_ENABLED': True,
            'GRAFANA_ANONYMOUS_ORG_ROLE': 'Viewer',
            'GRAFANA_PROXY_ENABLED': True,
            'GRAFANA_PROXY_TIMEOUT_SECONDS': 20,
            'GRAFANA_HEALTH_PANEL_MAP': 'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6',
            'MONITOR_QUEUE_WARN_PCT': 65.0,
            'MONITOR_QUEUE_CRIT_PCT': 85.0,
            'MONITOR_RETRY_FAIL_WARN_PCT': 5.0,
            'MONITOR_RETRY_FAIL_CRIT_PCT': 20.0,
            'MONITOR_DB_WRITE_LATENCY_WARN_MS': 500.0,
            'MONITOR_DB_WRITE_LATENCY_CRIT_MS': 1500.0,
            'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR': 3.0,
            'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR': 8.0,
            'OMS_ENABLED': True,
            'MQTT_UI_ENABLED': True,
            'BS_UPTIME_PANEL_ENABLED': False,
        }
        return render_template('config.html', config=default_config)

@app.route('/administration')
@login_required
@admin_scope_required('manage_users', 'manage_tenants', any_scope=True)
def administration():
    return render_template('administration.html')

@app.route('/api/config', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def update_config():
    try:
        data = request.json
        
        # Type safety: Validate request data
        if data is None:
            return jsonify({'success': False, 'message': 'No JSON data provided'}), 400
        
        def _to_bool(value, default=False):
            if isinstance(value, bool):
                return value
            if value is None:
                return default
            return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}

        # Values are already in seconds from HTML form (no conversion needed)
        auto_detach_timeout = int(data.get('AUTO_DETACH_TIMEOUT', 259200))
        auto_detach_warning_timeout = int(data.get('AUTO_DETACH_WARNING_TIMEOUT', 129600))
        auto_detach_check_interval = int(data.get('AUTO_DETACH_CHECK_INTERVAL', 3600))
        influx_snapshot_interval = max(15, int(data.get('INFLUX_SNAPSHOT_INTERVAL_SECONDS', 60)))
        timescale_snapshot_interval = max(15, int(data.get('TIMESCALE_SNAPSHOT_INTERVAL_SECONDS', 60)))
        timescale_telemetry_retention_days = max(1, int(data.get('TIMESCALE_TELEMETRY_RETENTION_DAYS', 90)))
        timescale_inventory_retention_days = max(1, int(data.get('TIMESCALE_INVENTORY_RETENTION_DAYS', 365)))
        timescale_compression_after_days = max(1, int(data.get('TIMESCALE_COMPRESSION_AFTER_DAYS', 7)))
        influx_uptime_query = str(data.get('INFLUX_UPTIME_QUERY', '')).replace('\r', ' ').replace('\n', ' ').strip()
        telemetry_source = str(data.get('TELEMETRY_SOURCE', 'auto')).strip().lower()
        if telemetry_source not in {'auto', 'runtime', 'influx'}:
            telemetry_source = 'auto'
        influx_enabled = _to_bool(data.get('INFLUX_ENABLED', False), False)
        if not influx_enabled and telemetry_source in {'auto', 'influx'}:
            telemetry_source = 'runtime'
        app_language = _normalize_app_language(data.get('APP_LANGUAGE', 'sk'))
        grafana_org_id = max(1, int(data.get('GRAFANA_ORG_ID', 1)))
        timescale_port = int(data.get('TIMESCALE_PORT', 5432))
        timescale_sslmode = str(data.get('TIMESCALE_SSLMODE', 'disable')).strip().lower()
        if timescale_sslmode not in {'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'}:
            timescale_sslmode = 'disable'

        # Preserve selected sensitive/legacy values if present
        existing_env = {}
        try:
            if os.path.exists('.env'):
                with open('.env', 'r') as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        if not line or line.startswith('#') or '=' not in line:
                            continue
                        key, value = line.split('=', 1)
                        existing_env[key.strip()] = value.strip()
        except Exception:
            existing_env = {}

        secret_key = existing_env.get('SECRET_KEY', os.getenv('SECRET_KEY', 'your-secret-key-here'))
        tls_client_cert_mode = existing_env.get('TLS_CLIENT_CERT_MODE', os.getenv('TLS_CLIENT_CERT_MODE', 'required'))
        cert_file = existing_env.get('CERT_FILE', 'certs/service_center_cert.pem')
        key_file = existing_env.get('KEY_FILE', 'certs/service_center_key.pem')
        ca_file = existing_env.get('CA_FILE', 'certs/ca_cert.pem')
        grafana_url = str(data.get('GRAFANA_URL', existing_env.get('GRAFANA_URL', 'http://localhost:3000'))).strip() or 'http://localhost:3000'
        grafana_internal_url = str(
            data.get('GRAFANA_INTERNAL_URL', existing_env.get('GRAFANA_INTERNAL_URL', 'http://grafana:3000'))
        ).strip() or 'http://grafana:3000'
        grafana_dashboard_uid = str(
            data.get('GRAFANA_DASHBOARD_UID', existing_env.get('GRAFANA_DASHBOARD_UID', 'service-center-overview'))
        ).strip() or 'service-center-overview'
        grafana_dashboard_slug = str(
            data.get('GRAFANA_DASHBOARD_SLUG', existing_env.get('GRAFANA_DASHBOARD_SLUG', 'service-center-overview'))
        ).strip() or 'service-center-overview'
        grafana_embed_enabled = _to_bool(
            data.get('GRAFANA_EMBED_ENABLED', existing_env.get('GRAFANA_EMBED_ENABLED', 'true')),
            True,
        )
        grafana_anonymous_enabled = _to_bool(
            data.get('GRAFANA_ANONYMOUS_ENABLED', existing_env.get('GRAFANA_ANONYMOUS_ENABLED', 'true')),
            True,
        )
        grafana_anonymous_role = str(
            data.get('GRAFANA_ANONYMOUS_ORG_ROLE', existing_env.get('GRAFANA_ANONYMOUS_ORG_ROLE', 'Viewer'))
        ).strip() or 'Viewer'
        grafana_proxy_enabled = _to_bool(
            data.get('GRAFANA_PROXY_ENABLED', existing_env.get('GRAFANA_PROXY_ENABLED', 'true')),
            True,
        )
        grafana_proxy_timeout = max(
            2,
            int(data.get('GRAFANA_PROXY_TIMEOUT_SECONDS', existing_env.get('GRAFANA_PROXY_TIMEOUT_SECONDS', 20))),
        )
        grafana_admin_user = str(
            existing_env.get('GRAFANA_ADMIN_USER', os.getenv('GRAFANA_ADMIN_USER', 'admin'))
        ).strip() or 'admin'
        grafana_admin_password = str(
            existing_env.get('GRAFANA_ADMIN_PASSWORD', os.getenv('GRAFANA_ADMIN_PASSWORD', 'admin'))
        )
        grafana_proxy_bearer_token = str(
            existing_env.get('GRAFANA_PROXY_BEARER_TOKEN', os.getenv('GRAFANA_PROXY_BEARER_TOKEN', ''))
        ).strip()
        grafana_proxy_basic_user = str(
            existing_env.get(
                'GRAFANA_PROXY_BASIC_USER',
                os.getenv('GRAFANA_PROXY_BASIC_USER', grafana_admin_user),
            )
        ).strip()
        grafana_proxy_basic_password = str(
            existing_env.get(
                'GRAFANA_PROXY_BASIC_PASSWORD',
                os.getenv('GRAFANA_PROXY_BASIC_PASSWORD', grafana_admin_password),
            )
        )
        grafana_panel_map = str(
            data.get(
                'GRAFANA_HEALTH_PANEL_MAP',
                existing_env.get(
                    'GRAFANA_HEALTH_PANEL_MAP',
                    'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6',
                ),
            )
        ).strip()
        if not grafana_panel_map:
            grafana_panel_map = 'throughput:1,signal:2,active_sensors:3,active_base_stations:4,top_sensors:5,recent_messages:6'

        def _parse_float(value, default):
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        def _parse_threshold_pair(
            warn_key,
            crit_key,
            default_warn,
            default_crit,
            minimum=0.0,
        ):
            warn = max(float(minimum), _parse_float(data.get(warn_key, default_warn), default_warn))
            crit = max(warn, _parse_float(data.get(crit_key, default_crit), default_crit))
            return round(warn, 2), round(crit, 2)

        monitor_queue_warn_pct, monitor_queue_crit_pct = _parse_threshold_pair(
            'MONITOR_QUEUE_WARN_PCT',
            'MONITOR_QUEUE_CRIT_PCT',
            65.0,
            85.0,
        )
        monitor_retry_warn_pct, monitor_retry_crit_pct = _parse_threshold_pair(
            'MONITOR_RETRY_FAIL_WARN_PCT',
            'MONITOR_RETRY_FAIL_CRIT_PCT',
            5.0,
            20.0,
        )
        monitor_db_latency_warn_ms, monitor_db_latency_crit_ms = _parse_threshold_pair(
            'MONITOR_DB_WRITE_LATENCY_WARN_MS',
            'MONITOR_DB_WRITE_LATENCY_CRIT_MS',
            500.0,
            1500.0,
        )
        monitor_reconnect_warn_per_hour, monitor_reconnect_crit_per_hour = _parse_threshold_pair(
            'MONITOR_MQTT_RECONNECT_WARN_PER_HOUR',
            'MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR',
            3.0,
            8.0,
        )

        # Update the .env file - this is the primary configuration source
        env_content = f"""# TLS Server Configuration
LISTEN_HOST={data.get('LISTEN_HOST', '0.0.0.0')}
LISTEN_PORT={data.get('LISTEN_PORT', 16018)}

# SSL/TLS Certificate Configuration
CERT_FILE={cert_file}
KEY_FILE={key_file}
CA_FILE={ca_file}
TLS_CLIENT_CERT_MODE={tls_client_cert_mode}

# MQTT Configuration
MQTT_ENABLED={str(_to_bool(data.get('MQTT_ENABLED', True), True)).lower()}
MQTT_BROKER={data.get('MQTT_BROKER', 'localhost')}
MQTT_PORT={data.get('MQTT_PORT', 1883)}
MQTT_USERNAME={data.get('MQTT_USERNAME', '')}
MQTT_PASSWORD={data.get('MQTT_PASSWORD', '')}
BASE_TOPIC={data.get('BASE_TOPIC', 'bssci/')}

# Monitoring / Alert thresholds
MONITOR_QUEUE_WARN_PCT={monitor_queue_warn_pct}
MONITOR_QUEUE_CRIT_PCT={monitor_queue_crit_pct}
MONITOR_RETRY_FAIL_WARN_PCT={monitor_retry_warn_pct}
MONITOR_RETRY_FAIL_CRIT_PCT={monitor_retry_crit_pct}
MONITOR_DB_WRITE_LATENCY_WARN_MS={monitor_db_latency_warn_ms}
MONITOR_DB_WRITE_LATENCY_CRIT_MS={monitor_db_latency_crit_ms}
MONITOR_MQTT_RECONNECT_WARN_PER_HOUR={monitor_reconnect_warn_per_hour}
MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR={monitor_reconnect_crit_per_hour}

# Application Configuration
SENSOR_CONFIG_FILE=endpoints.json
STATUS_INTERVAL={data.get('STATUS_INTERVAL', 30)}
DEDUPLICATION_DELAY={data.get('DEDUPLICATION_DELAY', 2.0)}

# Web Interface Configuration
WEB_HOST=0.0.0.0
WEB_PORT=5000
WEB_DEBUG=false

# Auto-detach Configuration
AUTO_DETACH_ENABLED={str(data.get('AUTO_DETACH_ENABLED', True)).lower()}
AUTO_DETACH_TIMEOUT={auto_detach_timeout}
AUTO_DETACH_HOURS={auto_detach_timeout // 3600}
AUTO_DETACH_WARNING_TIMEOUT={auto_detach_warning_timeout}
AUTO_DETACH_WARNING_HOURS={auto_detach_warning_timeout // 3600}
AUTO_DETACH_CHECK_INTERVAL={auto_detach_check_interval}

# Timezone Configuration
TIMEZONE={data.get('TIMEZONE', 'Europe/Berlin')}
APP_LANGUAGE={app_language}

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/bssci_service.log

# Optional modules
OMS_ENABLED={str(_to_bool(data.get('OMS_ENABLED', True), True)).lower()}
MQTT_UI_ENABLED={str(_to_bool(data.get('MQTT_UI_ENABLED', True), True)).lower()}
BS_UPTIME_PANEL_ENABLED={str(_to_bool(data.get('BS_UPTIME_PANEL_ENABLED', False), False)).lower()}

# Telemetry Source
# auto | runtime | influx
TELEMETRY_SOURCE={telemetry_source}

# InfluxDB (optional - needed if TELEMETRY_SOURCE=influx/auto)
INFLUX_ENABLED={str(influx_enabled).lower()}
INFLUXDB_URL={data.get('INFLUXDB_URL', '')}
INFLUXDB_ORG={data.get('INFLUXDB_ORG', '')}
INFLUXDB_BUCKET={data.get('INFLUXDB_BUCKET', '')}
INFLUXDB_TOKEN={data.get('INFLUXDB_TOKEN', '')}
INFLUXDB_VERIFY_SSL={str(_to_bool(data.get('INFLUXDB_VERIFY_SSL', True), True)).lower()}
INFLUX_UPTIME_MEASUREMENT={data.get('INFLUX_UPTIME_MEASUREMENT', 'bssci_bs_uptime')}
INFLUX_UPTIME_FIELD={data.get('INFLUX_UPTIME_FIELD', 'status')}
INFLUX_UPTIME_EUI_TAG={data.get('INFLUX_UPTIME_EUI_TAG', 'eui')}
INFLUX_UPTIME_QUERY={influx_uptime_query}
INFLUX_INVENTORY_WRITE_ENABLED={str(_to_bool(data.get('INFLUX_INVENTORY_WRITE_ENABLED', True), True)).lower()}
INFLUX_INVENTORY_MEASUREMENT={data.get('INFLUX_INVENTORY_MEASUREMENT', 'bssci_inventory_events')}
INFLUX_SNAPSHOT_ENABLED={str(_to_bool(data.get('INFLUX_SNAPSHOT_ENABLED', True), True)).lower()}
INFLUX_SNAPSHOT_INTERVAL_SECONDS={influx_snapshot_interval}
INFLUX_SNAPSHOT_MEASUREMENT={data.get('INFLUX_SNAPSHOT_MEASUREMENT', 'bssci_inventory_snapshot')}

# TimescaleDB/PostgreSQL (optional - recommended for multi-tenant operational store)
TIMESCALE_ENABLED={str(_to_bool(data.get('TIMESCALE_ENABLED', False), False)).lower()}
TIMESCALE_HOST={data.get('TIMESCALE_HOST', 'timescaledb')}
TIMESCALE_PORT={timescale_port}
TIMESCALE_DB={data.get('TIMESCALE_DB', 'bssci')}
TIMESCALE_USER={data.get('TIMESCALE_USER', 'bssci_user')}
TIMESCALE_PASSWORD={data.get('TIMESCALE_PASSWORD', '')}
TIMESCALE_SSLMODE={timescale_sslmode}
TIMESCALE_DEFAULT_TENANT={data.get('TIMESCALE_DEFAULT_TENANT', 'default')}
TIMESCALE_INVENTORY_WRITE_ENABLED={str(_to_bool(data.get('TIMESCALE_INVENTORY_WRITE_ENABLED', True), True)).lower()}
TIMESCALE_TELEMETRY_WRITE_ENABLED={str(_to_bool(data.get('TIMESCALE_TELEMETRY_WRITE_ENABLED', True), True)).lower()}
TIMESCALE_SNAPSHOT_ENABLED={str(_to_bool(data.get('TIMESCALE_SNAPSHOT_ENABLED', True), True)).lower()}
TIMESCALE_SNAPSHOT_INTERVAL_SECONDS={timescale_snapshot_interval}
TIMESCALE_RETENTION_ENABLED={str(_to_bool(data.get('TIMESCALE_RETENTION_ENABLED', True), True)).lower()}
TIMESCALE_TELEMETRY_RETENTION_DAYS={timescale_telemetry_retention_days}
TIMESCALE_INVENTORY_RETENTION_DAYS={timescale_inventory_retention_days}
TIMESCALE_COMPRESSION_ENABLED={str(_to_bool(data.get('TIMESCALE_COMPRESSION_ENABLED', True), True)).lower()}
TIMESCALE_COMPRESSION_AFTER_DAYS={timescale_compression_after_days}

# Grafana
GRAFANA_ADMIN_USER={grafana_admin_user}
GRAFANA_ADMIN_PASSWORD={grafana_admin_password}
GRAFANA_URL={grafana_url}
GRAFANA_INTERNAL_URL={grafana_internal_url}
GRAFANA_DASHBOARD_UID={grafana_dashboard_uid}
GRAFANA_DASHBOARD_SLUG={grafana_dashboard_slug}
GRAFANA_ORG_ID={grafana_org_id}
GRAFANA_EMBED_ENABLED={str(grafana_embed_enabled).lower()}
GRAFANA_ANONYMOUS_ENABLED={str(grafana_anonymous_enabled).lower()}
GRAFANA_ANONYMOUS_ORG_ROLE={grafana_anonymous_role}
GRAFANA_PROXY_ENABLED={str(grafana_proxy_enabled).lower()}
GRAFANA_PROXY_TIMEOUT_SECONDS={grafana_proxy_timeout}
GRAFANA_PROXY_BEARER_TOKEN={grafana_proxy_bearer_token}
GRAFANA_PROXY_BASIC_USER={grafana_proxy_basic_user}
GRAFANA_PROXY_BASIC_PASSWORD={grafana_proxy_basic_password}
GRAFANA_HEALTH_PANEL_MAP={grafana_panel_map}

# Security
SECRET_KEY={secret_key}"""
        
        # Write to .env file with error handling for Docker environments
        try:
            with open('.env', 'w') as f:
                f.write(env_content)
        except PermissionError as pe:
            # Try alternative approach for Docker/Synology environments
            try:
                import tempfile
                import shutil
                # Write to temp file first, then move
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
                    tmp.write(env_content)
                    tmp_name = tmp.name
                shutil.move(tmp_name, '.env')
            except Exception as fallback_error:
                raise Exception(f"Cannot write .env file. Docker volume not mounted as writable? Original error: {pe}, Fallback error: {fallback_error}")
        
        # Reload environment variables
        from dotenv import load_dotenv
        load_dotenv(override=True)
            
        # Force reload of the bssci_config module to pick up new .env values
        import importlib
        import sys
        if 'bssci_config' in sys.modules:
            importlib.reload(sys.modules['bssci_config'])
        
        safe_changed_keys = []
        for raw_key in (data.keys() if isinstance(data, dict) else []):
            key = str(raw_key or "").strip()
            key_lower = key.lower()
            if not key:
                continue
            if any(marker in key_lower for marker in _AUDIT_SENSITIVE_KEY_MARKERS):
                continue
            safe_changed_keys.append(key)
        safe_changed_keys = sorted(set(safe_changed_keys))
        _record_admin_audit(
            action='config.update',
            entity='config',
            target_id='.env',
            status='success',
            details={
                'changed_keys': safe_changed_keys,
                'changed_count': len(safe_changed_keys),
                'timescale_enabled': _to_bool(data.get('TIMESCALE_ENABLED', False), False),
                'telemetry_source': telemetry_source,
                'mqtt_enabled': _to_bool(data.get('MQTT_ENABLED', True), True),
                'oms_enabled': _to_bool(data.get('OMS_ENABLED', True), True),
                'mqtt_ui_enabled': _to_bool(data.get('MQTT_UI_ENABLED', True), True),
                'bs_uptime_panel_enabled': _to_bool(data.get('BS_UPTIME_PANEL_ENABLED', False), False),
                'app_language': app_language,
            },
        )
        
        return jsonify({'success': True, 'message': 'Configuration updated in .env file and reloaded successfully.'})
    except Exception as e:
        print(f"Error updating config: {e}")
        return jsonify({'success': False, 'message': f'Configuration update failed: {str(e)}'})

@app.route('/certificates')
@login_required
@permission_required('can_manage_certificates')
def certificates():
    return render_template('certificates.html')

@app.route('/logs')
@admin_scope_required('view_service_logs')
def logs():
    return render_template('logs.html')

@app.route('/mqtt')
@login_required
@internal_portal_required
def mqtt():
    if not getattr(bssci_config, 'MQTT_UI_ENABLED', True):
        return redirect(url_for('index'))
    return render_template('mqtt.html')

@app.route('/documentation')
@login_required
def documentation():
    return render_template('documentation.html')

@app.route('/traffic')
@login_required
@internal_portal_required
def traffic():
    return redirect(url_for('health'))

@app.route('/oms')
@login_required
@internal_portal_required
def oms():
    if not getattr(bssci_config, 'OMS_ENABLED', True):
        return redirect(url_for('index'))
    return render_template('oms.html')

@app.route('/health')
@login_required
@internal_portal_required
def health():
    return render_template('health.html')

def _build_health_stats_payload():
    """Build comprehensive health statistics for dashboard and health views."""
    global tls_server_instance

    result = {
        "success": True,
        "system": {
            "uptime": 0,
            "total_sensors": 0,
            "active_sensors": 0,
            "total_base_stations": 0,
            "connected_base_stations": 0,
            "total_packets_received": 0,
            "total_packets_lost": 0,
            "overall_packet_loss_rate": 0,
            "avg_snr": 0,
            "avg_rssi": 0
        },
        "base_stations": [],
        "sensors": [],
        "snr_rssi_history": []
    }
    timescale_summary = _timescale_fetch_telemetry_summary(window_minutes=24 * 60, bucket_seconds=300, top_limit=10)
    result["timescale"] = timescale_summary

    if tls_server_instance:
        start_time = tls_server_instance.traffic_metrics.get('start_time', 0)
        if start_time:
            result["system"]["uptime"] = int(datetime.now(timezone.utc).timestamp() - start_time)

        result["system"]["total_sensors"] = len(tls_server_instance.sensor_config)
        result["system"]["active_sensors"] = len(tls_server_instance.active_sensors_hourly)
        result["system"]["total_base_stations"] = len(tls_server_instance.connected_base_stations) + len(tls_server_instance.connecting_base_stations)
        result["system"]["connected_base_stations"] = len(tls_server_instance.connected_base_stations)

        total_received = 0
        total_lost = 0
        for eui, stats in tls_server_instance.sensor_packet_stats.items():
            total_received += stats.get('packets_received', 0)
            total_lost += stats.get('packets_lost', 0)

        result["system"]["total_packets_received"] = total_received
        result["system"]["total_packets_lost"] = total_lost
        if total_received + total_lost > 0:
            result["system"]["overall_packet_loss_rate"] = round(total_lost / (total_received + total_lost) * 100, 2)

        bs_config = load_base_station_config().get("base_stations", {})
        for writer, bs_eui in tls_server_instance.connected_base_stations.items():
            eui_lower = bs_eui.lower()
            health = tls_server_instance.base_station_health.get(eui_lower, {})
            bs_info = bs_config.get(eui_lower, {})
            result["base_stations"].append({
                "eui": eui_lower,
                "name": bs_info.get("name", ""),
                "status": "connected",
                "cpu": health.get("cpu", 0),
                "memory": health.get("memory", 0),
                "duty_cycle": health.get("duty_cycle", 0),
                "uptime": health.get("uptime", 0)
            })

        for eui, stats in tls_server_instance.sensor_packet_stats.items():
            received = stats.get('packets_received', 0)
            lost = stats.get('packets_lost', 0)
            snr_avg = stats.get('snr_sum', 0) / max(stats.get('snr_count', 1), 1)
            rssi_avg = stats.get('rssi_sum', 0) / max(stats.get('rssi_count', 1), 1)
            loss_rate = 0
            if received + lost > 0:
                loss_rate = round(lost / (received + lost) * 100, 2)

            result["sensors"].append({
                "eui": eui.lower(),
                "packets_received": received,
                "packets_lost": lost,
                "packet_loss_rate": loss_rate,
                "avg_snr": round(snr_avg, 2),
                "avg_rssi": round(rssi_avg, 2)
            })

        result["sensors"].sort(key=lambda x: x["packet_loss_rate"], reverse=True)

        total_snr = 0
        total_rssi = 0
        sensor_count = 0
        for stats in tls_server_instance.sensor_packet_stats.values():
            if stats.get('snr_count', 0) > 0:
                total_snr += stats['snr_sum'] / stats['snr_count']
                total_rssi += stats['rssi_sum'] / stats['rssi_count']
                sensor_count += 1

        if sensor_count > 0:
            result["system"]["avg_snr"] = round(total_snr / sensor_count, 2)
            result["system"]["avg_rssi"] = round(total_rssi / sensor_count, 2)

        result["snr_rssi_history"] = tls_server_instance.snr_rssi_history

        distribution = {"excellent": 0, "good": 0, "fair": 0, "poor": 0, "critical": 0}
        for stats in tls_server_instance.sensor_packet_stats.values():
            if stats.get('snr_count', 0) > 0:
                avg_snr = stats['snr_sum'] / stats['snr_count']
                if avg_snr >= 10:
                    distribution["excellent"] += 1
                elif avg_snr >= 5:
                    distribution["good"] += 1
                elif avg_snr >= 0:
                    distribution["fair"] += 1
                elif avg_snr >= -5:
                    distribution["poor"] += 1
                else:
                    distribution["critical"] += 1
        result["signal_distribution"] = distribution

    if not result["sensors"] and timescale_summary.get("success"):
        for sensor in timescale_summary.get("top_sensors", []):
            result["sensors"].append({
                "eui": str(sensor.get("sensor_eui", "")).lower(),
                "packets_received": int(sensor.get("uplinks", 0) or 0),
                "packets_lost": 0,
                "packet_loss_rate": float(sensor.get("avg_packet_loss_pct", 0.0) or 0.0),
                "avg_snr": round(float(sensor.get("avg_snr", 0.0) or 0.0), 2),
                "avg_rssi": round(float(sensor.get("avg_rssi", 0.0) or 0.0), 2),
            })
        result["sensors"].sort(key=lambda x: x["packet_loss_rate"], reverse=True)

    if not result["snr_rssi_history"] and timescale_summary.get("success"):
        result["snr_rssi_history"] = [
            {
                "timestamp": point.get("timestamp"),
                "avg_snr": point.get("avg_snr"),
                "avg_rssi": point.get("avg_rssi"),
            }
            for point in (timescale_summary.get("series") or [])
        ]

    return result

@app.route('/api/health', methods=['GET'])
@login_required
@internal_portal_required
def get_health_stats():
    try:
        return jsonify(_build_health_stats_payload())
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/base-stations')
@login_required
@internal_portal_required
def base_stations():
    telemetry_source = (getattr(bssci_config, 'TELEMETRY_SOURCE', 'auto') or 'auto').strip().lower()
    if telemetry_source not in {'auto', 'runtime', 'influx'}:
        telemetry_source = 'auto'
    influx_enabled = bool(getattr(bssci_config, 'INFLUX_ENABLED', True))
    if not influx_enabled and telemetry_source in {'auto', 'influx'}:
        telemetry_source = 'runtime'
    influx_configured = bool(
        getattr(bssci_config, 'INFLUXDB_URL', '')
        and getattr(bssci_config, 'INFLUXDB_ORG', '')
        and getattr(bssci_config, 'INFLUXDB_BUCKET', '')
        and getattr(bssci_config, 'INFLUXDB_TOKEN', '')
    )
    return render_template(
        'base_stations.html',
        telemetry_source=telemetry_source,
        influx_enabled=influx_enabled,
        influx_configured=influx_configured,
        bs_uptime_panel_enabled=bool(getattr(bssci_config, 'BS_UPTIME_PANEL_ENABLED', False)),
    )

@app.route('/base-stations/<eui>')
@login_required
@internal_portal_required
def base_station_detail_page(eui):
    return render_template('base_station_detail.html', base_station_eui=str(eui or '').strip().upper())

@app.route('/network')
@login_required
@internal_portal_required
def network():
    return render_template('network.html')

@app.route('/coverage')
@login_required
@internal_portal_required
def coverage():
    return redirect(url_for('network'))

def _normalize_eui_upper(value: Any) -> str:
    return str(value or "").strip().upper()

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _build_sensor_name_index(sensor_config: List[Dict[str, Any]]) -> Dict[str, str]:
    index = {}
    for sensor in sensor_config or []:
        if not isinstance(sensor, dict):
            continue
        sensor_eui = _normalize_eui_upper(sensor.get("eui", ""))
        if not sensor_eui:
            continue
        sensor_name = str(sensor.get("name", "") or "").strip()
        index[sensor_eui] = sensor_name if sensor_name else f"{sensor_eui[:8]}..."
    return index

def _load_configured_sensors_index() -> Dict[str, Dict[str, Any]]:
    """
    Load configured sensors from the DB-first inventory store.
    Returns mapping:
      EUI_UPPER -> {"name": str, "tags": list[str], "bidi": bool, "payload_decoder": str, "attached_base_stations": list[str]}
    """
    result: Dict[str, Dict[str, Any]] = {}
    try:
        sensors = _load_all_sensors()
    except Exception:
        sensors = []

    active_tenant = _active_tenant_id()
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
            continue
        sensor_eui = _normalize_eui_upper(sensor.get("eui", ""))
        if not sensor_eui:
            continue
        sensor_name = str(sensor.get("name", "") or "").strip()
        result[sensor_eui] = {
            "name": sensor_name if sensor_name else f"{sensor_eui[:8]}...",
            "tags": _normalize_sensor_tags(sensor.get("tags", [])),
            "bidi": bool(sensor.get("bidi", False)),
            "payload_decoder": str(sensor.get("payload_decoder", "auto") or "auto"),
            "attached_base_stations": _normalize_base_station_route_list(
                sensor.get("attached_base_stations", [])
            ),
        }
    return result

def _collect_network_snapshot() -> Dict[str, Any]:
    """
    Build one normalized snapshot used by both:
      - /api/coverage/topology
      - /api/network
    """
    global tls_server_instance

    active_tenant = _active_tenant_id()
    bs_config_raw = load_base_station_config().get("base_stations", {}) or {}
    bs_config = {}
    for eui_key, bs_data in bs_config_raw.items():
        bs_eui = _normalize_eui_upper(eui_key)
        if not bs_eui:
            continue
        if not _tenant_matches(_tenant_id_from_base_station(bs_data), active_tenant):
            continue
        bs_config[bs_eui] = bs_data if isinstance(bs_data, dict) else {}

    connected_bs = set()
    bs_health = {}
    sensor_topology = {}
    sensor_name_index = {}
    configured_sensors = _load_configured_sensors_index()
    configured_sensor_routes: Dict[str, List[str]] = {
        sensor_eui: _normalize_base_station_route_list(meta.get("attached_base_stations", []))
        for sensor_eui, meta in configured_sensors.items()
    }
    runtime_registered_sensors = set()
    registered_sensor_routes: Dict[str, List[str]] = {}

    if tls_server_instance:
        connected_map = getattr(tls_server_instance, "connected_base_stations", {}) or {}
        connected_bs = {
            _normalize_eui_upper(bs_eui)
            for bs_eui in connected_map.values()
            if _normalize_eui_upper(bs_eui)
        }
        bs_health = getattr(tls_server_instance, "base_station_health", {}) or {}
        sensor_topology = getattr(tls_server_instance, "sensor_topology", {}) or {}
        sensor_name_index = _build_sensor_name_index(getattr(tls_server_instance, "sensor_config", []) or [])
        registered_map = getattr(tls_server_instance, "registered_sensors", {}) or {}
        runtime_registered_sensors = {
            _normalize_eui_upper(sensor_eui)
            for sensor_eui in registered_map.keys()
            if _normalize_eui_upper(sensor_eui)
        }
        for sensor_eui_raw, reg_raw in registered_map.items():
            sensor_eui = _normalize_eui_upper(sensor_eui_raw)
            if not sensor_eui:
                continue

            # Keep tenant-safe inventory scope when building relation graph.
            if sensor_eui not in configured_sensors and sensor_eui not in sensor_topology:
                continue

            reg_payload = reg_raw if isinstance(reg_raw, dict) else {}
            bs_candidates = reg_payload.get("base_stations", [])
            if isinstance(bs_candidates, str):
                bs_candidates = [bs_candidates]
            elif not isinstance(bs_candidates, (list, tuple, set)):
                bs_candidates = []

            routes: List[str] = []
            for bs_candidate in bs_candidates:
                bs_eui = _normalize_eui_upper(bs_candidate)
                if not bs_eui:
                    continue
                if bs_eui not in routes:
                    routes.append(bs_eui)
            if routes:
                registered_sensor_routes[sensor_eui] = routes

    # Merge runtime-loaded sensor names over file-loaded names.
    for sensor_eui, sensor_name in sensor_name_index.items():
        configured_sensors.setdefault(sensor_eui, {
            "name": sensor_name,
            "tags": [],
            "bidi": False
        })
        configured_sensors[sensor_eui]["name"] = sensor_name

    all_bs = set(bs_config.keys()) | connected_bs
    coverage_sensors = {}
    sensor_nodes = []
    edges = []
    edge_ids = set()
    edge_pairs = set()

    def _classify_live_link_status(snr_value: float) -> tuple[str, bool]:
        snr = _safe_float(snr_value, -100.0)
        if snr >= 10:
            return "good", False
        if snr >= 0:
            return "fair", False
        if snr >= -5:
            return "poor", True
        return "critical", True

    for sensor_eui_raw, topo_raw in sensor_topology.items():
        sensor_eui = _normalize_eui_upper(sensor_eui_raw)
        if not sensor_eui:
            continue

        topo = topo_raw if isinstance(topo_raw, dict) else {}
        receiving_raw = topo.get("receiving_bases", {})
        receiving = receiving_raw if isinstance(receiving_raw, dict) else {}
        primary_bs = _normalize_eui_upper(topo.get("primary_bs", ""))

        coverage_receiving = {}
        for bs_eui_raw, bs_stats_raw in receiving.items():
            bs_eui = _normalize_eui_upper(bs_eui_raw)
            if not bs_eui:
                continue

            all_bs.add(bs_eui)
            bs_stats = bs_stats_raw if isinstance(bs_stats_raw, dict) else {}
            snr = round(_safe_float(bs_stats.get("snr", 0.0), 0.0), 2)
            rssi = round(_safe_float(bs_stats.get("rssi", -100.0), -100.0), 2)
            count = int(_safe_float(bs_stats.get("count", 0), 0))
            last_seen = _safe_float(bs_stats.get("last_seen", 0), 0.0)

            coverage_receiving[bs_eui] = {
                "snr": snr,
                "rssi": rssi,
                "count": count
            }

            edge_id = f"edge_{sensor_eui}_{bs_eui}"
            if edge_id not in edge_ids:
                link_status, problematic = _classify_live_link_status(snr)
                edge_ids.add(edge_id)
                edge_pairs.add((sensor_eui, bs_eui))
                edges.append({
                    "id": edge_id,
                    "source": f"sensor_{sensor_eui}",
                    "target": f"bs_{bs_eui}",
                    "primary": bs_eui == primary_bs,
                    "snr": snr,
                    "rssi": rssi,
                    "last_seen": last_seen,
                    "count": count,
                    "route_kind": "live",
                    "link_status": link_status,
                    "problematic": problematic
                })

        if coverage_receiving:
            coverage_sensors[sensor_eui] = {
                "base_stations": coverage_receiving
            }

        assigned_bases = []
        for candidate in (registered_sensor_routes.get(sensor_eui, []), configured_sensor_routes.get(sensor_eui, [])):
            for bs_eui in candidate:
                if bs_eui not in assigned_bases:
                    assigned_bases.append(bs_eui)
        if not primary_bs and assigned_bases:
            primary_bs = assigned_bases[0]
        receiver_count = max(len(coverage_receiving), len(assigned_bases))

        sensor_nodes.append({
            "id": f"sensor_{sensor_eui}",
            "type": "sensor",
            "eui": sensor_eui,
            "label": configured_sensors.get(sensor_eui, {}).get("name", sensor_name_index.get(sensor_eui, f"{sensor_eui[:8]}...")),
            "primary_bs": primary_bs,
            "receiver_count": receiver_count,
            "live_receiver_count": len(coverage_receiving),
            "assigned_bases": assigned_bases,
            "configured": sensor_eui in configured_sensors,
            "registered": sensor_eui in runtime_registered_sensors
        })

    # Include configured sensors even when there is currently no live topology.
    existing_sensor_euis = {str(node.get("eui", "")).upper() for node in sensor_nodes}
    for sensor_eui, sensor_meta in configured_sensors.items():
        if sensor_eui in existing_sensor_euis:
            continue
        assigned_bases = []
        for candidate in (registered_sensor_routes.get(sensor_eui, []), configured_sensor_routes.get(sensor_eui, [])):
            for bs_eui in candidate:
                if bs_eui not in assigned_bases:
                    assigned_bases.append(bs_eui)
        primary_bs = assigned_bases[0] if assigned_bases else ""
        sensor_nodes.append({
            "id": f"sensor_{sensor_eui}",
            "type": "sensor",
            "eui": sensor_eui,
            "label": str(sensor_meta.get("name", f"{sensor_eui[:8]}...")),
            "primary_bs": primary_bs,
            "receiver_count": len(assigned_bases),
            "live_receiver_count": 0,
            "assigned_bases": assigned_bases,
            "configured": True,
            "registered": sensor_eui in runtime_registered_sensors
        })

    # Add assignment edges from registration data when live topology edge does not exist.
    for sensor_node in sensor_nodes:
        sensor_eui = _normalize_eui_upper(sensor_node.get("eui", ""))
        if not sensor_eui:
            continue
        primary_bs = _normalize_eui_upper(sensor_node.get("primary_bs", ""))
        assigned_bases = sensor_node.get("assigned_bases", [])
        if not isinstance(assigned_bases, list):
            continue
        registered_bs_set = set(registered_sensor_routes.get(sensor_eui, []))
        configured_bs_set = set(configured_sensor_routes.get(sensor_eui, []))

        for bs_eui in assigned_bases:
            bs_eui_upper = _normalize_eui_upper(bs_eui)
            if not bs_eui_upper:
                continue
            all_bs.add(bs_eui_upper)
            edge_key = (sensor_eui, bs_eui_upper)
            if edge_key in edge_pairs:
                continue

            edge_id = f"edge_registration_{sensor_eui}_{bs_eui_upper}"
            if edge_id in edge_ids:
                continue

            edge_ids.add(edge_id)
            edge_pairs.add(edge_key)
            if bs_eui_upper in registered_bs_set:
                route_kind = "registration"
            elif bs_eui_upper in configured_bs_set:
                route_kind = "configured"
            else:
                route_kind = "configured"
            is_bs_connected = bs_eui_upper in connected_bs
            link_status = "assigned" if is_bs_connected else "assigned_offline"
            edges.append({
                "id": edge_id,
                "source": f"sensor_{sensor_eui}",
                "target": f"bs_{bs_eui_upper}",
                "primary": bs_eui_upper == primary_bs,
                "snr": None,
                "rssi": None,
                "last_seen": 0,
                "count": 0,
                "route_kind": route_kind,
                "link_status": link_status,
                "problematic": not is_bs_connected
            })

    base_station_nodes = []
    for bs_eui in sorted(all_bs):
        bs_cfg = bs_config.get(bs_eui, {})
        health = bs_health.get(bs_eui.lower(), {}) or bs_health.get(bs_eui, {}) or {}
        bs_name = str(bs_cfg.get("name", "") or "").strip()

        base_station_nodes.append({
            "id": f"bs_{bs_eui}",
            "type": "base_station",
            "eui": bs_eui,
            "label": bs_name if bs_name else f"{bs_eui[:8]}...",
            "connected": bs_eui in connected_bs,
            "cpu": round(_safe_float(health.get("cpu", 0.0), 0.0), 1),
            "memory": round(_safe_float(health.get("memory", 0.0), 0.0), 1),
            "duty_cycle": round(_safe_float(health.get("duty_cycle", 0.0), 0.0), 1)
        })

    sensor_nodes.sort(key=lambda item: (item.get("label", ""), item.get("eui", "")))
    edges.sort(key=lambda item: item.get("id", ""))

    return {
        "coverage": {
            "sensors": coverage_sensors,
            "base_stations": sorted(all_bs)
        },
        "topology": {
            "nodes": base_station_nodes + sensor_nodes,
            "edges": edges
        },
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "base_station_count": len(base_station_nodes),
            "connected_base_station_count": len(connected_bs),
            "sensor_count": len(sensor_nodes),
            "edge_count": len(edges)
        }
    }

def _build_network_topology_payload() -> Dict[str, Any]:
    """Build normalized topology payload for customer dashboard and internal network view."""
    snapshot = _collect_network_snapshot()
    return {
        'success': True,
        'nodes': snapshot["topology"]["nodes"],
        'edges': snapshot["topology"]["edges"],
        'meta': snapshot["meta"]
    }

@app.route('/api/coverage/topology')
@login_required
@internal_portal_required
def api_coverage_topology():
    """Get sensor topology with SNR/RSSI per base station for coverage heatmap"""
    try:
        snapshot = _collect_network_snapshot()
        return jsonify({
            **snapshot["coverage"],
            "meta": snapshot["meta"]
        })
    except Exception as e:
        logger.exception("Failed to build coverage topology snapshot")
        return jsonify({'sensors': {}, 'base_stations': [], 'error': str(e)})

def _build_coverage_positions_read_payload() -> Dict[str, Any]:
    """Build tenant-filtered coverage positions payload for map-based customer views."""
    positions_file = _coverage_positions_file()
    active_tenant = _active_tenant_id()
    tenant_sensor_keys = {
        f"sensor_{str(sensor.get('eui', '')).strip().upper()}"
        for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
    }
    tenant_bs_keys = {
        f"bs_{str(eui).strip().upper()}"
        for eui in _filter_base_stations_for_tenant(
            load_base_station_config().get("base_stations", {}),
            tenant_id=active_tenant,
        ).keys()
    }
    allowed_keys = tenant_sensor_keys | tenant_bs_keys

    _sync_coverage_positions_to_inventory(
        tenant_id=active_tenant,
        only_missing=True,
        state=_load_coverage_positions_state(),
        allowed_keys=allowed_keys,
    )

    state = _sync_inventory_gps_to_coverage_positions()
    if state:
        filtered_state = dict(state)
        positions = state.get("positions", {})
        if isinstance(positions, dict):
            filtered_state["positions"] = {
                key: value for key, value in positions.items()
                if key in allowed_keys
            }
        return filtered_state

    if os.path.exists(positions_file):
        with open(positions_file, 'r') as f:
            raw_state = json.load(f)
        if isinstance(raw_state, dict) and isinstance(raw_state.get("positions"), dict):
            filtered_state = dict(raw_state)
            filtered_state["positions"] = {
                key: value for key, value in raw_state.get("positions", {}).items()
                if key in allowed_keys
            }
            return filtered_state
        return raw_state if isinstance(raw_state, dict) else {}

    return {}

@app.route('/api/coverage/positions', methods=['GET', 'POST'])
@login_required
def api_coverage_positions():
    """Get or save coverage map device positions"""
    positions_file = _coverage_positions_file()
    
    if request.method == 'POST':
        perms = get_user_permissions()
        if not perms.get('can_edit_sensors', False):
            return jsonify({'success': False, 'error': 'Insufficient permissions'}), 403
        try:
            incoming = request.get_json() or {}
            if not isinstance(incoming, dict):
                return jsonify({'success': False, 'error': 'Invalid payload'}), 400

            active_tenant = _active_tenant_id()
            tenant_sensor_keys = {
                f"sensor_{str(sensor.get('eui', '')).strip().upper()}"
                for sensor in _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=active_tenant)
            }
            tenant_bs_keys = {
                f"bs_{str(eui).strip().upper()}"
                for eui in _filter_base_stations_for_tenant(
                    load_base_station_config().get("base_stations", {}),
                    tenant_id=active_tenant,
                ).keys()
            }
            editable_keys = tenant_sensor_keys | tenant_bs_keys

            state = _load_coverage_positions_state()
            if not isinstance(state, dict):
                state = {"positions": {}}
            current_positions = state.setdefault("positions", {})
            if not isinstance(current_positions, dict):
                current_positions = {}
                state["positions"] = current_positions

            incoming_positions = incoming.get("positions", incoming)
            incoming_positions = incoming_positions if isinstance(incoming_positions, dict) else {}

            for key in list(current_positions.keys()):
                if key in editable_keys:
                    current_positions.pop(key, None)

            normalized_incoming_positions = {}
            for key, value in incoming_positions.items():
                canonical_key, normalized_payload = _normalize_coverage_position_record(key, value)
                if not canonical_key or canonical_key not in editable_keys:
                    continue
                existing = normalized_incoming_positions.get(canonical_key)
                normalized_incoming_positions[canonical_key] = _merge_coverage_position_payload(existing, normalized_payload)

            current_positions.update(normalized_incoming_positions)

            for key, value in incoming.items():
                if key == "positions":
                    continue
                state[key] = value

            # Keep inventory GPS consistent with saved OSM positions from the map.
            sync_summary = _sync_coverage_positions_to_inventory(
                tenant_id=active_tenant,
                only_missing=False,
                state=state,
                allowed_keys=editable_keys,
            )

            with open(positions_file, 'w') as f:
                json.dump(state, f, indent=2)
            return jsonify({'success': True, 'synced_to_inventory': sync_summary})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        try:
            return jsonify(_build_coverage_positions_read_payload())
        except Exception as e:
            return jsonify({'error': str(e)}), 500

@app.route('/api/coverage/device-gps', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def api_coverage_device_gps():
    """Update device GPS from coverage map and sync into device settings."""
    try:
        payload = request.get_json() or {}
        raw_type = str(payload.get("device_type", "") or "").strip().lower()
        device_type = "sensor" if raw_type == "sensor" else ("bs" if raw_type in {"bs", "base_station"} else "")
        if not device_type:
            return jsonify({"success": False, "error": "Invalid device_type"}), 400

        eui = _normalize_eui_upper(payload.get("eui", ""))
        if not eui:
            return jsonify({"success": False, "error": "EUI is required"}), 400

        gps_lat, gps_lng = _normalize_gps_coordinates(payload.get("gps_lat"), payload.get("gps_lng"))
        if gps_lat is None or gps_lng is None:
            return jsonify({"success": False, "error": "Both latitude and longitude are required"}), 400

        if device_type == "sensor":
            _update_sensor_gps_by_eui(eui, gps_lat, gps_lng)
            _try_record_inventory_event(
                "sensor",
                "updated",
                eui,
                {"gps_lat": gps_lat, "gps_lng": gps_lng, "source": "coverage_map"}
            )
        else:
            _update_base_station_gps_by_eui(eui, gps_lat, gps_lng)
            _try_record_inventory_event(
                "base_station",
                "updated",
                eui.lower(),
                {"gps_lat": gps_lat, "gps_lng": gps_lng, "source": "coverage_map"}
            )

        _upsert_device_gps_position(device_type, eui, gps_lat, gps_lng)
        return jsonify({
            "success": True,
            "device_type": device_type,
            "eui": eui,
            "gps_lat": gps_lat,
            "gps_lng": gps_lng
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Failed to update GPS from coverage map")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/coverage/floorplan', methods=['GET', 'POST'])
@login_required
def api_coverage_floorplan():
    """Get or save floorplan image (base64 encoded)"""
    floorplan_file = 'coverage_floorplan.txt'
    
    if request.method == 'POST':
        perms = get_user_permissions()
        if not perms.get('can_edit_sensors', False):
            return jsonify({'success': False, 'error': 'Insufficient permissions'}), 403
        try:
            data = request.get_json()
            image_data = data.get('image', '')
            with open(floorplan_file, 'w') as f:
                f.write(image_data)
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        try:
            if os.path.exists(floorplan_file):
                with open(floorplan_file, 'r') as f:
                    return jsonify({'image': f.read()})
            return jsonify({'image': None})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

@app.route('/api/network')
@login_required
@internal_portal_required
def api_network():
    """Get network topology data for visualization"""
    try:
        return jsonify(_build_network_topology_payload())
    except Exception as e:
        logger.exception("Failed to build network topology snapshot")
        return jsonify({'success': False, 'error': str(e)}), 500

def load_base_station_config():
    """Load base station configuration from the DB-first store."""
    if _db_first_config_enabled():
        bootstrap_state = _bootstrap_base_stations_store_if_needed()
        payload, err = _load_base_station_payload_from_db()
        if isinstance(payload, dict):
            return payload
        if err:
            logger.warning("Falling back to %s for base station load: %s", BASE_STATIONS_RECOVERY_FILE, err)
        elif bootstrap_state.get("seeded"):
            logger.info("Bootstrapped base station store from %s", bootstrap_state.get("source"))
    try:
        config_path = getattr(bssci_config, 'BASE_STATION_CONFIG_FILE', 'base_stations.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"base_stations": {}}

def save_base_station_config(config):
    """Save base station configuration to the DB-first store."""
    if _db_first_config_enabled():
        ok, err = _save_base_station_payload_to_db(config)
        if ok:
            _invalidate_customer_dashboard_cache()
            return
        logger.warning("Falling back to %s for base station save: %s", BASE_STATIONS_RECOVERY_FILE, err)
    import os
    config_path = getattr(bssci_config, 'BASE_STATION_CONFIG_FILE', 'base_stations.json')
    try:
        dir_path = os.path.dirname(config_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
    except:
        pass
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    _invalidate_customer_dashboard_cache()

def _build_base_stations_runtime_payload() -> Dict[str, Any]:
    """Build base station runtime summary for customer-safe dashboard aggregation."""
    global tls_server_instance
    config = load_base_station_config()
    active_tenant = _active_tenant_id()
    bs_config = _filter_base_stations_for_tenant(config.get("base_stations", {}), tenant_id=active_tenant)

    connected_bs = {}
    connecting_bs = {}
    bs_health = {}
    bs_sensors = {}

    if tls_server_instance:
        status = tls_server_instance.get_base_station_status()
        for bs in status.get("connected", []):
            eui = bs["eui"].lower()
            connected_bs[eui] = bs
        for bs in status.get("connecting", []):
            eui = bs["eui"].lower()
            connecting_bs[eui] = bs
        if hasattr(tls_server_instance, 'base_station_health'):
            bs_health = tls_server_instance.base_station_health
        if hasattr(tls_server_instance, 'sensor_config'):
            for sensor in tls_server_instance.sensor_config:
                if not _tenant_matches(_tenant_id_from_sensor(sensor), active_tenant):
                    continue
                preferred = sensor.get('preferredDownlinkPath', {})
                if isinstance(preferred, dict):
                    bs_eui = preferred.get('baseStation', '').lower()
                    if bs_eui:
                        bs_sensors[bs_eui] = bs_sensors.get(bs_eui, 0) + 1

    normalized_bs_config = {}
    for raw_eui, raw_bs_data in (bs_config or {}).items():
        eui_lower = str(raw_eui or "").strip().lower()
        if not eui_lower:
            continue
        payload = dict(raw_bs_data) if isinstance(raw_bs_data, dict) else {}
        existing = normalized_bs_config.get(eui_lower)
        if existing is None:
            normalized_bs_config[eui_lower] = payload
        else:
            merged = dict(existing)
            merged.update({k: v for k, v in payload.items() if v not in (None, "", [], {})})
            normalized_bs_config[eui_lower] = merged

    result = []
    for eui_lower, bs_data in normalized_bs_config.items():
        if eui_lower in connected_bs:
            status = "connected"
        elif eui_lower in connecting_bs:
            status = "connecting"
        else:
            status = "offline"

        health = bs_health.get(eui_lower, {})
        last_status_change = ""
        last_status_event = ""
        status_age_seconds = None
        bs_events = bs_uptime_events.get(eui_lower, [])
        if bs_events:
            last = bs_events[-1]
            last_status_change = last.get("timestamp", "")
            last_status_event = last.get("event", "")
            try:
                ts = datetime.fromisoformat(last_status_change)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                status_age_seconds = max(0, int((datetime.now(timezone.utc) - ts).total_seconds()))
            except Exception:
                status_age_seconds = None

        result.append({
            "eui": eui_lower,
            "name": bs_data.get("name", ""),
            "tags": bs_data.get("tags", []),
            "tenant_id": bs_data.get("tenant_id", active_tenant),
            "status": status,
            "configured_ip": bs_data.get("ip", ""),
            "gps_lat": bs_data.get("gps_lat"),
            "gps_lng": bs_data.get("gps_lng"),
            "health": health,
            "connected_sensors": bs_sensors.get(eui_lower, 0),
            "last_status_event": last_status_event,
            "last_status_change": last_status_change,
            "status_age_seconds": status_age_seconds
        })

    result.sort(key=lambda x: (x["status"] != "connected", x["status"] != "connecting", x["eui"]))
    current_statuses = {bs["eui"]: bs["status"] for bs in result}
    _track_bs_status_changes(current_statuses)
    return {"success": True, "base_stations": result}

@app.route('/api/base-stations', methods=['GET'])
@login_required
@internal_portal_required
def get_base_stations():
    """Get all base stations with status and health data"""
    try:
        return jsonify(_build_base_stations_runtime_payload())
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/base-stations/storage-meta', methods=['GET'])
@login_required
@admin_scope_required('manage_tenants')
def get_base_stations_storage_meta():
    config = load_base_station_config()
    base_stations = (config or {}).get("base_stations", {}) if isinstance(config, dict) else {}
    storage_meta = _base_stations_storage_backend_meta()
    return jsonify({
        'success': True,
        'storage_backend': storage_meta.get('backend', 'json'),
        'storage_db_enabled': bool(storage_meta.get('db_enabled', False)),
        'bootstrap_state': storage_meta.get('bootstrap_state', {}),
        'seed_defaults_file': storage_meta.get('seed_defaults_file', BASE_STATIONS_RECOVERY_FILE),
        'recovery_file': storage_meta.get('recovery_file', BASE_STATIONS_RECOVERY_FILE),
        'base_station_count': len(base_stations or {}),
    })


@app.route('/api/base-stations/export', methods=['GET'])
@login_required
@admin_scope_required('manage_tenants')
def export_base_stations():
    try:
        active_tenant = _active_tenant_id()
        config = load_base_station_config()
        base_stations = _filter_base_stations_for_tenant((config or {}).get("base_stations", {}), tenant_id=active_tenant)
        storage_meta = _base_stations_storage_backend_meta()
        payload_rows = []
        for eui, base_station in sorted((base_stations or {}).items(), key=lambda item: str(item[0]).lower()):
            row = _serialize_base_station_for_export(eui, base_station)
            if row:
                payload_rows.append(row)
        payload = {
            'format_version': 1,
            'exported_at': datetime.now(timezone.utc).isoformat(),
            'storage_backend': storage_meta.get('backend', 'json'),
            'seed_defaults_file': storage_meta.get('seed_defaults_file', BASE_STATIONS_RECOVERY_FILE),
            'recovery_file': storage_meta.get('recovery_file', BASE_STATIONS_RECOVERY_FILE),
            'base_stations': payload_rows,
        }
        _record_admin_audit(
            action='base_station.export',
            entity='base_station',
            target_id='tenant',
            status='success',
            details={
                'tenant_id': active_tenant,
                'count': len(payload_rows),
                'storage_backend': storage_meta.get('backend', 'json'),
            },
        )
        filename = f"base_stations_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        return Response(
            json.dumps(payload, indent=2, ensure_ascii=False),
            mimetype='application/json',
            headers={'Content-Disposition': f'attachment; filename={filename}'},
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/base-stations/import', methods=['POST'])
@login_required
@admin_scope_required('manage_tenants')
def import_base_stations():
    try:
        payload = _parse_json_upload_or_payload()
    except ValueError as exc:
        _record_admin_audit(
            action='base_station.import',
            entity='base_station',
            target_id='tenant',
            status='error',
            details={'error': str(exc)},
        )
        return jsonify({'success': False, 'error': str(exc)}), 400

    imported = payload.get("data", payload) if isinstance(payload, dict) else {}
    rows_in = imported.get("base_stations", imported.get("items", [])) if isinstance(imported, dict) else []
    if isinstance(rows_in, dict):
        rows_in = [
            dict((value or {}), eui=key)
            for key, value in rows_in.items()
            if isinstance(value, dict)
        ]
    if not isinstance(rows_in, list):
        return jsonify({'success': False, 'error': 'Import payload must contain a base_stations list.'}), 400

    active_tenant = _active_tenant_id()
    storage_meta = _base_stations_storage_backend_meta()
    config = load_base_station_config()
    base_stations = dict((config or {}).get("base_stations", {}) or {})
    created = 0
    updated = 0
    skipped = []

    for entry in rows_in:
        if not isinstance(entry, dict):
            skipped.append({'reason': 'invalid_record'})
            continue
        eui = str(entry.get('eui') or '').strip().lower()
        if not eui or not _validate_eui(eui):
            skipped.append({'reason': 'invalid_eui', 'eui': str(entry.get('eui') or '')})
            continue
        try:
            gps_lat, gps_lng = _normalize_gps_coordinates(entry.get("gps_lat"), entry.get("gps_lng"))
        except ValueError as exc:
            skipped.append({'eui': eui.upper(), 'reason': str(exc)})
            continue
        existing = base_stations.get(eui, {})
        tenant_id = _resolve_write_tenant_id(
            entry.get("tenant_id"),
            existing_tenant=(existing or {}).get("tenant_id"),
        )
        row = {
            "name": str(entry.get("name") or (existing or {}).get("name") or "").strip(),
            "tags": list(entry.get("tags") or (existing or {}).get("tags") or []),
            "ip": str(entry.get("ip") or (existing or {}).get("ip") or "").strip(),
            "gps_lat": gps_lat,
            "gps_lng": gps_lng,
            "tenant_id": tenant_id,
        }
        base_stations[eui] = row
        _upsert_device_gps_position("bs", eui, gps_lat, gps_lng)
        if existing:
            updated += 1
        else:
            created += 1

    config["base_stations"] = base_stations
    save_base_station_config(config)

    _try_record_inventory_event(
        "base_station",
        "imported",
        "batch",
        {
            "created_count": created,
            "updated_count": updated,
            "error_count": len(skipped),
            "tenant_id": active_tenant,
        }
    )
    _record_admin_audit(
        action='base_station.import',
        entity='base_station',
        target_id='tenant',
        status='success',
        details={
            'tenant_id': active_tenant,
            'created': created,
            'updated': updated,
            'skipped': len(skipped),
            'storage_backend': storage_meta.get('backend', 'json'),
        },
    )
    return jsonify({
        'success': True,
        'created': created,
        'updated': updated,
        'skipped': skipped,
        'storage_backend': storage_meta.get('backend', 'json'),
        'seed_defaults_file': storage_meta.get('seed_defaults_file', BASE_STATIONS_RECOVERY_FILE),
        'recovery_file': storage_meta.get('recovery_file', BASE_STATIONS_RECOVERY_FILE),
    })


@app.route('/api/base-stations/certificates/status')
@login_required
@internal_portal_required
def get_bs_certificates_status():
    """Get read-only certificate status for tenant-visible base stations."""
    try:
        config = load_base_station_config()
        bs_config = _filter_base_stations_for_tenant(config.get("base_stations", {}), tenant_id=_active_tenant_id())
        result = []
        for eui_key, bs_data in bs_config.items():
            eui_lower = eui_key.lower()
            bs_cert_dir = os.path.join('certs', f'bs_{eui_lower}')
            cert_path = os.path.join(bs_cert_dir, f'{eui_lower}_cert.pem')
            cert_exists = os.path.exists(cert_path)
            cert_expires = bs_data.get("cert_expires", "")
            cert_generated = bs_data.get("cert_generated", "")
            status = "missing"
            if cert_exists and cert_expires:
                try:
                    exp_dt = datetime.strptime(cert_expires, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    if exp_dt < now:
                        status = "expired"
                    elif (exp_dt - now).days < 30:
                        status = "expiring_soon"
                    else:
                        status = "valid"
                except:
                    status = "valid" if cert_exists else "missing"
            elif cert_exists:
                status = "valid"
            result.append({
                "eui": eui_lower,
                "name": bs_data.get("name", ""),
                "tenant_id": bs_data.get("tenant_id", _active_tenant_id()),
                "cert_exists": cert_exists,
                "cert_status": status,
                "cert_generated": cert_generated,
                "cert_expires": cert_expires
            })
        return jsonify({"success": True, "certificates": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/uptime')
@login_required
@internal_portal_required
def get_bs_uptime():
    """Get uptime data for all base stations"""
    try:
        active_tenant = _active_tenant_id()
        allowed_bs = {
            str(eui).strip().upper()
            for eui in _filter_base_stations_for_tenant(
                load_base_station_config().get("base_stations", {}),
                tenant_id=active_tenant,
            ).keys()
        }
        requested_source = (request.args.get("source") or bssci_config.TELEMETRY_SOURCE or "auto").strip().lower()
        if requested_source not in {"auto", "runtime", "influx"}:
            requested_source = "auto"
        influx_enabled = bool(getattr(bssci_config, "INFLUX_ENABLED", True))
        if not influx_enabled and requested_source in {"auto", "influx"}:
            requested_source = "runtime"

        filtered_runtime_events = {}
        for eui, events in (bs_uptime_events or {}).items():
            eui_upper = str(eui or "").strip().upper()
            if eui_upper in allowed_bs:
                filtered_runtime_events[eui_upper] = events

        runtime_payload = {
            "success": True,
            "source": "runtime_events_memory",
            "requested_source": requested_source,
            "uptime_events": filtered_runtime_events
        }

        if requested_source == "runtime":
            return jsonify(runtime_payload)

        if influx_enabled and requested_source in {"auto", "influx"}:
            influx_result = _get_influx_uptime_events()
            if influx_result.get("success"):
                filtered_influx = {}
                for eui, events in (influx_result.get("uptime_events", {}) or {}).items():
                    if str(eui or "").strip().upper() in allowed_bs:
                        filtered_influx[str(eui or "").strip().upper()] = events
                return jsonify({
                    "success": True,
                    "source": influx_result.get("source", "influxdb"),
                    "requested_source": requested_source,
                    "uptime_events": filtered_influx
                })

            if requested_source == "influx":
                # Explicit influx requested - return runtime fallback with reason to keep UI alive.
                runtime_payload["fallback_reason"] = influx_result.get("error", "Influx query failed")
                runtime_payload["source"] = "runtime_events_memory_fallback"
                return jsonify(runtime_payload)

            # auto mode fallback
            runtime_payload["fallback_reason"] = influx_result.get("error", "Influx query failed")
            return jsonify(runtime_payload)

        return jsonify(runtime_payload)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/influx/sync-inventory', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def sync_inventory_to_influx():
    """Force one-time snapshot sync of sensors/base stations to InfluxDB."""
    try:
        if not bool(getattr(bssci_config, "INFLUX_ENABLED", True)):
            return jsonify({
                "success": False,
                "message": "InfluxDB integration is disabled."
            }), 404
        trigger = (request.args.get("trigger") or "manual").strip().lower()
        result = _sync_inventory_snapshot_to_influx(trigger=trigger)
        if result.get("success"):
            return jsonify({
                "success": True,
                "message": f'Snapshot written: {result.get("line_count", 0)} points',
                **result
            })
        return jsonify({
            "success": False,
            "message": f'Influx sync failed: {result.get("error", "unknown error")}',
            **result
        }), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/timescale/status', methods=['GET'])
@login_required
@permission_required('can_edit_config')
def timescale_status():
    """Check TimescaleDB connectivity and readiness."""
    worker_stats = get_timescale_uplink_runtime_stats()
    ok, err = _timescale_is_ready()
    if not ok:
        return jsonify({
            "success": False,
            "enabled": bool(getattr(bssci_config, "TIMESCALE_ENABLED", False)),
            "error": err,
            "telemetry_worker": worker_stats,
        }), 200
    conn, conn_err = _timescale_connect()
    if conn is None:
        return jsonify({
            "success": False,
            "enabled": True,
            "error": conn_err,
            "telemetry_worker": worker_stats,
        }), 200
    try:
        _ensure_timescale_schema(conn)
        if _timescale_telemetry_enabled():
            _ensure_timescale_uplink_worker_started()
        with conn.cursor() as cur:
            cur.execute("SELECT NOW()")
            now_value = cur.fetchone()[0]
        return jsonify({
            "success": True,
            "enabled": True,
            "message": "TimescaleDB reachable",
            "db_time": str(now_value),
            "host": getattr(bssci_config, "TIMESCALE_HOST", "timescaledb"),
            "database": getattr(bssci_config, "TIMESCALE_DB", "bssci"),
            "policies": {
                "retention_enabled": bool(getattr(bssci_config, "TIMESCALE_RETENTION_ENABLED", True)),
                "telemetry_retention_days": int(getattr(bssci_config, "TIMESCALE_TELEMETRY_RETENTION_DAYS", 90)),
                "inventory_retention_days": int(getattr(bssci_config, "TIMESCALE_INVENTORY_RETENTION_DAYS", 365)),
                "compression_enabled": bool(getattr(bssci_config, "TIMESCALE_COMPRESSION_ENABLED", True)),
                "compression_after_days": int(getattr(bssci_config, "TIMESCALE_COMPRESSION_AFTER_DAYS", 7)),
            },
            "telemetry_worker": get_timescale_uplink_runtime_stats(),
        })
    except Exception as exc:
        return jsonify({
            "success": False,
            "enabled": True,
            "error": str(exc),
            "telemetry_worker": get_timescale_uplink_runtime_stats(),
        }), 200
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.route('/api/timescale/sync-inventory', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def sync_inventory_to_timescale():
    """Force one-time snapshot sync of sensors/base stations to TimescaleDB."""
    try:
        trigger = (request.args.get("trigger") or "manual").strip().lower()
        result = _sync_inventory_snapshot_to_timescale(trigger=trigger)
        if result.get("success"):
            return jsonify({
                "success": True,
                "message": f'Snapshot written: {result.get("line_count", 0)} records',
                **result
            })
        return jsonify({
            "success": False,
            "message": f'Timescale sync failed: {result.get("error", "unknown error")}',
            **result
        }), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/timescale/policies/apply', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def apply_timescale_policies():
    """Apply retention/compression policy settings to TimescaleDB."""
    conn, err = _timescale_connect()
    if conn is None:
        return jsonify({"success": False, "error": err}), 500
    try:
        _ensure_timescale_schema(conn)
        with conn.cursor() as cur:
            _timescale_apply_policies(cur)
        return jsonify({
            "success": True,
            "message": "Timescale policies applied",
            "policies": {
                "retention_enabled": bool(getattr(bssci_config, "TIMESCALE_RETENTION_ENABLED", True)),
                "telemetry_retention_days": int(getattr(bssci_config, "TIMESCALE_TELEMETRY_RETENTION_DAYS", 90)),
                "inventory_retention_days": int(getattr(bssci_config, "TIMESCALE_INVENTORY_RETENTION_DAYS", 365)),
                "compression_enabled": bool(getattr(bssci_config, "TIMESCALE_COMPRESSION_ENABLED", True)),
                "compression_after_days": int(getattr(bssci_config, "TIMESCALE_COMPRESSION_AFTER_DAYS", 7)),
            },
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.route('/api/timescale/telemetry', methods=['GET'])
@login_required
def timescale_telemetry_summary():
    """Get aggregated telemetry uplink summary from TimescaleDB."""
    try:
        window_minutes = int(request.args.get("minutes", 60))
        bucket_seconds = int(request.args.get("bucket_seconds", 60))
        top_limit = int(request.args.get("top", 10))
        result = _timescale_fetch_telemetry_summary(
            window_minutes=window_minutes,
            bucket_seconds=bucket_seconds,
            top_limit=top_limit,
        )
        result["telemetry_worker"] = get_timescale_uplink_runtime_stats()
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/<eui>', methods=['GET'])
@login_required
@internal_portal_required
def get_base_station(eui):
    """Get single base station details"""
    try:
        config = load_base_station_config()
        bs_data = config.get("base_stations", {}).get(eui.lower(), {}) or {}
        if not bs_data:
            return jsonify({"success": False, "error": "Base station not found"}), 404
        if not _tenant_matches(_tenant_id_from_base_station(bs_data), _active_tenant_id()):
            return jsonify({"success": False, "error": "Základňová stanica sa nenašla v aktuálnom priestore."}), 404
        return jsonify({"eui": eui.lower(), **bs_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations', methods=['POST'])
@login_required
@admin_scope_required('manage_tenants')
def add_base_station():
    """Add new base station"""
    try:
        data = request.get_json() or {}
        eui = data.get("eui", "").lower()
        gps_lat, gps_lng = _normalize_gps_coordinates(data.get("gps_lat"), data.get("gps_lng"))
        
        if not eui or not _validate_eui(eui):
            return jsonify({"success": False, "error": "Invalid EUI (must be 16 hex characters)"}), 400
        
        config = load_base_station_config()
        if eui in config.get("base_stations", {}):
            return jsonify({"success": False, "error": "Base station already exists"}), 400
        tenant_id = _resolve_write_tenant_id(data.get("tenant_id"))
        
        config["base_stations"][eui] = {
            "name": data.get("name", ""),
            "tags": data.get("tags", []),
            "ip": data.get("ip", ""),
            "gps_lat": gps_lat,
            "gps_lng": gps_lng,
            "tenant_id": tenant_id,
        }
        save_base_station_config(config)

        _try_record_inventory_event(
            "base_station",
            "created",
            eui,
            {
                "name": data.get("name", ""),
                "ip": data.get("ip", ""),
                "tags": data.get("tags", []),
                "gps_lat": gps_lat,
                "gps_lng": gps_lng,
                "tenant_id": tenant_id,
            }
        )

        _upsert_device_gps_position("bs", eui, gps_lat, gps_lng)
        after_snapshot = _base_station_audit_snapshot(eui, config["base_stations"][eui])
        _record_admin_audit(
            action='base_station.create',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                "tenant_id": tenant_id,
                "generate_cert": bool(data.get("generate_cert", True)),
                "after": after_snapshot,
                "changed_fields": list(after_snapshot.keys()),
            },
        )
        
        generate_cert = data.get("generate_cert", True)
        cert_download_url = None
        if generate_cert:
            success, msg = _generate_bs_certificate(eui, audit_context='base_station_create')
            if success:
                cert_download_url = f"/api/base-stations/{eui}/certificate/download"
        
        result = {"success": True}
        if cert_download_url:
            result["cert_download_url"] = cert_download_url
        return jsonify(result)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/<eui>', methods=['PUT'])
@login_required
@admin_scope_required('manage_tenants')
def update_base_station(eui):
    """Update base station"""
    try:
        data = request.get_json() or {}
        eui = eui.lower()
        gps_lat, gps_lng = _normalize_gps_coordinates(data.get("gps_lat"), data.get("gps_lng"))
        
        config = load_base_station_config()
        if "base_stations" not in config:
            config["base_stations"] = {}
        if eui in config["base_stations"]:
            existing_tenant = _tenant_id_from_base_station(config["base_stations"].get(eui, {}))
            if not _tenant_matches(existing_tenant, _active_tenant_id()):
                return jsonify({"success": False, "error": "Základňová stanica sa nenašla v aktuálnom priestore."}), 404
        
        previous_data = dict(config["base_stations"].get(eui, {}))
        previous_snapshot = _base_station_audit_snapshot(eui, previous_data)
        config["base_stations"][eui] = {
            "name": data.get("name", ""),
            "tags": data.get("tags", []),
            "ip": data.get("ip", ""),
            "gps_lat": gps_lat,
            "gps_lng": gps_lng,
            "tenant_id": _resolve_write_tenant_id(
                data.get("tenant_id"),
                existing_tenant=previous_data.get("tenant_id"),
            ),
        }
        save_base_station_config(config)

        _try_record_inventory_event(
            "base_station",
            "updated",
            eui,
            {
                "name": data.get("name", ""),
                "ip": data.get("ip", ""),
                "tags": data.get("tags", []),
                "gps_lat": gps_lat,
                "gps_lng": gps_lng,
                "previous_name": previous_data.get("name", ""),
                "previous_ip": previous_data.get("ip", ""),
                "tenant_id": config["base_stations"][eui].get("tenant_id", _active_tenant_id()),
            }
        )

        _upsert_device_gps_position("bs", eui, gps_lat, gps_lng)
        after_snapshot = _base_station_audit_snapshot(eui, config["base_stations"][eui])
        _record_admin_audit(
            action='base_station.update',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                "tenant_id": config["base_stations"][eui].get("tenant_id", _active_tenant_id()),
                "before": previous_snapshot,
                "after": after_snapshot,
                "changed_fields": _audit_changed_fields(previous_snapshot, after_snapshot),
            },
        )
        
        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/base-stations/<eui>', methods=['DELETE'])
@login_required
@admin_scope_required('manage_tenants')
def delete_base_station(eui):
    """Delete base station from config"""
    try:
        config = load_base_station_config()
        eui = eui.lower()
        
        removed = config.get("base_stations", {}).get(eui, {})
        if removed and not _tenant_matches(_tenant_id_from_base_station(removed), _active_tenant_id()):
            return jsonify({"success": False, "error": "Základňová stanica sa nenašla v aktuálnom priestore."}), 404
        if eui in config.get("base_stations", {}):
            del config["base_stations"][eui]
            save_base_station_config(config)

        _try_record_inventory_event(
            "base_station",
            "deleted",
            eui,
            {
                "name": removed.get("name", ""),
                "ip": removed.get("ip", ""),
                "tags": removed.get("tags", []),
                "gps_lat": removed.get("gps_lat"),
                "gps_lng": removed.get("gps_lng"),
                "tenant_id": _tenant_id_from_base_station(removed),
            }
        )

        _remove_device_position("bs", eui)
        _record_admin_audit(
            action='base_station.delete',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                "tenant_id": _tenant_id_from_base_station(removed),
                "before": _base_station_audit_snapshot(eui, removed),
            },
        )
        
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

def _build_traffic_metrics_payload() -> Dict[str, Any]:
    """Build traffic metrics payload used by customer dashboard and internal traffic view."""
    global tls_server_instance
    timescale_summary = _timescale_fetch_telemetry_summary(window_minutes=60, bucket_seconds=60, top_limit=10)
    timescale_summary["telemetry_worker"] = get_timescale_uplink_runtime_stats()
    if tls_server_instance and hasattr(tls_server_instance, 'get_traffic_metrics'):
        data = tls_server_instance.get_traffic_metrics()
        return {'success': True, **data, 'timescale': timescale_summary}
    return {
        'success': True,
        'metrics': {
            'messages_in': 0,
            'messages_out': 0,
            'messages_dropped': 0,
            'bytes_in': 0,
            'bytes_out': 0,
            'vm_messages': 0,
            'attach_requests': 0,
            'detach_requests': 0,
            'status_requests': 0,
            'start_time': 0
        },
        'dedup_stats': {'total_messages': 0, 'duplicate_messages': 0, 'published_messages': 0},
        'history': [],
        'connections': 0,
        'timescale': timescale_summary
    }

@app.route('/api/traffic/metrics')
@login_required
@internal_portal_required
def get_traffic_metrics():
    """Get traffic metrics for visualization"""
    try:
        return jsonify(_build_traffic_metrics_payload())
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/traffic/reset', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def reset_traffic_metrics():
    """Reset traffic metrics"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'reset_traffic_metrics'):
            tls_server_instance.reset_traffic_metrics()
            return jsonify({'success': True, 'message': 'Traffic metrics reset successfully'})
        return jsonify({'success': False, 'message': 'TLS server not available'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/oms/meters')
@login_required
@internal_portal_required
def get_oms_meters():
    """Get all tracked OMS meters"""
    try:
        if not getattr(bssci_config, 'OMS_ENABLED', True):
            return jsonify({'success': False, 'message': 'OMS module is disabled'}), 404
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_oms_meters'):
            meters = tls_server_instance.get_oms_meters()
            return jsonify({'success': True, 'meters': list(meters.values())})
        return jsonify({'success': True, 'meters': []})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/oms/stats')
@login_required
@internal_portal_required
def get_oms_stats():
    """Get OMS statistics"""
    try:
        if not getattr(bssci_config, 'OMS_ENABLED', True):
            return jsonify({'success': False, 'message': 'OMS module is disabled'}), 404
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_oms_stats'):
            stats = tls_server_instance.get_oms_stats()
            return jsonify({'success': True, **stats})
        return jsonify({'success': True, 'total_meters': 0, 'total_messages': 0})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/logs')
@admin_scope_required('view_service_logs')
def get_logs():
    global log_entries
    ensure_web_log_handler()

    # Get query parameters for filtering
    level_filter = str(request.args.get('level', 'all') or 'all').strip().upper()
    logger_filter = str(request.args.get('logger', 'all') or 'all').strip()
    text_filter = str(request.args.get('q', '') or '').strip().lower()
    try:
        limit = int(request.args.get('limit', 100))
    except (TypeError, ValueError):
        limit = 100
    limit = max(10, min(limit, 1000))

    # Filter logs based on parameters
    filtered_logs = list(log_entries)

    if level_filter != 'ALL':
        filtered_logs = [log for log in filtered_logs if str(log.get('level', '')).upper() == level_filter]

    if logger_filter.lower() != 'all':
        needle = logger_filter.lower()
        filtered_logs = [log for log in filtered_logs if needle in str(log.get('logger', '')).lower()]

    if text_filter:
        def _log_matches_text(entry):
            message = str(entry.get('message', '')).lower()
            logger_name = str(entry.get('logger', '')).lower()
            level_name = str(entry.get('level', '')).lower()
            timestamp = str(entry.get('timestamp', '')).lower()
            source = str(entry.get('source', '')).lower()
            return (
                text_filter in message
                or text_filter in logger_name
                or text_filter in level_name
                or text_filter in timestamp
                or text_filter in source
            )
        filtered_logs = [log for log in filtered_logs if _log_matches_text(log)]

    # Return the most recent logs (up to limit)
    recent_logs = filtered_logs[-limit:] if len(filtered_logs) > limit else filtered_logs

    level_counts = {'ERROR': 0, 'WARNING': 0, 'INFO': 0, 'DEBUG': 0}
    for log in filtered_logs:
        level = str(log.get('level', '')).upper()
        if level in level_counts:
            level_counts[level] += 1
    logger_names = sorted({str(log.get('logger', '')).strip() for log in log_entries if str(log.get('logger', '')).strip()})

    return jsonify({
        'logs': recent_logs,
        'total_logs': len(log_entries),
        'filtered_logs': len(filtered_logs),
        'source': 'memory',
        'level_counts': level_counts,
        'logger_names': logger_names
    })


def _evaluate_monitor_level(value, warning_threshold, critical_threshold):
    number = float(value or 0.0)
    if number >= float(critical_threshold):
        return "critical"
    if number >= float(warning_threshold):
        return "warning"
    return "good"


def _build_mqtt_monitor_insights(runtime_status):
    mqtt_queue = dict((runtime_status or {}).get("queue", {}) or {})
    mqtt_stats = dict((runtime_status or {}).get("stats", {}) or {})
    timescale_stats = get_timescale_uplink_runtime_stats()

    def _safe_threshold(value, fallback):
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    queue_warn = max(0.0, _safe_threshold(getattr(bssci_config, "MONITOR_QUEUE_WARN_PCT", 65.0), 65.0))
    queue_crit = max(queue_warn, _safe_threshold(getattr(bssci_config, "MONITOR_QUEUE_CRIT_PCT", 85.0), 85.0))
    retry_warn = max(0.0, _safe_threshold(getattr(bssci_config, "MONITOR_RETRY_FAIL_WARN_PCT", 5.0), 5.0))
    retry_crit = max(retry_warn, _safe_threshold(getattr(bssci_config, "MONITOR_RETRY_FAIL_CRIT_PCT", 20.0), 20.0))
    latency_warn = max(0.0, _safe_threshold(getattr(bssci_config, "MONITOR_DB_WRITE_LATENCY_WARN_MS", 500.0), 500.0))
    latency_crit = max(latency_warn, _safe_threshold(getattr(bssci_config, "MONITOR_DB_WRITE_LATENCY_CRIT_MS", 1500.0), 1500.0))
    reconnect_warn = max(
        0.0,
        _safe_threshold(getattr(bssci_config, "MONITOR_MQTT_RECONNECT_WARN_PER_HOUR", 3.0), 3.0),
    )
    reconnect_crit = max(
        reconnect_warn,
        _safe_threshold(getattr(bssci_config, "MONITOR_MQTT_RECONNECT_CRIT_PER_HOUR", 8.0), 8.0),
    )

    in_util = float(mqtt_queue.get("in_utilization_pct") or 0.0)
    out_util = float(mqtt_queue.get("out_utilization_pct") or 0.0)
    db_util = float(timescale_stats.get("queue_utilization_pct") or 0.0)
    queue_depth_pct = max(in_util, out_util, db_util)
    queue_level = _evaluate_monitor_level(queue_depth_pct, warning_threshold=queue_warn, critical_threshold=queue_crit)

    mqtt_retry_exhausted = int(mqtt_stats.get("outgoing_retry_exhausted", 0) or 0)
    mqtt_retry_attempts = int(mqtt_stats.get("outgoing_retried", 0) or 0)
    mqtt_retry_fail_rate_pct = (
        (mqtt_retry_exhausted / mqtt_retry_attempts) * 100.0
        if mqtt_retry_attempts > 0 else 0.0
    )
    db_failed_batches = int(timescale_stats.get("failed_batches", 0) or 0)
    db_retried_batches = int(timescale_stats.get("retried_batches", 0) or 0)
    db_retry_denominator = db_failed_batches + db_retried_batches
    db_retry_fail_rate_pct = (
        (db_failed_batches / db_retry_denominator) * 100.0
        if db_retry_denominator > 0 else 0.0
    )
    retry_fail_rate_pct = max(mqtt_retry_fail_rate_pct, db_retry_fail_rate_pct)
    retry_level = _evaluate_monitor_level(retry_fail_rate_pct, warning_threshold=retry_warn, critical_threshold=retry_crit)

    db_latency_samples = int(timescale_stats.get("write_latency_samples", 0) or 0)
    db_write_latency_ms = float(timescale_stats.get("write_latency_last_ms", 0.0) or 0.0)
    if db_latency_samples <= 0:
        db_latency_level = "neutral"
        db_write_latency_display = None
    else:
        db_latency_level = _evaluate_monitor_level(
            db_write_latency_ms,
            warning_threshold=latency_warn,
            critical_threshold=latency_crit,
        )
        db_write_latency_display = round(db_write_latency_ms, 2)

    reconnects_last_hour = int((runtime_status or {}).get("reconnects_last_hour", 0) or 0)
    reconnect_total = int(mqtt_stats.get("reconnect_count", 0) or 0)
    reconnect_level = _evaluate_monitor_level(
        reconnects_last_hour,
        warning_threshold=reconnect_warn,
        critical_threshold=reconnect_crit,
    )

    alerts = []
    if queue_level in {"warning", "critical"}:
        alerts.append({
            "level": queue_level,
            "metric": "queue_depth",
            "text": f"Queue depth is high ({round(queue_depth_pct, 1)}%).",
        })
    if retry_level in {"warning", "critical"}:
        alerts.append({
            "level": retry_level,
            "metric": "retry_fail_rate",
            "text": f"Retry failure rate is elevated ({round(retry_fail_rate_pct, 2)}%).",
        })
    if db_latency_level in {"warning", "critical"}:
        alerts.append({
            "level": db_latency_level,
            "metric": "db_write_latency",
            "text": f"DB write latency is high ({round(db_write_latency_ms, 1)} ms).",
        })
    if reconnect_level in {"warning", "critical"}:
        alerts.append({
            "level": reconnect_level,
            "metric": "mqtt_reconnect_count",
            "text": f"MQTT reconnect frequency is high ({reconnects_last_hour}/h).",
        })
    if not alerts:
        alerts.append({
            "level": "good",
            "metric": "overall",
            "text": "No critical runtime signals detected.",
        })

    return {
        "metrics": {
            "queue_depth": {
                "value_pct": round(queue_depth_pct, 2),
                "level": queue_level,
                "mqtt_in_pct": round(in_util, 2),
                "mqtt_out_pct": round(out_util, 2),
                "db_queue_pct": round(db_util, 2),
            },
            "retry_fail_rate": {
                "value_pct": round(retry_fail_rate_pct, 2),
                "level": retry_level,
                "mqtt_retry_fail_rate_pct": round(mqtt_retry_fail_rate_pct, 2),
                "db_retry_fail_rate_pct": round(db_retry_fail_rate_pct, 2),
                "mqtt_retry_exhausted": mqtt_retry_exhausted,
                "db_failed_batches": db_failed_batches,
            },
            "db_write_latency": {
                "value_ms": db_write_latency_display,
                "level": db_latency_level,
                "last_ms": round(db_write_latency_ms, 2),
                "avg_ms": round(float(timescale_stats.get("write_latency_avg_ms", 0.0) or 0.0), 2),
                "max_ms": round(float(timescale_stats.get("write_latency_max_ms", 0.0) or 0.0), 2),
                "samples": db_latency_samples,
            },
            "mqtt_reconnect_count": {
                "value_per_hour": reconnects_last_hour,
                "level": reconnect_level,
                "total": reconnect_total,
            },
        },
        "alerts": alerts,
        "timescale": {
            "queue_size": int(timescale_stats.get("queue_size", 0) or 0),
            "queue_maxsize": int(timescale_stats.get("queue_maxsize", 0) or 0),
            "queue_utilization_pct": round(float(timescale_stats.get("queue_utilization_pct", 0.0) or 0.0), 2),
            "retry_attempts": int(timescale_stats.get("retry_attempts", 0) or 0),
            "failed_batches": db_failed_batches,
            "retried_batches": db_retried_batches,
            "last_error": str(timescale_stats.get("last_error", "") or ""),
            "last_write_ts": float(timescale_stats.get("last_write_ts", 0.0) or 0.0),
        },
        "thresholds": {
            "queue_warn_pct": round(queue_warn, 2),
            "queue_crit_pct": round(queue_crit, 2),
            "retry_warn_pct": round(retry_warn, 2),
            "retry_crit_pct": round(retry_crit, 2),
            "db_latency_warn_ms": round(latency_warn, 2),
            "db_latency_crit_ms": round(latency_crit, 2),
            "reconnect_warn_per_hour": round(reconnect_warn, 2),
            "reconnect_crit_per_hour": round(reconnect_crit, 2),
        },
    }

@app.route('/api/mqtt/monitor')
@login_required
@internal_portal_required
def get_mqtt_monitor():
    if not getattr(bssci_config, 'MQTT_UI_ENABLED', True):
        return jsonify({"success": False, "error": "MQTT module is disabled"}), 404
    try:
        global mqtt_client_instance
        if not mqtt_client_instance:
            runtime_status = {
                "enabled": bool(getattr(bssci_config, 'MQTT_ENABLED', True)),
                "connected": False,
                "message": "MQTT transport is disabled." if not getattr(bssci_config, 'MQTT_ENABLED', True) else "MQTT client is not initialized.",
                "stats": {},
                "queue": {"in_size": 0, "out_size": 0, "in_utilization_pct": 0.0, "out_utilization_pct": 0.0},
                "recent_incoming_topics": [],
                "recent_outgoing_topics": [],
                "recent_errors": [],
                "last_error": "",
                "connected_since": 0,
                "disconnected_since": 0,
                "connected_seconds": 0,
                "reconnects_last_hour": 0,
                "broker_host": "",
                "broker_port": int(getattr(bssci_config, "MQTT_PORT", 1883)),
                "base_topic": "",
                "username_set": bool(getattr(bssci_config, "MQTT_USERNAME", "")),
            }
            return jsonify({
                "success": True,
                "available": False,
                "status": runtime_status,
                "insights": _build_mqtt_monitor_insights(runtime_status),
            })

        if hasattr(mqtt_client_instance, "get_runtime_status"):
            runtime_status = mqtt_client_instance.get_runtime_status()
        else:
            runtime_status = {
                "enabled": bool(getattr(bssci_config, 'MQTT_ENABLED', True)),
                "connected": bool(getattr(mqtt_client_instance, "connected", False)),
                "stats": dict(getattr(mqtt_client_instance, "stats", {}) or {}),
                "queue": {"in_size": 0, "out_size": 0},
                "recent_incoming_topics": [],
                "recent_outgoing_topics": [],
                "recent_errors": [],
                "last_error": "",
                "connected_since": 0,
                "disconnected_since": 0,
                "connected_seconds": 0,
                "reconnects_last_hour": 0,
                "broker_host": str(getattr(mqtt_client_instance, "broker_host", "") or ""),
                "broker_port": int(getattr(bssci_config, "MQTT_PORT", 1883)),
                "base_topic": str(getattr(mqtt_client_instance, "base_topic", "") or ""),
                "username_set": bool(getattr(bssci_config, "MQTT_USERNAME", "")),
            }
        insights = _build_mqtt_monitor_insights(runtime_status)

        return jsonify({
            "success": True,
            "available": True,
            "status": runtime_status,
            "insights": insights,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/mqtt/publish-test', methods=['POST'])
@login_required
@permission_required('can_edit_config')
def mqtt_publish_test():
    if not getattr(bssci_config, 'MQTT_UI_ENABLED', True):
        return jsonify({"success": False, "error": "MQTT module is disabled"}), 404
    if not getattr(bssci_config, 'MQTT_ENABLED', True):
        return jsonify({"success": False, "error": "MQTT transport is disabled"}), 503
    try:
        global mqtt_client_instance
        if not mqtt_client_instance:
            return jsonify({"success": False, "error": "MQTT client is not initialized"}), 503

        connected = bool(getattr(mqtt_client_instance, "connected", False))
        if not connected:
            return jsonify({"success": False, "error": "MQTT is currently disconnected"}), 409

        out_queue = getattr(mqtt_client_instance, "mqtt_out_queue", None)
        if out_queue is None:
            return jsonify({"success": False, "error": "MQTT outgoing queue is unavailable"}), 503

        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({"success": False, "error": "Invalid JSON body"}), 400

        raw_topic = str(data.get("topic", "") or "").strip()
        if not raw_topic:
            return jsonify({"success": False, "error": "Topic is required"}), 400
        if "+" in raw_topic or "#" in raw_topic:
            return jsonify({"success": False, "error": "Wildcard topics are not allowed"}), 400

        base_topic = str(
            getattr(
                mqtt_client_instance,
                "base_topic",
                globals().get("BASE_TOPIC", "mioty") or "mioty",
            )
            or "mioty"
        ).rstrip("/")
        if raw_topic == base_topic:
            return jsonify({"success": False, "error": "Use a topic below the base topic"}), 400
        if raw_topic.startswith(base_topic + "/"):
            topic_suffix = raw_topic[len(base_topic) + 1 :].strip("/")
        else:
            topic_suffix = raw_topic.strip("/")
        if not topic_suffix:
            return jsonify({"success": False, "error": "Topic suffix is required"}), 400
        if len(topic_suffix) > 240:
            return jsonify({"success": False, "error": "Topic is too long"}), 400

        payload_mode = str(data.get("payload_mode", "text") or "text").strip().lower()
        payload_raw = data.get("payload", "")
        if payload_mode == "json":
            try:
                payload_value = json.loads(str(payload_raw or ""))
            except json.JSONDecodeError as exc:
                return jsonify({"success": False, "error": f"Invalid JSON payload: {exc.msg}"}), 400
        elif payload_mode == "text":
            payload_value = str(payload_raw or "")
        else:
            return jsonify({"success": False, "error": "payload_mode must be 'text' or 'json'"}), 400

        try:
            qos = int(data.get("qos", 0))
        except (TypeError, ValueError):
            qos = 0
        qos = 0 if qos < 0 else (2 if qos > 2 else qos)
        retain = bool(data.get("retain", False))

        msg = {
            "topic": topic_suffix,
            "payload": payload_value,
            "qos": qos,
            "retain": retain,
        }

        try:
            out_queue.put_nowait(msg)
        except asyncio.QueueFull:
            return jsonify({"success": False, "error": "MQTT publish queue is full"}), 503

        payload_size = 0
        try:
            payload_size = len(json.dumps(payload_value, ensure_ascii=True))
        except Exception:
            payload_size = len(str(payload_value))
        _record_admin_audit(
            action='mqtt.publish_test',
            entity='mqtt',
            target_id=f"{base_topic}/{topic_suffix}",
            status='success',
            details={
                "topic": f"{base_topic}/{topic_suffix}",
                "payload_mode": payload_mode,
                "payload_size": payload_size,
                "qos": qos,
                "retain": retain,
            },
        )

        return jsonify({
            "success": True,
            "message": "MQTT message queued for publish",
            "topic": f"{base_topic}/{topic_suffix}",
            "qos": qos,
            "retain": retain,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# =========================
# UPDATE MANAGEMENT SYSTEM
# =========================

def get_current_version():
    """Get current version from VERSION file or fallback methods"""
    try:
        # First: Try to read VERSION file (preferred method)
        if os.path.exists('VERSION'):
            try:
                with open('VERSION', 'r') as f:
                    version = f.read().strip()
                    if version:
                        return f"v{version}" if not version.startswith('v') else version
            except:
                pass
        
        # Fallback: Try Git commands
        try:
            lock_file = '.git/index.lock'
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
            
            result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                commit_hash = result.stdout.strip()
                
                tag_result = subprocess.run(['git', 'describe', '--tags', '--exact-match', 'HEAD'], 
                                          capture_output=True, text=True, timeout=10)
                if tag_result.returncode == 0:
                    return tag_result.stdout.strip()
                else:
                    return f"commit-{commit_hash}"
        except FileNotFoundError:
            pass
        except Exception as e:
            if "No such file or directory" not in str(e):
                print(f"Git command error: {e}")
        
        # Fallback 1: try to read .git/HEAD directly
        try:
            with open('.git/HEAD', 'r') as f:
                head_ref = f.read().strip()
                if head_ref.startswith('ref: refs/heads/'):
                    # Get branch name and try to read commit
                    branch = head_ref.split('/')[-1]
                    ref_path = f'.git/refs/heads/{branch}'
                    try:
                        with open(ref_path, 'r') as ref_file:
                            commit = ref_file.read().strip()[:7]
                            return f"local-{commit}"
                    except:
                        return f"branch-{branch}"
                else:
                    # Direct commit hash
                    return f"local-{head_ref[:7]}"
        except:
            pass
        
        # Fallback 2: Use file modification timestamps
        try:
            import time
            main_files = ['main.py', 'web_ui.py', 'TLSServer.py', 'mqtt_interface.py']
            latest_time = 0
            for file in main_files:
                if os.path.exists(file):
                    mtime = os.path.getmtime(file)
                    latest_time = max(latest_time, mtime)
            
            if latest_time > 0:
                date_str = time.strftime('%Y%m%d', time.localtime(latest_time))
                return f"local-{date_str}"
        except:
            pass
            
        return "local-version"
    except Exception as e:
        print(f"Error getting current version: {e}")
        return "version-unknown"

def get_remote_version():
    """Get latest remote version - checks releases first, then commits"""
    GITHUB_REPO = "plasmonized/containerized-mioty-Service-Center"
    
    try:
        import urllib.request
        import ssl
        
        ctx = ssl.create_default_context()
        
        # First: Try to get latest release/tag
        try:
            releases_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            req = urllib.request.Request(releases_url, headers={'User-Agent': 'BSSCI-Service-Center'})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                data = json.loads(response.read().decode())
                tag_name = data.get('tag_name', '')
                if tag_name:
                    return tag_name if tag_name.startswith('v') else f"v{tag_name}"
        except:
            pass
        
        # Fallback: Try to get latest tag
        try:
            tags_url = f"https://api.github.com/repos/{GITHUB_REPO}/tags"
            req = urllib.request.Request(tags_url, headers={'User-Agent': 'BSSCI-Service-Center'})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                tags = json.loads(response.read().decode())
                if tags and len(tags) > 0:
                    tag_name = tags[0].get('name', '')
                    if tag_name:
                        return tag_name if tag_name.startswith('v') else f"v{tag_name}"
        except:
            pass
        
        # Fallback: Get latest commit
        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/main"
        req = urllib.request.Request(api_url, headers={'User-Agent': 'BSSCI-Service-Center'})
        
        try:
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                data = json.loads(response.read().decode())
                commit_hash = data.get('sha', '')[:7]
                commit_date = data.get('commit', {}).get('committer', {}).get('date', '')[:10]
                return f"commit-{commit_hash} ({commit_date})"
        except Exception as api_error:
            logger.error(f"GitHub API error: {api_error}")
        
        # Fallback to Git commands if API fails
        try:
            lock_file = '.git/index.lock'
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
            
            subprocess.run(['git', 'fetch', '--tags', 'origin', 'main'], 
                         capture_output=True, text=True, timeout=30)
            
            result = subprocess.run(['git', 'rev-parse', '--short', 'origin/main'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return f"commit-{result.stdout.strip()}"
        except:
            pass
        
        return "remote-unavailable"
    except Exception as e:
        print(f"Error getting remote version: {e}")
        return "remote-check-unavailable"

def get_commit_log(limit=5):
    """Get recent commit log - works with or without Git"""
    try:
        # First try Git commands
        try:
            # Try to unlock git if needed
            lock_file = '.git/index.lock'
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except:
                    pass
            
            result = subprocess.run(['git', 'log', '--oneline', f'-{limit}'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                commits = []
                for line in result.stdout.strip().split('\n'):
                    if line:
                        parts = line.split(' ', 1)
                        commits.append({
                            'hash': parts[0],
                            'message': parts[1] if len(parts) > 1 else ''
                        })
                return commits
        except FileNotFoundError:
            # Git not installed
            pass
        except Exception as e:
            if "No such file or directory" in str(e):
                # Git not installed
                pass
            else:
                print(f"Git command error: {e}")
        
        # Fallback: Return info about the current installation
        return [
            {'hash': 'local', 'message': 'Local installation - Git not available'},
            {'hash': 'info', 'message': 'Install Git to see commit history'},
            {'hash': 'note', 'message': 'Version checking works without Git'}
        ]
    except Exception as e:
        print(f"Error getting commit log: {e}")
        return [{'hash': 'error', 'message': f'Unable to get history: {str(e)}'}]

def parse_version(version_str):
    """Parse version string to comparable tuple"""
    try:
        v = version_str.lstrip('v').split('-')[0]
        parts = v.replace('.', ' ').split()
        return tuple(int(p) for p in parts if p.isdigit())
    except:
        return (0,)

def check_for_updates():
    """Check if updates are available using GitHub API"""
    GITHUB_REPO = "plasmonized/containerized-mioty-Service-Center"
    
    try:
        current = get_current_version()
        remote = get_remote_version()
        
        updates_available = False
        status_message = None
        
        if remote in ['remote-unavailable', 'remote-check-unavailable']:
            updates_available = False
            status_message = 'Cannot connect to GitHub to check for updates'
        elif current.startswith('v') and remote.startswith('v'):
            # Both are version numbers - compare them
            current_ver = parse_version(current)
            remote_ver = parse_version(remote)
            if remote_ver > current_ver:
                updates_available = True
                status_message = f'Update available: {current} -> {remote}'
        elif "commit-" in current and "commit-" in remote:
            # Both are commit hashes
            current_hash = current.split("commit-")[1].split()[0][:7]
            remote_hash = remote.split("commit-")[1].split()[0][:7]
            if current_hash != remote_hash:
                updates_available = True
        elif current.startswith("local-") or current.startswith("v"):
            # Local installation or version, but remote is commit-based
            updates_available = True
            status_message = 'Update available'
        
        # Get recent commits via GitHub API
        recent_commits = []
        try:
            import urllib.request
            import ssl
            ctx = ssl.create_default_context()
            
            api_url = f"https://api.github.com/repos/{GITHUB_REPO}/commits?per_page=5"
            req = urllib.request.Request(api_url, headers={'User-Agent': 'BSSCI-Service-Center'})
            
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                commits_data = json.loads(response.read().decode())
                for commit in commits_data:
                    recent_commits.append({
                        'hash': commit['sha'][:7],
                        'message': commit['commit']['message'].split('\n')[0][:60]
                    })
                
                # If we got commits but remote was unavailable, use first commit as remote version
                if commits_data and remote in ['remote-unavailable', 'remote-check-unavailable']:
                    first_commit = commits_data[0]
                    remote_hash = first_commit['sha'][:7]
                    commit_date = first_commit['commit']['committer']['date'][:10]
                    remote = f"commit-{remote_hash} ({commit_date})"
                    updates_available = True
                    status_message = 'Update available'
        except Exception as e:
            logger.error(f"Error fetching commits: {e}")

        result = {
            'current_version': current,
            'remote_version': remote,
            'updates_available': updates_available,
            'recent_commits': recent_commits,
            'status': 'success'
        }
        
        if status_message:
            result['message'] = status_message
            
        return result
    except Exception as e:
        logger.error(f"Error checking for updates: {e}")
        return {'status': 'error', 'error': str(e)}

def create_backup():
    """Create backup before update"""
    try:
        # Use /tmp for Docker compatibility, fallback to current dir
        backup_base = '/tmp' if os.path.exists('/tmp') and os.access('/tmp', os.W_OK) else '.'
        backup_dir = os.path.join(backup_base, f"bssci_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Create backup directory
        os.makedirs(backup_dir, exist_ok=True)
        
        # Backup important files and recovery JSONs when present
        files_to_backup = ['.env', 'VERSION']
        for recovery_file in [SENSORS_RECOVERY_FILE, BASE_STATIONS_RECOVERY_FILE, USERS_RECOVERY_FILE, TENANT_REGISTRY_FILE]:
            if recovery_file and recovery_file not in files_to_backup:
                files_to_backup.append(recovery_file)
        dirs_to_backup = ['certs']
        
        for file in files_to_backup:
            if os.path.exists(file):
                shutil.copy2(file, backup_dir)
                
        for dir_name in dirs_to_backup:
            if os.path.exists(dir_name):
                try:
                    shutil.copytree(dir_name, os.path.join(backup_dir, dir_name))
                except Exception as e:
                    logger.warning(f"Could not backup {dir_name}: {e}")
        
        return {'success': True, 'backup_dir': backup_dir}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def perform_update():
    """Perform update by downloading from GitHub"""
    GITHUB_REPO = "plasmonized/containerized-mioty-Service-Center"
    
    try:
        # Create backup first
        backup_result = create_backup()
        if not backup_result['success']:
            return {'success': False, 'error': f"Backup failed: {backup_result['error']}"}
        
        # First try git pull if we have a git repo
        if os.path.exists('.git'):
            try:
                subprocess.run(['git', 'reset', '--hard', 'HEAD'], capture_output=True, timeout=30)
                result = subprocess.run(['git', 'pull', 'origin', 'main'], 
                                      capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    return {
                        'success': True, 
                        'message': 'Update completed successfully via git',
                        'backup_dir': backup_result['backup_dir'],
                        'git_output': result.stdout
                    }
            except Exception as e:
                logger.error(f"Git pull failed, trying ZIP download: {e}")
        
        # Fallback: Download ZIP from GitHub
        import urllib.request
        import ssl
        import zipfile
        import tempfile
        
        ctx = ssl.create_default_context()
        
        # Try main branch first, then master
        for branch in ['main', 'master']:
            zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{branch}.zip"
            
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_file:
                    tmp_path = tmp_file.name
                    req = urllib.request.Request(zip_url, headers={'User-Agent': 'BSSCI-Service-Center'})
                    
                    with urllib.request.urlopen(req, timeout=60, context=ctx) as response:
                        tmp_file.write(response.read())
                
                # Extract ZIP
                extract_dir = tempfile.mkdtemp()
                with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                extracted_folders = os.listdir(extract_dir)
                if extracted_folders:
                    source_dir = os.path.join(extract_dir, extracted_folders[0])
                    
                    files_to_update = ['web_ui.py', 'TLSServer.py', 'main.py', 'web_main.py', 
                                     'mqtt_interface.py', 'messages.py', 'requirements.txt', 'VERSION']
                    dirs_to_update = ['templates', 'static']
                    
                    updated_files = []
                    for filename in files_to_update:
                        src = os.path.join(source_dir, filename)
                        if os.path.exists(src):
                            shutil.copy2(src, filename)
                            updated_files.append(filename)
                    
                    for dirname in dirs_to_update:
                        src_dir = os.path.join(source_dir, dirname)
                        if os.path.exists(src_dir):
                            for item in os.listdir(src_dir):
                                shutil.copy2(os.path.join(src_dir, item), 
                                           os.path.join(dirname, item))
                                updated_files.append(f'{dirname}/{item}')
                
                os.unlink(tmp_path)
                shutil.rmtree(extract_dir, ignore_errors=True)
                
                return {
                    'success': True, 
                    'message': f'Update completed via GitHub ({branch} branch)',
                    'backup_dir': backup_result['backup_dir'],
                    'updated_files': updated_files
                }
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    logger.warning(f"Branch {branch} not found, trying next...")
                    continue
                raise
        
        return {'success': False, 'error': 'Could not download from GitHub (no valid branch found)'}
        
    except PermissionError as e:
        logger.error(f"Update failed - permission denied: {e}")
        return {
            'success': False, 
            'error': 'Permission denied - files are read-only. For Docker: rebuild container with "docker-compose up -d --build"'
        }
    except Exception as e:
        logger.error(f"Update failed: {e}")
        if 'Permission denied' in str(e):
            return {
                'success': False, 
                'error': 'Permission denied - files are read-only. For Docker: rebuild container with "docker-compose up -d --build"'
            }
        return {'success': False, 'error': str(e)}

@app.route('/api/system/version')
def api_get_version():
    """Get current and remote version info"""
    try:
        version_info = check_for_updates()
        # Use remote commits from check_for_updates() - don't override with local git
        return jsonify(version_info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/check-updates')
def api_check_updates():
    """Check for available updates"""
    try:
        return jsonify(check_for_updates())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/update', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def api_perform_update():
    """System update is disabled in customized deployments."""
    return jsonify({
        'success': False,
        'error': 'Auto-update is disabled for this customized deployment. Use Git workflow and container rebuild.'
    }), 403

@app.route('/api/payload-profiles', methods=['GET'])
@login_required
def api_get_payload_profiles():
    """Return all payload profiles (seeded defaults + user-defined)."""
    try:
        from TLSServer import load_custom_payload_profiles, ensure_default_payload_profiles
        ensure_default_payload_profiles()
        profiles = load_custom_payload_profiles()
        return jsonify({'success': True, 'profiles': profiles})
    except Exception as exc:
        return jsonify({'success': False, 'message': str(exc)}), 500


@app.route('/api/payload-profiles', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def api_save_payload_profile():
    """Create or replace a single custom payload profile."""
    try:
        from TLSServer import load_custom_payload_profiles, save_custom_payload_profiles
        data = request.get_json(force=True) or {}
        profile_id = str(data.get('id') or data.get('key') or '').strip()
        if not profile_id:
            return jsonify({'success': False, 'message': 'Profile id is required.'}), 400
        existing = load_custom_payload_profiles()
        # Replace existing entry with same id or append
        updated = [p for p in existing if str(p.get('id') or '') != profile_id]
        updated.append(data)
        saved = save_custom_payload_profiles(updated)
        saved_profile = next((p for p in saved if str(p.get('id') or '') == profile_id), None)
        return jsonify({'success': True, 'message': f'Profil "{profile_id}" bol uložený.', 'profile': saved_profile})
    except ValueError as exc:
        return jsonify({'success': False, 'message': str(exc)}), 400
    except Exception as exc:
        return jsonify({'success': False, 'message': str(exc)}), 500


@app.route('/api/payload-profiles/<profile_id>', methods=['DELETE'])
@login_required
@admin_scope_required('manage_system')
def api_delete_payload_profile(profile_id):
    """Delete a custom payload profile by id."""
    try:
        from TLSServer import load_custom_payload_profiles, save_custom_payload_profiles
        normalized_id = str(profile_id or '').strip().lower().replace('-', '_')
        existing = load_custom_payload_profiles()
        updated = [p for p in existing if str(p.get('id') or '') != normalized_id]
        if len(updated) == len(existing):
            return jsonify({'success': False, 'message': f'Profil "{normalized_id}" nebol nájdený.'}), 404
        save_custom_payload_profiles(updated)
        return jsonify({'success': True, 'message': f'Profil "{normalized_id}" bol odstránený.'})
    except Exception as exc:
        return jsonify({'success': False, 'message': str(exc)}), 500


@app.route('/api/system/restart', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def api_restart_system():
    """Restart the service after update"""
    try:
        # Schedule restart in a separate thread to allow response to be sent
        def restart_service():
            time.sleep(2)  # Give time for response to be sent
            os._exit(0)  # Force exit - service manager should restart
            
        restart_thread = threading.Thread(target=restart_service)
        restart_thread.daemon = True
        restart_thread.start()
        
        return jsonify({'success': True, 'message': 'Service restart initiated'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Global variable to store TLS server instance
# tls_server_instance = None # Already defined at the top

def set_tls_server(server):
    """Set the TLS server instance"""
    global tls_server_instance
    tls_server_instance = server
    _ensure_influx_snapshot_worker_started()
    if _timescale_telemetry_enabled():
        _ensure_timescale_uplink_worker_started()


def set_mqtt_client(client):
    """Set the MQTT client instance for runtime status reporting."""
    global mqtt_client_instance
    mqtt_client_instance = client

def get_bssci_service_status():
    """Get the status of the BSSCI service - thread-safe version"""
    try:
        global tls_server_instance, mqtt_client_instance
        tls_server = tls_server_instance
        
        if not tls_server:
            return {
                'running': False,
                'service_type': 'web_ui',
                'tls_server': {'active': False},
                'mqtt_broker': {'active': False},
                'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []},
                'total_sensors': 0,
                'registered_sensors': 0,
                'pending_requests': 0,
                'error': 'TLS server not available'
            }
            
        # Get base station status safely without asyncio operations
        bs_status = {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []}
        try:
            # Thread-safe access to base station collections
            connected_count = 0
            connecting_count = 0
            connected_stations = []
            connecting_stations = []
            active_tenant = _active_tenant_id()
            allowed_bs = {
                str(eui).strip().upper()
                for eui in _filter_base_stations_for_tenant(
                    load_base_station_config().get("base_stations", {}),
                    tenant_id=active_tenant,
                ).keys()
            }
            
            if hasattr(tls_server, 'connected_base_stations'):
                connected_dict = getattr(tls_server, 'connected_base_stations', {})
                for writer, bs_eui in list(connected_dict.items()):
                    bs_upper = str(bs_eui or "").strip().upper()
                    if bs_upper not in allowed_bs:
                        continue
                    connected_stations.append({
                        "eui": bs_upper,
                        "address": "connected",
                        "status": "connected"
                    })
                connected_count = len(connected_stations)
            
            if hasattr(tls_server, 'connecting_base_stations'):
                connecting_dict = getattr(tls_server, 'connecting_base_stations', {})
                for writer, bs_eui in list(connecting_dict.items()):
                    bs_upper = str(bs_eui or "").strip().upper()
                    if bs_upper not in allowed_bs:
                        continue
                    connecting_stations.append({
                        "eui": bs_upper,
                        "address": "connecting", 
                        "status": "connecting"
                    })
                connecting_count = len(connecting_stations)
                
            bs_status = {
                "connected": connected_stations,
                "connecting": connecting_stations,
                "total_connected": connected_count,
                "total_connecting": connecting_count
            }
        except Exception as e:
            print(f"Error getting base station status: {e}")
            
        # Get sensor count safely
        total_sensors = 0
        registered_sensors = 0
        try:
            # Count sensors from configured inventory instead of runtime status to avoid asyncio issues
            sensors = _filter_sensors_for_tenant(_load_all_sensors(), tenant_id=_active_tenant_id())
            total_sensors = len(sensors)
            # For now, assume all configured sensors could be registered
            registered_sensors = total_sensors
        except Exception as e:
            print(f"Error counting sensors: {e}")

        # Build response safely
        mqtt_enabled = bool(getattr(bssci_config, 'MQTT_ENABLED', True))
        mqtt_active = bool(mqtt_enabled and mqtt_client_instance and getattr(mqtt_client_instance, "connected", False))
        mqtt_stats = dict(getattr(mqtt_client_instance, "stats", {}) or {}) if mqtt_client_instance else {}
        runtime_status = None
        if mqtt_client_instance and hasattr(mqtt_client_instance, "get_runtime_status"):
            try:
                runtime_status = mqtt_client_instance.get_runtime_status()
            except Exception:
                runtime_status = None
        if not isinstance(runtime_status, dict):
            runtime_status = {
                "enabled": mqtt_enabled,
                "connected": mqtt_active,
                "stats": mqtt_stats,
                "queue": {"in_size": 0, "out_size": 0, "in_utilization_pct": 0.0, "out_utilization_pct": 0.0},
                "reconnects_last_hour": 0,
            }
        monitor_insights = _build_mqtt_monitor_insights(runtime_status)

        response = {
            'running': True,
            'service_type': 'web_ui',
            'base_stations': bs_status,
            'tls_server': {
                'active': True,
                'listening_port': getattr(bssci_config, 'LISTEN_PORT', 16018),
                'connected_base_stations': bs_status.get('total_connected', 0),
                'total_sensors': total_sensors,
                'registered_sensors': registered_sensors
            },
            'mqtt_broker': {
                'enabled': mqtt_enabled,
                'active': mqtt_active,
                'broker_host': getattr(bssci_config, 'MQTT_BROKER', 'localhost'),
                'broker_port': getattr(bssci_config, 'MQTT_PORT', 1883),
                'stats': mqtt_stats,
                'runtime': runtime_status,
                'monitor_insights': monitor_insights,
            },
            'total_sensors': total_sensors,
            'registered_sensors': registered_sensors,
            'pending_requests': 0  # Avoid accessing asyncio objects
        }
        
        return response
        
    except Exception as e:
        print(f"Error in get_bssci_service_status: {e}")
        import traceback
        traceback.print_exc()
        return {
            'running': False,
            'service_type': 'web_ui',
            'tls_server': {'active': False},
            'mqtt_broker': {'enabled': bool(getattr(bssci_config, 'MQTT_ENABLED', True)), 'active': False},
            'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []},
            'total_sensors': 0,
            'registered_sensors': 0,
            'pending_requests': 0,
            'error': f'Status error: {str(e)}'
        }

@app.route('/api/logs/clear', methods=['POST'])
@login_required
@admin_scope_required('clear_service_logs')
def clear_logs():
    global log_entries
    log_entries = []
    return jsonify({'success': True, 'message': 'Logs cleared successfully'})


@app.route('/api/audit/logs', methods=['GET'])
@login_required
@admin_scope_required('view_admin_audit')
def get_admin_audit_logs():
    action_filter = str(request.args.get('action', 'all') or 'all').strip().lower()
    entity_filter = str(request.args.get('entity', 'all') or 'all').strip().lower()
    actor_filter = str(request.args.get('actor', 'all') or 'all').strip().lower()
    status_filter = str(request.args.get('status', 'all') or 'all').strip().lower()
    target_filter = str(request.args.get('target_id', '') or '').strip().lower()
    text_filter = str(request.args.get('q', '') or '').strip().lower()
    try:
        limit = int(request.args.get('limit', 200))
    except (TypeError, ValueError):
        limit = 200
    limit = max(20, min(limit, 2000))

    if _db_first_config_enabled():
        summary, err = _fetch_admin_audit_summary_from_db(
            action_filter=action_filter,
            entity_filter=entity_filter,
            actor_filter=actor_filter,
            status_filter=status_filter,
            text_filter=text_filter,
            target_filter=target_filter,
            limit=limit,
        )
        if summary is not None:
            recent = summary.get('entries', [])
            total = int(summary.get('total', 0) or 0)
            filtered_total = int(summary.get('filtered_total', 0) or 0)
            actions = list(summary.get('actions', []))
            entities = list(summary.get('entities', []))
            actors = list(summary.get('actors', []))
            status_counts = dict(summary.get('status_counts', {'success': 0, 'warning': 0, 'error': 0}))
            source_label = 'db'
        else:
            logger.warning("Falling back to in-memory admin audit filtering: %s", err)
            filtered, source_entries = _filter_admin_audit_entries(
                action_filter=action_filter,
                entity_filter=entity_filter,
                actor_filter=actor_filter,
                status_filter=status_filter,
                text_filter=text_filter,
                target_filter=target_filter,
            )
            with _admin_audit_lock:
                total = len(admin_audit_entries)
            filtered_total = len(filtered)
            recent = filtered[-limit:] if filtered_total > limit else filtered
            recent = list(reversed(recent))
            actions = sorted({str(item.get('action', '')).strip() for item in source_entries if str(item.get('action', '')).strip()})
            entities = sorted({str(item.get('entity', '')).strip() for item in source_entries if str(item.get('entity', '')).strip()})
            actors = sorted({str(item.get('actor', '')).strip() for item in source_entries if str(item.get('actor', '')).strip()})
            status_counts = {'success': 0, 'warning': 0, 'error': 0}
            for item in filtered:
                normalized = str(item.get('status', 'success')).strip().lower()
                if normalized in status_counts:
                    status_counts[normalized] += 1
                elif normalized:
                    status_counts[normalized] = status_counts.get(normalized, 0) + 1
            source_label = 'memory+file'
    else:
        filtered, source_entries = _filter_admin_audit_entries(
            action_filter=action_filter,
            entity_filter=entity_filter,
            actor_filter=actor_filter,
            status_filter=status_filter,
            text_filter=text_filter,
            target_filter=target_filter,
        )
        with _admin_audit_lock:
            total = len(admin_audit_entries)
        filtered_total = len(filtered)
        recent = filtered[-limit:] if filtered_total > limit else filtered
        recent = list(reversed(recent))

        actions = sorted({str(item.get('action', '')).strip() for item in source_entries if str(item.get('action', '')).strip()})
        entities = sorted({str(item.get('entity', '')).strip() for item in source_entries if str(item.get('entity', '')).strip()})
        actors = sorted({str(item.get('actor', '')).strip() for item in source_entries if str(item.get('actor', '')).strip()})

        status_counts = {'success': 0, 'warning': 0, 'error': 0}
        for item in filtered:
            normalized = str(item.get('status', 'success')).strip().lower()
            if normalized in status_counts:
                status_counts[normalized] += 1
            elif normalized:
                status_counts[normalized] = status_counts.get(normalized, 0) + 1
        source_label = 'memory+file'

    return jsonify({
        'success': True,
        'entries': recent,
        'total': total,
        'filtered_total': filtered_total,
        'shown': len(recent),
        'actions': actions,
        'entities': entities,
        'actors': actors,
        'status_counts': status_counts,
        'source': source_label,
    })


def _filter_admin_audit_entries(
    action_filter='all',
    entity_filter='all',
    actor_filter='all',
    status_filter='all',
    text_filter='',
    target_filter='',
):
    with _admin_audit_lock:
        all_entries = list(admin_audit_entries)
    filtered = list(all_entries)

    if action_filter != 'all':
        filtered = [item for item in filtered if str(item.get('action', '')).strip().lower() == action_filter]
    if entity_filter != 'all':
        filtered = [item for item in filtered if str(item.get('entity', '')).strip().lower() == entity_filter]
    if actor_filter != 'all':
        filtered = [item for item in filtered if str(item.get('actor', '')).strip().lower() == actor_filter]
    if status_filter != 'all':
        filtered = [item for item in filtered if str(item.get('status', '')).strip().lower() == status_filter]
    if target_filter:
        filtered = [item for item in filtered if str(item.get('target_id', '')).strip().lower() == target_filter]
    if text_filter:
        text_filter = str(text_filter).strip().lower()

        def _matches_text(entry):
            blob = " ".join([
                str(entry.get('action', '')),
                str(entry.get('entity', '')),
                str(entry.get('target_id', '')),
                str(entry.get('actor', '')),
                str(entry.get('status', '')),
                str(entry.get('path', '')),
                json.dumps(entry.get('details', {}), ensure_ascii=True),
            ]).lower()
            return text_filter in blob

        filtered = [item for item in filtered if _matches_text(item)]
    return filtered, all_entries


@app.route('/api/audit/logs/export', methods=['GET'])
@login_required
@admin_scope_required('export_admin_audit')
def export_admin_audit_logs():
    action_filter = str(request.args.get('action', 'all') or 'all').strip().lower()
    entity_filter = str(request.args.get('entity', 'all') or 'all').strip().lower()
    actor_filter = str(request.args.get('actor', 'all') or 'all').strip().lower()
    status_filter = str(request.args.get('status', 'all') or 'all').strip().lower()
    target_filter = str(request.args.get('target_id', '') or '').strip().lower()
    text_filter = str(request.args.get('q', '') or '').strip().lower()
    export_format = str(request.args.get('format', 'json') or 'json').strip().lower()
    if export_format not in {'json', 'csv'}:
        export_format = 'json'

    try:
        requested_limit = int(request.args.get('limit', 5000))
    except (TypeError, ValueError):
        requested_limit = 5000
    max_rows = max(100, int(getattr(bssci_config, "ADMIN_AUDIT_EXPORT_MAX_ROWS", 50000) or 50000))
    limit = max(1, min(requested_limit, max_rows))

    if _db_first_config_enabled():
        exported_rows, err = _fetch_admin_audit_entries_from_db(
            action_filter=action_filter,
            entity_filter=entity_filter,
            actor_filter=actor_filter,
            status_filter=status_filter,
            text_filter=text_filter,
            target_filter=target_filter,
            limit=limit,
            newest_first=True,
        )
        if exported_rows is None:
            logger.warning("Falling back to in-memory admin audit export: %s", err)
            filtered, _ = _filter_admin_audit_entries(
                action_filter=action_filter,
                entity_filter=entity_filter,
                actor_filter=actor_filter,
                status_filter=status_filter,
                text_filter=text_filter,
                target_filter=target_filter,
            )
            filtered_total = len(filtered)
            exported_rows = filtered[-limit:] if filtered_total > limit else filtered
            exported_rows = list(reversed(exported_rows))
        else:
            filtered_total_payload, _ = _fetch_admin_audit_summary_from_db(
                action_filter=action_filter,
                entity_filter=entity_filter,
                actor_filter=actor_filter,
                status_filter=status_filter,
                text_filter=text_filter,
                target_filter=target_filter,
                limit=1,
            )
            filtered_total = int((filtered_total_payload or {}).get('filtered_total', len(exported_rows)) or 0)
    else:
        filtered, _ = _filter_admin_audit_entries(
            action_filter=action_filter,
            entity_filter=entity_filter,
            actor_filter=actor_filter,
            status_filter=status_filter,
            text_filter=text_filter,
            target_filter=target_filter,
        )
        filtered_total = len(filtered)
        exported_rows = filtered[-limit:] if filtered_total > limit else filtered
        exported_rows = list(reversed(exported_rows))

    _record_admin_audit(
        action='audit.export',
        entity='audit',
        target_id='admin_audit',
        status='success',
        details={
            'format': export_format,
            'limit': limit,
            'filtered_total': filtered_total,
            'exported_total': len(exported_rows),
            'filters': {
                'action': action_filter,
                'entity': entity_filter,
                'actor': actor_filter,
                'status': status_filter,
                'target_id': target_filter,
                'q': text_filter,
            },
        },
    )

    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if export_format == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'timestamp', 'action', 'entity', 'target_id', 'status',
            'actor', 'role', 'actor_tenant', 'active_tenant',
            'method', 'path', 'ip', 'details',
        ])
        for entry in exported_rows:
            writer.writerow([
                entry.get('timestamp', ''),
                entry.get('action', ''),
                entry.get('entity', ''),
                entry.get('target_id', ''),
                entry.get('status', ''),
                entry.get('actor', ''),
                entry.get('role', ''),
                entry.get('actor_tenant', ''),
                entry.get('active_tenant', ''),
                entry.get('method', ''),
                entry.get('path', ''),
                entry.get('ip', ''),
                json.dumps(entry.get('details', {}), ensure_ascii=True),
            ])
        content = output.getvalue()
        filename = f'admin-audit-{stamp}.csv'
        return Response(
            content,
            mimetype='text/csv;charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename={filename}'},
        )

    payload = {
        'exported_at': datetime.now(timezone.utc).isoformat(),
        'filters': {
            'action': action_filter,
            'entity': entity_filter,
            'actor': actor_filter,
            'status': status_filter,
            'target_id': target_filter,
            'q': text_filter,
        },
        'total_filtered': filtered_total,
        'exported': len(exported_rows),
        'entries': exported_rows,
    }
    filename = f'admin-audit-{stamp}.json'
    return Response(
        json.dumps(payload, indent=2, ensure_ascii=True),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


@app.route('/api/audit/logs/clear', methods=['POST'])
@login_required
@admin_scope_required('clear_admin_audit')
def clear_admin_audit_logs():
    global admin_audit_entries
    with _admin_audit_lock:
        admin_audit_entries = []
    if _db_first_config_enabled():
        ok, err = _clear_admin_audit_entries_in_db()
        if not ok:
            return jsonify({'success': False, 'message': f'Failed to clear admin audit log: {err}'}), 500
    else:
        try:
            os.makedirs(os.path.dirname(admin_audit_log_file), exist_ok=True)
            with open(admin_audit_log_file, "w", encoding="utf-8") as f:
                f.write("")
        except Exception as exc:
            return jsonify({'success': False, 'message': f'Failed to clear admin audit log: {exc}'}), 500
    return jsonify({'success': True, 'message': 'Admin audit log cleared successfully'})

@app.route('/api/bssci/status')
@app.route('/api/service/status')  # Support both endpoints for compatibility
@login_required
def bssci_status():
    try:
        status = get_bssci_service_status()
        return jsonify(status)
    except Exception as e:
        app.logger.error(f"Error in bssci_status endpoint: {e}")
        error_response = {
            'running': False,
            'error': f'Service status error: {str(e)}',
            'service_type': 'web_ui',
            'tls_server': {'active': False},
            'mqtt_broker': {'active': False},
            'base_stations': {'total_connected': 0, 'total_connecting': 0, 'connected': [], 'connecting': []},
            'total_sensors': 0,
            'registered_sensors': 0,
            'pending_requests': 0
        }
        return jsonify(error_response), 500

def _build_customer_dashboard_summary_payload() -> Dict[str, Any]:
    """Build the fast customer dashboard summary payload for first paint."""
    return {
        'success': True,
        'statusData': get_bssci_service_status(),
        'gatewaysData': _build_base_stations_runtime_payload(),
    }

def _build_customer_dashboard_runtime_payload() -> Dict[str, Any]:
    """Build slower dashboard widgets that can be hydrated after first paint."""
    return {
        'success': True,
        'vmData': _build_vm_status_payload(),
        'trafficData': _build_traffic_metrics_payload(),
        'healthData': _build_health_stats_payload(),
        'topologyData': _build_network_topology_payload(),
        'coverageState': _build_coverage_positions_read_payload(),
    }

def _build_customer_dashboard_payload() -> Dict[str, Any]:
    """Aggregate customer-safe dashboard data without exposing internal runtime API surface."""
    payload = _build_customer_dashboard_summary_payload()
    payload.update(_build_customer_dashboard_runtime_payload())
    return payload

@app.route('/api/customer/dashboard', methods=['GET'])
@login_required
def customer_dashboard_payload():
    """Customer portal dashboard payload composed from customer-safe helpers."""
    try:
        active_tenant = _active_tenant_id()
        cached_payload = _get_cached_customer_dashboard_payload(active_tenant)
        if cached_payload:
            return jsonify(cached_payload)
        payload = _build_customer_dashboard_payload()
        _store_cached_customer_dashboard_payload(active_tenant, payload)
        return jsonify(payload)
    except Exception as exc:
        logger.exception("Failed to build customer dashboard payload")
        return jsonify({'success': False, 'error': str(exc)}), 500

@app.route('/api/customer/dashboard/summary', methods=['GET'])
@login_required
def customer_dashboard_summary_payload():
    """Fast customer dashboard payload used for first paint."""
    try:
        active_tenant = _active_tenant_id()
        cached_payload = _get_cached_customer_dashboard_summary_payload(active_tenant)
        if cached_payload:
            return jsonify(cached_payload)
        payload = _build_customer_dashboard_summary_payload()
        _store_cached_customer_dashboard_summary_payload(active_tenant, payload)
        return jsonify(payload)
    except Exception as exc:
        logger.exception("Failed to build customer dashboard summary payload")
        return jsonify({'success': False, 'error': str(exc)}), 500

@app.route('/api/customer/dashboard/runtime', methods=['GET'])
@login_required
def customer_dashboard_runtime_payload():
    """Deferred customer dashboard widgets hydrated after first paint."""
    try:
        active_tenant = _active_tenant_id()
        cached_payload = _get_cached_customer_dashboard_runtime_payload(active_tenant)
        if cached_payload:
            return jsonify(cached_payload)
        payload = _build_customer_dashboard_runtime_payload()
        _store_cached_customer_dashboard_runtime_payload(active_tenant, payload)
        return jsonify(payload)
    except Exception as exc:
        logger.exception("Failed to build customer dashboard runtime payload")
        return jsonify({'success': False, 'error': str(exc)}), 500

@app.route('/api/customer/base-stations', methods=['GET'])
@login_required
def customer_base_stations_payload():
    """Customer-safe base station summary used by customer sensor attach flows."""
    try:
        return jsonify(_build_base_stations_runtime_payload())
    except Exception as exc:
        logger.exception("Failed to build customer base station payload")
        return jsonify({'success': False, 'error': str(exc)}), 500

@app.route('/api/base_stations')
@login_required
def api_base_stations():
    """Get base stations for coverage map (tenant-aware, normalized and deduplicated)."""
    try:
        global tls_server_instance

        active_tenant = _active_tenant_id()
        _sync_coverage_positions_to_inventory(tenant_id=active_tenant, only_missing=True)
        raw_bs_config = load_base_station_config().get("base_stations", {})
        bs_config = _filter_base_stations_for_tenant(raw_bs_config, tenant_id=active_tenant)

        # Normalize configured entries by EUI to prevent duplicate records caused by case/format drift.
        normalized_config = {}
        for raw_eui, raw_config in (bs_config or {}).items():
            eui_upper = _normalize_eui_upper(raw_eui)
            if not eui_upper:
                continue

            config = dict(raw_config) if isinstance(raw_config, dict) else {}
            existing = normalized_config.get(eui_upper)
            if existing is None:
                normalized_config[eui_upper] = config
                continue

            # Merge duplicate definitions and keep the richer one.
            merged = dict(existing)
            merged.update({k: v for k, v in config.items() if v not in (None, "", [], {})})

            existing_has_gps = existing.get("gps_lat") is not None and existing.get("gps_lng") is not None
            config_has_gps = config.get("gps_lat") is not None and config.get("gps_lng") is not None
            if config_has_gps and not existing_has_gps:
                merged["gps_lat"] = config.get("gps_lat")
                merged["gps_lng"] = config.get("gps_lng")

            if not merged.get("name"):
                merged["name"] = config.get("name") or existing.get("name") or eui_upper[:8]

            normalized_config[eui_upper] = merged

        # Build owner map from full config to avoid leaking connected stations from other tenants.
        eui_owner_tenant = {}
        for owner_eui, owner_cfg in (raw_bs_config or {}).items():
            owner_upper = _normalize_eui_upper(owner_eui)
            if not owner_upper:
                continue
            eui_owner_tenant[owner_upper] = _tenant_id_from_base_station(owner_cfg)

        connected_euis = set()
        if tls_server_instance and hasattr(tls_server_instance, 'connected_base_stations'):
            for _, bs_eui in (getattr(tls_server_instance, 'connected_base_stations', {}) or {}).items():
                bs_upper = _normalize_eui_upper(bs_eui)
                if bs_upper:
                    connected_euis.add(bs_upper)

        base_stations = []
        for eui_upper, config in normalized_config.items():
            bs_tenant = _normalize_tenant_id(
                (config or {}).get('tenant_id'),
                fallback=active_tenant,
            )
            base_stations.append({
                'eui': eui_upper,
                'EUI': eui_upper,
                'name': (config or {}).get('name', eui_upper[:8]),
                'connected': eui_upper in connected_euis,
                'gps_lat': (config or {}).get('gps_lat'),
                'gps_lng': (config or {}).get('gps_lng'),
                'tenant_id': bs_tenant,
            })

        # Include connected stations that are not configured in current tenant inventory.
        for eui_upper in sorted(connected_euis):
            if eui_upper in normalized_config:
                continue
            owner_tenant = eui_owner_tenant.get(eui_upper)
            if owner_tenant and not _tenant_matches(owner_tenant, active_tenant):
                continue
            base_stations.append({
                'eui': eui_upper,
                'EUI': eui_upper,
                'name': eui_upper[:8],
                'connected': True,
                'gps_lat': None,
                'gps_lng': None,
                'tenant_id': active_tenant,
            })

        base_stations.sort(key=lambda item: (not bool(item.get('connected')), item.get('name', ''), item.get('eui', '')))
        return jsonify({'base_stations': base_stations})
    except Exception as e:
        return jsonify({'base_stations': [], 'error': str(e)})

@app.route('/api/base_stations/status')
def get_base_stations_status():
    """Get status of connected base stations - thread-safe version (legacy endpoint)"""
    try:
        global tls_server_instance
        tls_server = tls_server_instance

        if not tls_server:
            return jsonify({
                "connected": [],
                "connecting": [],
                "total_connected": 0,
                "total_connecting": 0,
                "error": "TLS server not initialized"
            }), 503

        connected_stations = []
        connecting_stations = []
        
        try:
            if hasattr(tls_server, 'connected_base_stations'):
                connected_dict = getattr(tls_server, 'connected_base_stations', {})
                for writer, bs_eui in list(connected_dict.items()):
                    try:
                        connected_stations.append({
                            "eui": bs_eui,
                            "address": "connected",
                            "status": "connected"
                        })
                    except Exception as e:
                        print(f"Error processing connected station {bs_eui}: {e}")
            
            if hasattr(tls_server, 'connecting_base_stations'):
                connecting_dict = getattr(tls_server, 'connecting_base_stations', {})
                for writer, bs_eui in list(connecting_dict.items()):
                    try:
                        connecting_stations.append({
                            "eui": bs_eui,
                            "address": "connecting",
                            "status": "connecting"
                        })
                    except Exception as e:
                        print(f"Error processing connecting station {bs_eui}: {e}")
                        
        except Exception as e:
            print(f"Error accessing base station collections: {e}")

        return jsonify({
            "connected": connected_stations,
            "connecting": connecting_stations,
            "total_connected": len(connected_stations),
            "total_connecting": len(connecting_stations)
        })
            
    except Exception as e:
        print(f"Error in get_base_stations_status endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "connected": [],
            "connecting": [],
            "total_connected": 0,
            "total_connecting": 0,
            "error": f"Base stations error: {str(e)}"
        })

# ==================== Variable MAC (VM) Sub-Channel API ====================

def _build_vm_status_payload() -> Dict[str, Any]:
    """Build VM status payload used by dashboard and internal OMS tools."""
    global tls_server_instance
    if tls_server_instance and hasattr(tls_server_instance, 'get_vm_status'):
        status = tls_server_instance.get_vm_status()
        return {'success': True, **status}
    return {'success': False, 'message': 'TLS server not available', 'active_sensors': {}}

@app.route('/api/vm/status')
@login_required
def get_vm_status():
    """Get VM sub-channel status for all sensors"""
    try:
        return jsonify(_build_vm_status_payload())
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/activate', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_activate():
    """Activate VM sub-channel reception on VM-capable base stations only
    
    Per BSSCI VM specification, this sends vm.activate with macType parameter.
    Only sends to base stations that have been confirmed as VM-capable.
    """
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.get_json(silent=True) or {}
        mac_type = data.get('macType', 0)
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_activate(mac_type, only_vm_capable=True))
        finally:
            loop.close()
        
        if success:
            return jsonify({'success': True, 'message': f'VM activate sent to VM-capable base stations (macType={mac_type})'})
        else:
            return jsonify({'success': False, 'message': 'No VM-capable base stations found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/deactivate', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_deactivate():
    """Deactivate VM sub-channel reception on VM-capable base stations only
    
    Per BSSCI VM specification, this sends vm.deactivate with macType parameter.
    Only sends to base stations that have been confirmed as VM-capable.
    """
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.get_json(silent=True) or {}
        mac_type = data.get('macType', 0)
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_deactivate(mac_type, only_vm_capable=True))
        finally:
            loop.close()
        
        if success:
            return jsonify({'success': True, 'message': f'VM deactivate sent to VM-capable base stations (macType={mac_type})'})
        else:
            return jsonify({'success': False, 'message': 'No VM-capable base stations found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/status', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_query_status():
    """Query VM sub-channel status - returns list of activated macTypes
    
    Per BSSCI VM specification, this sends vm.status to VM-capable base stations.
    Use discover=true to query ALL base stations (for initial VM capability detection).
    """
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.get_json(silent=True) or {}
        discover = data.get('discover', False)  # If true, query ALL base stations
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_status(only_vm_capable=not discover))
        finally:
            loop.close()
        
        if success:
            if discover:
                return jsonify({'success': True, 'message': 'VM status query sent to ALL base stations (discovery mode)'})
            else:
                return jsonify({'success': True, 'message': 'VM status query sent to VM-capable base stations'})
        else:
            if discover:
                return jsonify({'success': False, 'message': 'No base stations connected'})
            else:
                return jsonify({'success': False, 'message': 'No VM-capable base stations found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/log')
@login_required
def get_vm_log():
    """Get VM operation log entries"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'vm_log'):
            return jsonify({
                'success': True,
                'log': tls_server_instance.vm_log[-50:]
            })
        return jsonify({'success': True, 'log': []})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/capable')
@login_required
def get_vm_capable_base_stations():
    """Get list of base stations that support VM (Variable MAC)"""
    try:
        global tls_server_instance
        if tls_server_instance and hasattr(tls_server_instance, 'get_vm_capable_base_stations'):
            vm_capable = tls_server_instance.get_vm_capable_base_stations()
            connected = list(tls_server_instance.connected_base_stations.values()) if hasattr(tls_server_instance, 'connected_base_stations') else []
            return jsonify({
                'success': True,
                'vm_capable': vm_capable,
                'total_connected': len(connected),
                'connected_base_stations': connected
            })
        return jsonify({
            'success': True,
            'vm_capable': [],
            'total_connected': 0,
            'connected_base_stations': []
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/vm/send/<eui>', methods=['POST'])
@login_required
@permission_required('can_edit_sensors')
def vm_send_data_to_sensor(eui):
    """Send data to sensor via VM sub-channel (downlink)"""
    try:
        global tls_server_instance
        if not tls_server_instance:
            return jsonify({'success': False, 'message': 'TLS server not available'}), 503
        
        data = request.json
        if not data or 'data' not in data:
            return jsonify({'success': False, 'message': 'Missing data field'}), 400
        
        payload = bytes.fromhex(data['data']) if isinstance(data['data'], str) else bytes(data['data'])
        port = data.get('port', 1)
        
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(tls_server_instance.vm_send_data(eui, payload, port))
        finally:
            loop.close()
        
        if success:
            return jsonify({'success': True, 'message': f'VM downlink data sent to sensor {eui}'})
        else:
            return jsonify({'success': False, 'message': 'Failed to send VM data - VM may not be active for this sensor'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/certificates/status')
@login_required
@permission_required('can_manage_certificates')
def get_certificate_status():
    """Get status of SSL certificates"""
    import os
    from datetime import datetime
    try:
        cert_files = {
            'ca': 'certs/ca_cert.pem',
            'service': 'certs/service_center_cert.pem',
            'key': 'certs/service_center_key.pem'
        }

        status = {'certificates': {}}

        for cert_type, file_path in cert_files.items():
            if os.path.exists(file_path):
                status['certificates'][cert_type] = True
                # Try to get certificate expiry date
                try:
                    if cert_type != 'key':  # Don't try to parse private key as certificate
                        import ssl
                        import socket
                        from cryptography import x509
                        from cryptography.hazmat.backends import default_backend

                        with open(file_path, 'rb') as f:
                            cert_data = f.read()
                            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
                            expiry = cert.not_valid_after
                            status['certificates'][f'{cert_type}_expires'] = expiry.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass  # If we can't read the certificate, just mark as present
            else:
                status['certificates'][cert_type] = False

        return jsonify({'success': True, 'certificates': status['certificates']})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/download/<filename>')
@login_required
@permission_required('can_manage_certificates')
def download_certificate(filename):
    """Download a certificate file"""
    import os
    from flask import send_file, abort

    # Security: only allow specific certificate files
    allowed_files = ['ca_cert.pem', 'service_center_cert.pem', 'service_center_key.pem']
    if filename not in allowed_files:
        abort(404)

    file_path = os.path.join('certs', filename)
    if not os.path.exists(file_path):
        abort(404)

    return send_file(file_path, as_attachment=True, download_name=filename)

@app.route('/api/certificates/upload/<cert_type>', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def upload_certificate(cert_type):
    """Upload a new certificate"""
    import os
    from werkzeug.utils import secure_filename

    if 'certificate' not in request.files:
        return jsonify({'success': False, 'message': 'No file provided'})

    file = request.files['certificate']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'})

    # Map cert types to filenames
    cert_mapping = {
        'ca': 'ca_cert.pem',
        'service': 'service_center_cert.pem',
        'key': 'service_center_key.pem'
    }

    if cert_type not in cert_mapping:
        return jsonify({'success': False, 'message': 'Invalid certificate type'})

    try:
        # Ensure certs directory exists
        os.makedirs('certs', exist_ok=True)

        # Backup existing file
        target_file = os.path.join('certs', cert_mapping[cert_type])
        if os.path.exists(target_file):
            backup_file = target_file + '.backup'
            os.rename(target_file, backup_file)

        # Save new file
        file.save(target_file)

        return jsonify({'success': True, 'message': f'{cert_type.upper()} certificate uploaded successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/generate', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def generate_certificates():
    """Generate new SSL certificates"""
    import os
    import subprocess

    try:
        # Ensure certs directory exists
        os.makedirs('certs', exist_ok=True)

        # Generate new certificates using OpenSSL with static, validated commands
        import shlex

        # Execute certificate generation commands with completely static strings

        # Generate CA private key
        result = subprocess.run(['openssl', 'genrsa', '-out', 'certs/ca_key.pem', '2048'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'CA key generation failed: {result.stderr}'})

        # Generate CA certificate
        result = subprocess.run(['openssl', 'req', '-new', '-x509', '-key', 'certs/ca_key.pem', '-out', 'certs/ca_cert.pem', '-days', '365', '-subj', '/C=US/ST=State/L=City/O=BSSCI/CN=BSSCI-CA'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'CA certificate generation failed: {result.stderr}'})

        # Generate service private key
        result = subprocess.run(['openssl', 'genrsa', '-out', 'certs/service_center_key.pem', '2048'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'Service key generation failed: {result.stderr}'})

        # Generate service certificate request
        result = subprocess.run(['openssl', 'req', '-new', '-key', 'certs/service_center_key.pem', '-out', 'certs/service_center.csr', '-subj', '/C=US/ST=State/L=City/O=BSSCI/CN=bssci-service'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'Service certificate request generation failed: {result.stderr}'})

        # Sign service certificate with CA
        result = subprocess.run(['openssl', 'x509', '-req', '-in', 'certs/service_center.csr', '-CA', 'certs/ca_cert.pem', '-CAkey', 'certs/ca_key.pem', '-CAcreateserial', '-out', 'certs/service_center_cert.pem', '-days', '365'],
                               capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({'success': False, 'message': f'Service certificate signing failed: {result.stderr}'})

        # Clean up temporary files
        temp_files = ['certs/service_center.csr', 'certs/ca_cert.srl']
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

        return jsonify({'success': True, 'message': 'New certificates generated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/backup')
@login_required
@permission_required('can_manage_certificates')
def backup_certificates():
    """Download all certificates as ZIP"""
    import os
    import tempfile
    import zipfile
    from flask import send_file

    try:
        # Create temporary ZIP file
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')

        with zipfile.ZipFile(temp_zip.name, 'w') as zipf:
            cert_files = ['ca_cert.pem', 'service_center_cert.pem', 'service_center_key.pem']
            for cert_file in cert_files:
                file_path = os.path.join('certs', cert_file)
                if os.path.exists(file_path):
                    zipf.write(file_path, cert_file)

        return send_file(temp_zip.name, as_attachment=True, download_name='bssci_certificates_backup.zip')
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/certificates/restore', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def restore_certificates():
    """Restore certificates from ZIP backup"""
    import os
    import tempfile
    import zipfile

    if 'backup' not in request.files:
        return jsonify({'success': False, 'message': 'No backup file provided'})

    file = request.files['backup']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'})

    try:
        # Save uploaded ZIP to temporary location
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        file.save(temp_zip.name)

        # Extract certificates
        with zipfile.ZipFile(temp_zip.name, 'r') as zipf:
            # Ensure certs directory exists
            os.makedirs('certs', exist_ok=True)

            # Extract only certificate files
            cert_files = ['ca_cert.pem', 'service_center_cert.pem', 'service_center_key.pem']
            for cert_file in cert_files:
                if cert_file in zipf.namelist():
                    target_path = os.path.join('certs', cert_file)
                    # Backup existing file
                    if os.path.exists(target_path):
                        os.rename(target_path, target_path + '.backup')
                    # Extract new file
                    zipf.extract(cert_file, 'certs')

        # Clean up temporary file
        os.unlink(temp_zip.name)

        return jsonify({'success': True, 'message': 'Certificates restored successfully from backup'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/base-stations/<eui>/certificate/generate', methods=['POST'])
@login_required
@permission_required('can_manage_certificates')
def generate_bs_certificate(eui):
    """Generate certificate pair for specific base station"""
    try:
        eui = eui.lower()
        if not _validate_eui(eui):
            _record_admin_audit(
                action='base_station.generate_certificate',
                entity='base_station',
                target_id=eui,
                status='error',
                details={'message': 'Invalid EUI format', 'audit_context': 'api_route'},
            )
            return jsonify({'success': False, 'message': 'Invalid EUI format'}), 400
        config = load_base_station_config()
        if eui not in config.get("base_stations", {}):
            _record_admin_audit(
                action='base_station.generate_certificate',
                entity='base_station',
                target_id=eui,
                status='error',
                details={'message': 'Base station not found', 'audit_context': 'api_route'},
            )
            return jsonify({'success': False, 'message': 'Base station not found'}), 404
        success, msg = _generate_bs_certificate(eui, audit_context='api_route')
        if success:
            return jsonify({'success': True, 'message': msg, 'download_url': f'/api/base-stations/{eui}/certificate/download'})
        else:
            return jsonify({'success': False, 'message': msg}), 500
    except Exception as e:
        _record_admin_audit(
            action='base_station.generate_certificate',
            entity='base_station',
            target_id=str(eui or '').lower(),
            status='error',
            details={'message': str(e), 'audit_context': 'api_route'},
        )
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/base-stations/<eui>/certificate/download')
@login_required
@permission_required('can_manage_certificates')
def download_bs_certificate(eui):
    """Download ZIP with CA cert + BS cert + BS key"""
    try:
        eui = eui.lower()
        if not _validate_eui(eui):
            _record_admin_audit(
                action='base_station.download_certificate',
                entity='base_station',
                target_id=eui,
                status='error',
                details={'message': 'Invalid EUI format'},
            )
            return jsonify({'success': False, 'message': 'Invalid EUI format'}), 400
        config = load_base_station_config()
        bs_data = dict(config.get("base_stations", {}).get(eui, {}) or {})
        if not bs_data:
            _record_admin_audit(
                action='base_station.download_certificate',
                entity='base_station',
                target_id=eui,
                status='error',
                details={'message': 'Base station not found'},
            )
            return jsonify({'success': False, 'message': 'Base station not found'}), 404
        bs_cert_dir = os.path.join('certs', f'bs_{eui}')
        cert_path = os.path.join(bs_cert_dir, f'{eui}_cert.pem')
        key_path = os.path.join(bs_cert_dir, f'{eui}_key.pem')
        ca_path = 'certs/ca_cert.pem'
        if not os.path.exists(cert_path) or not os.path.exists(key_path):
            _record_admin_audit(
                action='base_station.download_certificate',
                entity='base_station',
                target_id=eui,
                status='error',
                details={
                    'tenant_id': _tenant_id_from_base_station(bs_data),
                    'message': 'Certificate not found for this base station',
                },
            )
            return jsonify({'success': False, 'message': 'Certificate not found for this base station'}), 404
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        with zipfile.ZipFile(temp_zip.name, 'w') as zipf:
            if os.path.exists(ca_path):
                zipf.write(ca_path, 'ca_cert.pem')
            zipf.write(cert_path, f'{eui}_cert.pem')
            zipf.write(key_path, f'{eui}_key.pem')
        _record_admin_audit(
            action='base_station.download_certificate',
            entity='base_station',
            target_id=eui,
            status='success',
            details={
                'tenant_id': _tenant_id_from_base_station(bs_data),
                'cert_generated': bs_data.get('cert_generated'),
                'cert_expires': bs_data.get('cert_expires'),
            },
        )
        return send_file(temp_zip.name, as_attachment=True, download_name=f'bs_{eui}_certificates.zip')
    except Exception as e:
        _record_admin_audit(
            action='base_station.download_certificate',
            entity='base_station',
            target_id=str(eui or '').lower(),
            status='error',
            details={'message': str(e)},
        )
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/container/restart', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def restart_container():
    """Force restart the entire container"""
    import subprocess
    import threading
    import time
    import os

    def container_restart_in_background():
        """Perform container restart in a separate thread"""
        try:
            time.sleep(1)  # Small delay to allow response to be sent
            
            logger.info("Forcing container restart")
            try:
                # Send SIGTERM to PID 1 (init process) to restart the container
                subprocess.run(['kill', '-TERM', '1'], check=False, timeout=5)
            except Exception as e:
                logger.error(f"Container restart failed: {e}")
                # Fallback: exit the main process which should cause container restart
                os._exit(0)
                
        except Exception as e:
            logger.error(f"Error during container restart: {e}")
            os._exit(1)

    try:
        # Start restart in background thread
        restart_thread = threading.Thread(target=container_restart_in_background)
        restart_thread.daemon = True
        restart_thread.start()
        
        return jsonify({'success': True, 'message': 'Container restart initiated. The container will restart completely to reload all environment variables.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/service/restart', methods=['POST'])
@login_required
@admin_scope_required('manage_system')
def restart_service():
    """Restart the BSSCI service with full environment reload"""
    import subprocess
    import threading
    import time
    import os

    def restart_in_background():
        """Perform the restart operation in a separate thread"""
        try:
            time.sleep(1)  # Small delay to allow response to be sent

            # Check if we're running in Docker
            is_docker = os.path.exists('/.dockerenv') or os.getenv('CONTAINER') == '1'
            
            # Check environment type
            is_replit = os.getenv('REPLIT_ENVIRONMENT') or os.getenv('REPL_SLUG')
            
            if is_replit:
                # In Replit, workflows auto-restart when the process exits
                logger.info("Replit environment detected - restarting via process exit")
                _restart_processes()
            elif is_docker:
                # In Docker (including Synology), use process exit with Docker restart policy
                # This avoids using kill/pkill commands that may not be available
                logger.info("Docker environment detected - restarting via process exit")
                logger.info("Docker restart policy will automatically restart the container")
                _restart_processes()
            else:
                # In regular environment without Docker
                logger.info("Regular environment detected - attempting process restart")
                _restart_processes()
                
        except Exception as e:
            logger.error(f"Error during restart: {e}")
            # Fallback to basic process restart
            _restart_processes()

    def _restart_processes():
        """Restart Python processes (Docker-compatible version without kill/pkill)"""
        try:
            logger.info("Initiating service restart for Docker environment...")
            
            # Give time for the response to be sent before restarting
            time.sleep(2)
            
            # In Docker with restart policy, we can simply exit and let Docker restart us
            # This works for Synology Docker and other containerized environments
            logger.info("Exiting process - Docker will restart automatically")
            
            # Use os._exit to bypass cleanup handlers and exit immediately
            import os
            os._exit(0)
            
        except Exception as e:
            logger.error(f"Error during process exit: {e}")
            # Fallback: try standard exit
            try:
                import sys
                sys.exit(0)
            except:
                # Last resort: force exit
                import os
                os._exit(1)

    try:
        # Start restart in background thread
        restart_thread = threading.Thread(target=restart_in_background)
        restart_thread.daemon = True
        restart_thread.start()

        return jsonify({'success': True, 'message': 'Service restart initiated. In Docker environments, the entire container will restart to reload environment variables.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

