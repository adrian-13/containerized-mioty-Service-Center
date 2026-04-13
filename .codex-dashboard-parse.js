function ensureMiniRuntimeInitialized() {
    if (miniRuntimeInitialized) return;
    bindMiniViewControls();
    initMiniCoverageMap();
    initMiniNetwork();
    miniRuntimeInitialized = true;

    setMiniTopologyView('map');
    refreshMiniViewportAfterLayoutChange();
}

function initDashboardContextMenus() {

function applyDashboardPayload(payload, meta = {}) {
    const data = payload || {};
    const statusData = data.statusData || null;
    const vmData = data.vmData || null;
    const trafficData = data.trafficData || null;
    const healthData = data.healthData || null;
    const topologyData = data.topologyData || null;
    const gatewaysData = data.gatewaysData || null;
    const coverageState = data.coverageState || null;
    const sensorsData = data.sensorsData || null;
    const baseStationsData = data.baseStationsData || null;
    const incidentsData = data.incidentsData || null;

    if (topologyData) latestTopologyData = topologyData;
    if (coverageState) latestCoverageState = coverageState;
    if (sensorsData) latestSensorInventoryData = sensorsData;
    if (baseStationsData) latestBaseStationInventoryData = baseStationsData;
    if (statusData) latestStatusData = statusData;
    if (vmData) latestVmData = vmData;
    if (trafficData) latestTrafficData = trafficData;
    if (healthData) latestHealthData = healthData;
    if (gatewaysData) latestGatewaysData = gatewaysData;
    if (incidentsData) latestIncidentsData = incidentsData;

    if (latestStatusData) updateStatusCards(latestStatusData);
    if (miniRuntimeInitialized && (latestTopologyData || latestCoverageState || latestSensorInventoryData || latestBaseStationInventoryData)) {
        if (miniTopologyView === 'graph') {
            updateTopologyPanel(latestTopologyData || {});
        } else {
            updateMiniCoverageMap(
                latestTopologyData || {},
                latestCoverageState || {},
                latestSensorInventoryData || {},
                latestBaseStationInventoryData || {}
            );
        }
    }
    const vmCapable = updateVmMetrics(latestVmData || {});
    const trafficInfo = updateTrafficMetrics(latestTrafficData || {});
    updateSensorLifecycleMetrics(latestSensorInventoryData || {});
    updateSensorOverviewMetrics(latestSensorInventoryData || {});
    const healthInfo = updateHealthMetrics(latestHealthData || {});
    updateOperationsPanel(trafficInfo, healthInfo, vmCapable);
    updateGatewayActions(latestGatewaysData || {});
    updateSensorRuntimeList(latestSensorInventoryData || {});
    renderIncidents(latestIncidentsData && Array.isArray(latestIncidentsData.incidents) ? latestIncidentsData.incidents : []);
    previousSnapshot.packetLoss = healthInfo.packetLoss || 0;
    updateLastUpdatedLabel(meta && Number.isFinite(meta.ts) ? meta.ts : undefined);
    updateDashboardCacheIndicator(meta);

function buildMiniMapRenderSignature(nodeEntries) {
    if (!Array.isArray(nodeEntries)) return '[]';
    const parts = nodeEntries.map((entry) => {
        const node = entry && entry.node ? entry.node : {};
        const pos = entry && entry.pos ? entry.pos : null;
        const type = node && node.type === 'base_station' ? 'base_station' : 'sensor';
        const eui = String((node && node.eui) || '').trim().toUpperCase();
        const label = String((node && node.label) || '').trim();
        const markerColor = String((node && node.marker_color) || '').trim();
        const lat = pos && Number.isFinite(Number(pos.lat)) ? Number(pos.lat).toFixed(6) : '';
        const lng = pos && Number.isFinite(Number(pos.lng)) ? Number(pos.lng).toFixed(6) : '';
        return [type, eui, label, markerColor, lat, lng].join('|');
    });
    parts.sort();
    return JSON.stringify(parts);
}

function buildMiniGraphRenderSignature(data) {
    const nodes = Array.isArray(data && data.nodes)
        ? data.nodes.map((node) => [
            String((node && node.id) || ''),
            String((node && node.type) || ''),
            String((node && node.eui) || '').trim().toUpperCase(),
            String((node && node.label) || ''),
            node && node.connected ? '1' : '0'
        ].join('|'))
        : [];
    const edges = Array.isArray(data && data.edges)
        ? data.edges.map((edge) => [
            String((edge && edge.id) || ''),
            String((edge && edge.source) || ''),
            String((edge && edge.target) || ''),
            edge && edge.primary ? '1' : '0'
        ].join('|'))
        : [];
    nodes.sort();
    edges.sort();
    return JSON.stringify({ nodes, edges });
}

function updateMiniCoverageMap(topologyData, coverageState, sensorInventoryData, baseStationInventoryData) {
    if (!miniOsmMap || !miniCoverageLayer) return;

    const positions = coverageState && coverageState.positions && typeof coverageState.positions === 'object'
        ? coverageState.positions
        : {};
    const nodes = buildMiniMapNodes(topologyData, sensorInventoryData, baseStationInventoryData);
    const nodeEntries = nodes.map((node) => ({
        node,
        pos: resolveMiniMarkerPosition(positions, node)
    }));
    const connectedBs = latestStatusData && latestStatusData.base_stations
        ? toNumber(latestStatusData.base_stations.total_connected)
        : 0;
    const nextSignature = `${buildMiniMapRenderSignature(nodeEntries)}|bs:${connectedBs}`;
    if (nextSignature === miniMapRenderSignature) return;
    miniMapRenderSignature = nextSignature;

    miniCoverageLayer.clearLayers();
    miniCoverageMarkers = {};

    let baseTotal = 0;
    let sensorTotal = 0;
    let baseShown = 0;
    let sensorShown = 0;

    const markerCoords = [];
    nodeEntries.forEach(({ node, pos }) => {
        const isBs = node.type === 'base_station';
        if (isBs) {
            baseTotal += 1;
        } else {
            sensorTotal += 1;
        }
        if (!pos) return;

        if (isBs) {
            baseShown += 1;
        } else {
            sensorShown += 1;
        }

        const markerStyle = getMiniMarkerBaseStyle(node);
        const markerKey = miniMarkerKey(node.type, node.eui || '');
        const popupId = markerKey.replace(/[^a-z0-9_-]+/gi, '-');
        const label = node.label || shortId(node.eui || '', 10);
        const markerEui = String(node.eui || '').trim().toUpperCase();
        const shortEui = markerEui.replace(/(.{4})(?=.)/g, '$1 ');
        const detailUrl = isBs
            ? '/base-stations/' + encodeURIComponent(markerEui)
            : '/sensors/' + encodeURIComponent(markerEui);
        const unlocked = !!miniMarkerUnlocked[markerKey];
        const marker = L.marker([pos.lat, pos.lng], {
            icon: makeMiniMarkerIcon(markerStyle.fillColor, unlocked),
            draggable: unlocked,
            autoPan: true,
            bubblingMouseEvents: false,
            riseOnHover: true,
            zIndexOffset: isBs ? 1300 : 1200
        });
        marker.__miniBaseStyle = { ...markerStyle };
        marker.__miniKey = markerKey;
        marker.__miniNode = {
            type: String(node.type || ''),
            eui: markerEui,
            label,
            marker_color: String(node.marker_color || '').trim()
        };
        marker.__miniRowSelector = isBs
            ? `.gateway-item[data-gateway-eui="${escapeHtml(markerEui)}"]`
            : `.sensor-item[data-sensor-id="${escapeHtml(markerEui)}"]`;

        marker.bindPopup(
            unlocked
                ? buildMiniMarkerUnlockedPopupHtml(label, shortEui, popupId, !!miniPendingLatLng[markerKey])
                : buildMiniMarkerPopupHtml(label, shortEui, detailUrl, popupId),
            { maxWidth: 240, className: 'sc-leaflet-popup' }
        );

        marker.bindTooltip(
            isBs
                ? appTranslate('dashboard.base_station_tooltip', `Base Station: ${label}`, { label })
                : appTranslate('dashboard.sensor_tooltip', `Sensor: ${label}`, { label }),
            {
                direction: 'top',
                offset: [0, -6]
            }
        );
        const resolveMarkerVisualStyle = () => getMiniMarkerBaseStyle({
            ...node,
            marker_color: marker.__miniNode ? String(marker.__miniNode.marker_color || '').trim() : ''
        });
        const refreshMarkerVisual = (isUnlocked = false) => {
            const nextStyle = resolveMarkerVisualStyle();
            marker.__miniBaseStyle = { ...nextStyle };
            marker.setIcon(makeMiniMarkerIcon(nextStyle.fillColor, isUnlocked));
        };
        const lockMarker = () => {
            miniMarkerUnlocked[markerKey] = false;
            marker.dragging?.disable?.();
            refreshMarkerVisual(false);
            marker.setPopupContent(buildMiniMarkerPopupHtml(label, shortEui, detailUrl, popupId));
        };

        const unlockMarker = () => {
            miniMarkerUnlocked[markerKey] = true;
            marker.dragging?.enable?.();
            refreshMarkerVisual(true);
            if (!miniPendingLatLng[markerKey]) {
                const current = marker.getLatLng();
                miniPendingLatLng[markerKey] = { _origLat: current.lat, _origLng: current.lng, lat: current.lat, lng: current.lng };
            }
            marker.setPopupContent(buildMiniMarkerUnlockedPopupHtml(label, shortEui, popupId, false));
            marker.openPopup();
            showMiniMapToast('PresĂşvanie odomknutĂ© â€” presuĹte marker', 2800);
        };
        const markerContextItems = (clientX, clientY) => {
            const detailLabel = isBs
                ? appTranslate('dashboard.open_base_station_detail', 'OtvoriĹĄ detail stanice')
                : appTranslate('dashboard.open_sensor_detail', 'OtvoriĹĄ detail senzora');
            const isDragOn = !!miniMarkerUnlocked[markerKey];
            const items = [
                {
                    icon: 'fa-arrow-up-right-from-square',
                    primary: true,
                    label: detailLabel,
                    action: () => {
                        window.location.href = detailUrl;
                    }
                },
                {
                    icon: isDragOn ? 'fa-lock' : 'fa-lock-open',
                    label: isDragOn
                        ? appTranslate('dashboard.lock_marker_position', 'ZamknĂşĹĄ polohu')
                        : appTranslate('dashboard.unlock_marker_move', 'OdomknĂşĹĄ presĂşvanie'),
                    action: () => {
                        if (miniMarkerUnlocked[markerKey]) {
                            delete miniPendingLatLng[markerKey];
                            lockMarker();
                        } else {
                            unlockMarker();
                        }
                    }
                }
            ];
            if (!isBs) {
                items.splice(1, 0, {
                    icon: 'fa-palette',
                    label: appTranslate('dashboard.change_marker_color', 'ZmeniĹĄ farbu'),
                    action: () => {
                        const currentColor = marker.__miniNode ? marker.__miniNode.marker_color : '';
                        window.setTimeout(() => {
                            showMiniColorPop(clientX, clientY, currentColor || null, (color) => {
                                fetch('/api/sensors/' + encodeURIComponent(markerEui) + '/marker-color', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ color: color || '' })
                                })
                                    .then((response) => response.json().then((data) => ({ ok: response.ok, data })).catch(() => ({ ok: response.ok, data: null })))
                                    .catch(() => ({ ok: false, data: { error: appTranslate('runtime.network_error', 'Chyba siete.') } }))
                                    .then((result) => {
                                        if (result.ok && result.data && result.data.success) {
                                            if (marker.__miniNode) {
                                                marker.__miniNode.marker_color = color ? String(color) : '';
                                            }
                                            updateMiniSensorMarkerColor(markerEui, color);
                                            refreshMarkerVisual(!!miniMarkerUnlocked[markerKey]);
                                            setDashboardMiniInteractionState({});
                                            showMiniMapToast(
                                                color
                                                    ? appTranslate('dashboard.marker_color_saved', 'Farba markera uloĹľenĂˇ')
                                                    : appTranslate('dashboard.marker_color_reset_done', 'PredvolenĂˇ farba obnovenĂˇ'),
                                                2200
                                            );
                                        } else {
                                            showMiniMapToast(
                                                'âś• ' + (((result.data && (result.data.message || result.data.error)) || appTranslate('dashboard.marker_color_failed', 'Nepodarilo sa uloĹľiĹĄ farbu'))),
                                                2600
                                            );
                                        }
                                    });
                            });
                        }, 80);
                    }
                });
            }
            return items;
        };
        window.__scMiniPopupActions[popupId] = {
            dots: () => {
                const dotsBtn = document.getElementById('sc-popup-dots-' + popupId);
                if (!dotsBtn) return;
                const rect = dotsBtn.getBoundingClientRect();
                showMiniCtx(
                    {
                        clientX: rect.left,
                        clientY: rect.bottom + 4,
                        preventDefault() {},
                        stopPropagation() {}
                    },
                    markerContextItems(rect.left, rect.bottom + 8)
                );
            },
            save: () => {
                const pending = miniPendingLatLng[markerKey];
                if (!pending) return;
                marker.closePopup();
                persistMiniMarkerPosition(marker.__miniNode, pending.lat, pending.lng)
                    .then((result) => {
                        if (result.ok && result.data && result.data.success) {
                            updateMiniCoveragePosition(marker.__miniNode, pending.lat, pending.lng);
                            if (!isBs && latestSensorInventoryData && typeof latestSensorInventoryData === 'object') {
                                const sensor = latestSensorInventoryData[markerEui] || latestSensorInventoryData[markerEui.toLowerCase()];
                                if (sensor && typeof sensor === 'object') {
                                    sensor.gps_lat = pending.lat;
                                    sensor.gps_lng = pending.lng;
                                }
                            }
                            showMiniMapToast('âś“ Poloha uloĹľenĂˇ', 2400);
                        } else {
                            marker.setLatLng([pending._origLat, pending._origLng]);
                            showMiniMapToast('âś• ' + (((result.data && (result.data.message || result.data.error)) || 'Chyba uloĹľenia')), 2400);
                        }
                        delete miniPendingLatLng[markerKey];
                        lockMarker();
                        setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
                        marker.openPopup();
                    })
                    .catch(() => {
                        marker.setLatLng([pending._origLat, pending._origLng]);
                        delete miniPendingLatLng[markerKey];
                        lockMarker();
                        setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
                        marker.openPopup();
                        showMiniMapToast('âś• Chyba uloĹľenia', 2400);
                    });
            },
            cancel: () => {
                const pending = miniPendingLatLng[markerKey];
                if (pending) marker.setLatLng([pending._origLat, pending._origLng]);
                delete miniPendingLatLng[markerKey];
                lockMarker();
                setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
                marker.openPopup();
                showMiniMapToast('Presunutie zruĹˇenĂ©', 1800);
            }
        };

        marker.on('mouseover', () => {
            setDashboardMiniInteractionState({ hoverKey: marker.__miniKey || '' });
        });
        marker.on('mouseout', () => {
            setDashboardMiniInteractionState({ hoverKey: '' });
        });
        marker.on('click', () => {
            const key = marker.__miniKey || '';
            if (!key) return;
            setDashboardMiniInteractionState({ activeKey: key, hoverKey: '' });
            marker.closeTooltip?.();
            marker.openPopup?.();
            const row = marker.__miniRowSelector ? document.querySelector(marker.__miniRowSelector) : null;
            row?.scrollIntoView?.({ behavior: 'smooth', block: 'nearest' });
        });
        marker.off('contextmenu');
        marker.on('popupopen', () => {
            setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
            marker.closeTooltip?.();
            const popupEl = document.querySelector('.leaflet-popup');
            if (popupEl) {
                popupEl.addEventListener('contextmenu', (popupEvent) => {
                    popupEvent.preventDefault();
                    popupEvent.stopPropagation();
                });
            }

            const dotsBtn = document.getElementById('sc-popup-dots-' + popupId);
            if (dotsBtn) {
                dotsBtn.addEventListener('click', (clickEvent) => {
                    clickEvent.stopPropagation();
                    const rect = dotsBtn.getBoundingClientRect();
                    const fakeEvent = {
                        clientX: rect.left,
                        clientY: rect.bottom + 4,
                        preventDefault() {},
                        stopPropagation() {}
                    };
                    showMiniCtx(fakeEvent, markerContextItems(rect.left, rect.bottom + 8));
                });
            }

            const saveBtn = document.getElementById('sc-popup-save-' + popupId);
            const cancelBtn = document.getElementById('sc-popup-cancel-' + popupId);

            if (saveBtn) {
                saveBtn.addEventListener('click', () => {
                    const pending = miniPendingLatLng[markerKey];
                    if (!pending) return;
                    marker.closePopup();
                    persistMiniMarkerPosition(marker.__miniNode, pending.lat, pending.lng).then((result) => {
                        if (result.ok && result.data && result.data.success) {
                            updateMiniCoveragePosition(marker.__miniNode, pending.lat, pending.lng);
                            if (!isBs && latestSensorInventoryData && typeof latestSensorInventoryData === 'object') {
                                const sensor = latestSensorInventoryData[markerEui] || latestSensorInventoryData[markerEui.toLowerCase()];
                                if (sensor && typeof sensor === 'object') {
                                    sensor.gps_lat = pending.lat;
                                    sensor.gps_lng = pending.lng;
                                }
                            }
                            showMiniMapToast('âś“ Poloha uloĹľenĂˇ', 2400);
                        } else {
                            marker.setLatLng([pending._origLat, pending._origLng]);
                            showMiniMapToast('âś• ' + (((result.data && (result.data.message || result.data.error)) || 'Chyba uloĹľenia')), 2400);
                        }
                        delete miniPendingLatLng[markerKey];
                        lockMarker();
                        setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
                        marker.openPopup();
                    });
                });
            }

            if (cancelBtn) {
                cancelBtn.addEventListener('click', () => {
                    const pending = miniPendingLatLng[markerKey];
                    if (pending) marker.setLatLng([pending._origLat, pending._origLng]);
                    delete miniPendingLatLng[markerKey];
                    lockMarker();
                    setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
                    marker.openPopup();
                    showMiniMapToast('Presunutie zruĹˇenĂ©', 1800);
                });
            }
        });
        marker.on('popupclose', () => {
            marker.closeTooltip?.();
        });
        marker.on('dragstart', () => {
            marker.closePopup();
            setDashboardMiniInteractionState({ activeKey: markerKey, hoverKey: '' });
            if (!miniPendingLatLng[markerKey]) {
                const current = marker.getLatLng();
                miniPendingLatLng[markerKey] = { _origLat: current.lat, _origLng: current.lng, lat: current.lat, lng: current.lng };
            }
            const pin = marker.getElement()?.querySelector?.('.sc-map-pin');
            if (pin) pin.classList.add('is-dragging');
        });
        marker.on('dragend', (dragEvent) => {
            const latLng = dragEvent.target.getLatLng();
            if (miniPendingLatLng[markerKey]) {
                miniPendingLatLng[markerKey].lat = latLng.lat;
                miniPendingLatLng[markerKey].lng = latLng.lng;
            }
            const pin = marker.getElement()?.querySelector?.('.sc-map-pin');
            if (pin) pin.classList.remove('is-dragging');
            marker.setPopupContent(buildMiniMarkerUnlockedPopupHtml(label, shortEui, popupId, true));
            marker.openPopup();
        });
        marker.on('contextmenu', (event) => {
            if (event && event.originalEvent) {
                L.DomEvent.preventDefault(event.originalEvent);
                L.DomEvent.stop(event.originalEvent);
            }
            const originalEvent = event.originalEvent || {};
            showMiniCtx(originalEvent, markerContextItems(Number(originalEvent.clientX || 0), Number(originalEvent.clientY || 0)));
        });
        marker.addTo(miniCoverageLayer);
        if (marker.__miniKey) {
            miniCoverageMarkers[marker.__miniKey] = marker;
        }
        markerCoords.push([pos.lat, pos.lng]);
    });

    const savedView = parseMiniCoverageOsmView(coverageState);
    if (!miniCoverageViewInitialized) {
        if (savedView) {
            miniOsmMap.setView(savedView.center, savedView.zoom);
            miniCoverageViewInitialized = true;
        } else if (markerCoords.length > 0) {
            const initialBounds = L.latLngBounds(markerCoords);
            miniOsmMap.fitBounds(initialBounds, { padding: [28, 28], maxZoom: 17 });
            miniCoverageViewInitialized = true;
        } else {
            miniOsmMap.setView(MINI_DEFAULT_CENTER, MINI_DEFAULT_ZOOM);
        }
    }

    setDashboardMiniInteractionState({});
    updateMiniMapSummary({
        baseShown,
        baseTotal,
        sensorShown,
        sensorTotal,
        demoActive: getAdminDashboardIncludeDemoData()
    });

    if (markerCoords.length > 0) {
        setMiniEmptyState('', '', false);
    } else {
            if (savedView && !miniCoverageViewInitialized) {
                miniOsmMap.setView(savedView.center, savedView.zoom);
                miniCoverageViewInitialized = true;
            }
        if (connectedBs === 0) {
            setMiniEmptyState(
                appTranslate('dashboard.connect_base_station_first', 'Connect a base station first'),
                appTranslate('dashboard.connect_base_station_map_note', 'Live topology remains empty until at least one connected base station is online.'),
                true
            );
        } else {
            setMiniEmptyState(
                appTranslate('dashboard.no_positioned_devices_yet', 'No positioned devices yet'),
                appTranslate('dashboard.add_gps_map_note', 'Add GPS in Base Stations or Sensors and markers will appear on the map.'),
                true
            );
        }
    }

    updateMiniMapPositionLabel();
    setDashboardMiniInteractionState({});
}

function updateTopologyPanel(data) {
    if (!data || !data.success) return;

    const totalConnectionsEl = document.getElementById('total-connections');
    if (totalConnectionsEl) totalConnectionsEl.textContent = (data.edges || []).length;
    if (!cyMini) return;

    const connectedBs = latestStatusData && latestStatusData.base_stations
        ? toNumber(latestStatusData.base_stations.total_connected)
        : 0;
    const nextGraphSignature = `${buildMiniGraphRenderSignature(data)}|bs:${connectedBs}`;
    if (nextGraphSignature === miniGraphRenderSignature) return;
    miniGraphRenderSignature = nextGraphSignature;

    const preserveUserView = miniGraphInitialized && miniGraphUserViewLocked;
    const prevZoom = preserveUserView ? cyMini.zoom() : null;
    const prevPan = preserveUserView ? { ...cyMini.pan() } : null;

    cyMini.elements().remove();

    const elements = [];
    (data.nodes || []).forEach(node => elements.push({ data: node }));
    (data.edges || []).forEach(edge => elements.push({ data: edge }));

    if (elements.length > 0) {
        cyMini.add(elements);
        cyMini.layout({
            name: 'cose',
            idealEdgeLength: 80,
            nodeOverlap: 20,
            refresh: 20,
            fit: !preserveUserView,
            padding: 20,
            randomize: false,
            componentSpacing: 60,
            nodeRepulsion: 5000,
            edgeElasticity: 50,
            gravity: 50,
            numIter: 500,
            animate: false
        }).run();

        if (preserveUserView && prevZoom !== null && prevPan) {
            cyMini.zoom(prevZoom);
            cyMini.pan(prevPan);
        }
    }

    miniGraphInitialized = true;
