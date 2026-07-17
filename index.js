let map;
let all_properties = typeof window_properties !== 'undefined' ? window_properties : [];
let markers = [];
let infoWindow;

// ── Favourites (persisted in localStorage) ───────────────────────────────────
const FAV_KEY = 'property_favourites';
let favourites = new Set(JSON.parse(localStorage.getItem(FAV_KEY) || '[]'));

// Filled in after Google Maps loads so toggleFav can rebuild pins
let PinElementClass = null;
const markerRegistry = {}; // pid → { marker, beds, recent, scale }

function isFav(pid) { return favourites.has(String(pid)); }

function _pinColors(pid, recent) {
    if (isFav(pid))  return { bg: '#f9a825', border: '#f57f17' };
    if (recent)      return { bg: '#e65100', border: '#bf360c' };
    return           { bg: 'green',   border: '#2e7d32' };
}

function toggleFav(pid, event) {
    if (event) event.stopPropagation();
    pid = String(pid);
    if (favourites.has(pid)) {
        favourites.delete(pid);
    } else {
        favourites.add(pid);
    }
    localStorage.setItem(FAV_KEY, JSON.stringify([...favourites]));
    const faved = favourites.has(pid);

    // Update heart buttons and card highlight
    document.querySelectorAll(`.fav-btn[data-pid="${pid}"]`).forEach(btn => {
        btn.textContent = faved ? '❤️' : '🤍';
        btn.title = faved ? 'Remove favourite' : 'Add favourite';
        btn.classList.toggle('is-fav', faved);
    });
    document.querySelectorAll(`.list-item[data-pid="${pid}"]`).forEach(el => {
        el.classList.toggle('list-item--fav', faved);
    });

    // Rebuild map marker pin with updated colour
    const reg = markerRegistry[pid];
    if (reg && PinElementClass) {
        const { bg, border } = _pinColors(pid, reg.recent);
        reg.marker.content = new PinElementClass({
            glyphText: reg.beds,
            glyphColor: 'white',
            background: bg,
            borderColor: border,
            scale: reg.scale,
        });
    }
}

function fmt(n) {
    if (n == null) return '—';
    return '£' + Math.round(n).toLocaleString('en-GB');
}

function isRecent(dateStr) {
    if (!dateStr) return false;
    const d = new Date(dateStr);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const recentDaysAgo = new Date(today);
    recentDaysAgo.setDate(today.getDate() - 7);
    return d >= recentDaysAgo;
}

function deltaHtml(delta) {
    if (delta == null) return '';
    const sign = delta >= 0 ? '+' : '−';
    const abs = Math.abs(delta);
    const color = delta <= 0 ? '#2e7d32' : '#c62828';  // green = listed below estimate
    return `<span style="color:${color};font-weight:600;">${sign}£${abs.toLocaleString('en-GB')}</span>`;
}

async function initMap() {
    //@ts-ignore
    const { Map, InfoWindow } = await google.maps.importLibrary("maps");
    const { AdvancedMarkerElement, PinElement } = await google.maps.importLibrary("marker");
    PinElementClass = PinElement;

    if (all_properties.length === 0) {
        console.error("Failed to load properties");
        document.getElementById("map").innerHTML = "<div style='padding: 20px;'><h3 style='color:red;'>Failed to load properties_data.js</h3></div>";
        return;
    }

    map = new Map(document.getElementById("map"), {
        zoom: 11,
        center: { lat: 51.41188, lng: -0.29607 },
        zoomControl: true,
        cameraControl: false,
        mapId: "DEMO_MAP_ID",
    });

    infoWindow = new InfoWindow();

    // Tiffin School
    new AdvancedMarkerElement({
        map,
        position: { lat: 51.41188, lng: -0.29607 },
        title: "Tiffin School (KT2 6RL)",
        content: new PinElement({ glyphText: "S", background: "blue", glyphColor: "white" }),
        zIndex: 1000,
    });

    // Office
    new AdvancedMarkerElement({
        map,
        position: { lat: 51.52281, lng: -0.09015 },
        title: "Office",
        content: new PinElement({ glyphText: "O", background: "black", glyphColor: "white" }),
        zIndex: 1000,
    });

    // Read URL params for filtering
    const urlParams = new URLSearchParams(window.location.search);
    const maxSchool = parseInt(urlParams.get('school_max')) || 1500;
    const maxOffice = parseInt(urlParams.get('office_max')) || 4100;
    const maxPrice = parseInt(urlParams.get('price_max')) || 1500000;
    const chainFreeOnly = urlParams.get('chain_free_only') === '1';
    const gardenFacing = urlParams.get('garden_facing') || '';
    const favsOnly = urlParams.get('favs_only') === '1';

    document.getElementById('school_max').value = maxSchool;
    document.getElementById('office_max').value = maxOffice;
    document.getElementById('price_max').value = maxPrice;
    document.getElementById('chain_free_only').checked = chainFreeOnly;
    document.getElementById('garden_facing').value = gardenFacing;
    document.getElementById('favs_only').checked = favsOnly;

    function secsToText(s) {
        const m = Math.round(s / 60);
        return m >= 60 ? `${Math.floor(m / 60)} hr ${m % 60} min` : `${m} min`;
    }
    document.getElementById('school_max_text').textContent = secsToText(maxSchool);
    document.getElementById('office_max_text').textContent = secsToText(maxOffice);

    let filtered = all_properties.filter(p =>
        (p.school_commute_seconds == null || p.school_commute_seconds <= maxSchool) &&
        (p.office_commute_seconds == null || p.office_commute_seconds <= maxOffice) &&
        (p.listing_price == null || p.listing_price <= maxPrice) &&
        (!chainFreeOnly || p.chain_free === true) &&
        (!gardenFacing || p.garden_facing === gardenFacing) &&
        (!favsOnly || isFav(p.property_id))
    );

    filtered.sort((a, b) => (a.school_commute_seconds || 99999) - (b.school_commute_seconds || 99999));

    document.getElementById('result-count').innerText = `${filtered.length} properties`;

    const listContainer = document.getElementById('property-list');
    listContainer.innerHTML = '';
    markers = [];

    filtered.forEach((p) => {
        if (!p.latitude || !p.longitude) return;

        const price = p.listing_price || 0;
        const scaleVal = 0.8 + ((Math.min(Math.max(price, 500000), 1500000) - 500000) / 1000000) * 0.7;

        const recent = isRecent(p.listing_update_date);
        const pid = String(p.property_id);
        const { bg, border } = _pinColors(pid, recent);
        const pin = new PinElement({
            glyphText: (p.beds || '?').toString(),
            glyphColor: 'white',
            background: bg,
            borderColor: border,
            scale: scaleVal,
        });

        const marker = new AdvancedMarkerElement({
            map,
            position: { lat: p.latitude, lng: p.longitude },
            title: `${fmt(p.listing_price)} — ${p.beds || '?'} bed`,
            content: pin,
            gmpClickable: true,
        });
        markers.push(marker);
        markerRegistry[pid] = { marker, beds: (p.beds || '?').toString(), recent, scale: scaleVal };

        const tenureText = p.tenure
            ? p.tenure.charAt(0).toUpperCase() + p.tenure.slice(1).toLowerCase()
            : 'Unknown';
        const floorText = p.floor_size ? ` · ${p.floor_size} sqft` : '';
        const schoolText = p.school_commute_text || '—';
        const officeText = p.office_commute_text || '—';
        const estLine = p.estimate_price != null
            ? `${fmt(p.estimate_price)} <span style="color:#888;font-size:0.9em;">(${fmt(p.estimate_low)} – ${fmt(p.estimate_high)})</span>`
            : '—';
        const updatedBadge = recent
            ? `<span style="background:#e65100;color:white;font-size:0.75em;font-weight:700;padding:1px 5px;border-radius:3px;margin-left:4px;">NEW</span>`
            : '';
        const updatedText = p.listing_update_date
            ? `<span style="color:#888;font-size:0.85em;">Updated: ${p.listing_update_date}${updatedBadge}</span>`
            : '';
        const chainBadge = p.chain_free === true
            ? `<span style="background:#1565c0;color:white;font-size:0.75em;font-weight:700;padding:1px 5px;border-radius:3px;margin-left:4px;">CHAIN FREE</span>`
            : '';
        const epcText = p.epc_rating ? `EPC ${p.epc_rating}` : '';
        const ctbText = p.council_tax_band ? `CTB ${p.council_tax_band}` : '';
        const metaLine = [epcText, ctbText].filter(Boolean).join(' · ');

        // LLM metadata lines
        const gardenIcon = { 'South': '☀️', 'South-West': '🌤️', 'South-East': '🌤️', 'West': '🌥️', 'East': '🌥️', 'North': '🌑', 'North-East': '🌑', 'North-West': '🌑' };
        const gardenText = p.garden_facing && p.garden_facing !== 'Unknown'
            ? `${gardenIcon[p.garden_facing] || ''}${p.garden_facing}-facing` : '';
        const outdoorText = p.outdoor_space && p.outdoor_space !== 'None' ? p.outdoor_space : '';
        const parkingText = p.parking_type && p.parking_type !== 'None'
            ? `🚗 ${p.parking_type}${p.parking_spaces > 0 ? ` ×${p.parking_spaces}` : ''}${p.parking_ev ? ' ⚡' : ''}` : '';
        const devText = p.dev_types && p.dev_types !== 'None'
            ? `🏗 ${p.dev_types}${p.dev_planning && p.dev_planning !== 'None' ? ` (${p.dev_planning})` : ''}` : '';
        const quietText = p.quiet_rating && p.quiet_rating !== 'Unknown' ? `🤫 ${p.quiet_rating}` : '';
        const riverText = p.river_proximity && p.river_proximity !== 'None' ? `🌊 ${p.river_proximity}` : '';
        const periodText = p.period_features ? '🏛 Period' : '';
        const glazingText = p.double_glazing ? '🪟 DG' : '';
        const unmodText = p.dev_unmodernized ? '🔧 Needs work' : '';
        const llmLine1 = [gardenText, outdoorText, parkingText].filter(Boolean).join(' · ');
        const llmLine2 = [devText, quietText, riverText, periodText, glazingText, unmodText].filter(Boolean).join(' · ');

        const thumbHtml = p.thumbnail_url
            ? `<a href="${p.detail_url}" target="_blank"><img src="${p.thumbnail_url}" style="width:100%;border-radius:4px;margin-bottom:6px;display:block;" loading="lazy"/></a>`
            : '';

        const favBtnPopup = `<button class="fav-btn${isFav(p.property_id) ? ' is-fav' : ''}" data-pid="${p.property_id}" onclick="toggleFav('${p.property_id}', event)" title="${isFav(p.property_id) ? 'Remove favourite' : 'Add favourite'}" style="position:absolute;top:6px;right:6px;">${isFav(p.property_id) ? '❤️' : '🤍'}</button>`;

        const infoContent = `
            <div style="max-width:280px;font-family:sans-serif;font-size:13px;line-height:1.5;position:relative;">
                ${favBtnPopup}
                ${thumbHtml}<a href="${p.detail_url}" target="_blank" style="font-size:1.1em;font-weight:700;color:#0066cc;">${fmt(p.listing_price)}</a>
                <span style="margin-left:6px;color:#555;">${p.beds || '?'} bed · ${p.baths || '?'} bath · ${tenureText}${floorText}</span><br>
                <span style="color:#333;">${p.address || ''}</span>${chainBadge}<br>
                ${updatedText}${metaLine ? `<br><span style="color:#666;font-size:0.85em;">${metaLine}</span>` : ''}
                ${llmLine1 ? `<br><span style="font-size:0.85em;color:#444;">${llmLine1}</span>` : ''}
                ${llmLine2 ? `<br><span style="font-size:0.85em;color:#666;">${llmLine2}</span>` : ''}
                <div style="margin:6px 0;padding:4px 0;border-top:1px solid #eee;border-bottom:1px solid #eee;">
                    <strong>Estimate:</strong> ${p.estimate_url ? `<a href="${p.estimate_url}" target="_blank">${estLine}</a>` : estLine}<br>
                    <strong>Delta:</strong> ${deltaHtml(p.price_delta)}
                </div>
                <strong>Commute:</strong><br>
                🏫 <a href="${p.school_commute_url}" target="_blank">${schoolText}</a> &nbsp;
                🏢 <a href="${p.office_commute_url}" target="_blank">${officeText}</a>
            </div>`;

        marker.addEventListener('gmp-click', () => {
            infoWindow.close();
            infoWindow.setContent(infoContent);
            infoWindow.open(marker.map, marker);
        });

        // Sidebar list item
        const item = document.createElement('div');
        item.className = 'list-item' + (isFav(p.property_id) ? ' list-item--fav' : '');
        item.dataset.pid = p.property_id;
        item.innerHTML = `
            <button class="fav-btn${isFav(p.property_id) ? ' is-fav' : ''}" data-pid="${p.property_id}" onclick="toggleFav('${p.property_id}', event)" title="${isFav(p.property_id) ? 'Remove favourite' : 'Add favourite'}">${isFav(p.property_id) ? '❤️' : '🤍'}</button>
            ${p.thumbnail_url ? `<img src="${p.thumbnail_url}" style="width:100%;border-radius:4px;margin-bottom:4px;display:block;" loading="lazy"/>` : ''}
            <h3><a href="${p.detail_url}" target="_blank" style="color:inherit;text-decoration:none;">${fmt(p.listing_price)}</a>${chainBadge}</h3>
            <p style="margin:2px 0;">${p.address || ''}</p>
            <p style="margin:2px 0;color:#888;">${p.beds || '?'} bed · ${p.baths || '?'} bath · ${tenureText}${floorText}${metaLine ? ' · ' + metaLine : ''}</p>
            ${llmLine1 ? `<p style="margin:2px 0;font-size:0.85em;color:#444;">${llmLine1}</p>` : ''}
            ${llmLine2 ? `<p style="margin:2px 0;font-size:0.85em;color:#666;">${llmLine2}</p>` : ''}
            <p style="margin:4px 0;">
                Est: ${p.estimate_price != null ? fmt(p.estimate_price) : '—'}
                <span style="font-size:0.85em;color:#888;">(${fmt(p.estimate_low)} – ${fmt(p.estimate_high)})</span>
                &nbsp;${deltaHtml(p.price_delta)}
            </p>
            <div class="commute">
                🏫 ${schoolText} &nbsp;|&nbsp; 🏢 ${officeText}
            </div>`;

        item.addEventListener('click', () => {
            map.setCenter({ lat: p.latitude, lng: p.longitude });
            map.setZoom(15);
            infoWindow.close();
            infoWindow.setContent(infoContent);
            infoWindow.open(marker.map, marker);
        });

        listContainer.appendChild(item);
    });
}

initMap();
