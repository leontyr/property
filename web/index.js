let map;
let all_properties = typeof window_properties !== 'undefined' ? window_properties : [];
let markers = [];
let infoWindow;

function fmt(n) {
    if (n == null) return '—';
    return '£' + Math.round(n).toLocaleString('en-GB');
}

function isRecent(dateStr) {
    if (!dateStr) return false;
    const d = new Date(dateStr);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const threeDaysAgo = new Date(today);
    threeDaysAgo.setDate(today.getDate() - 2);
    return d >= threeDaysAgo;
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
        position: { lat: 51.51922, lng: -0.09738 },
        title: "Office",
        content: new PinElement({ glyphText: "O", background: "black", glyphColor: "white" }),
        zIndex: 1000,
    });

    // Read URL params for filtering
    const urlParams = new URLSearchParams(window.location.search);
    const maxSchool = parseInt(urlParams.get('school_max')) || 1500;
    const maxOffice = parseInt(urlParams.get('office_max')) || 4100;
    const maxPrice = parseInt(urlParams.get('price_max')) || 1500000;

    document.getElementById('school_max').value = maxSchool;
    document.getElementById('office_max').value = maxOffice;
    document.getElementById('price_max').value = maxPrice;

    let filtered = all_properties.filter(p =>
        (p.school_commute_seconds == null || p.school_commute_seconds <= maxSchool) &&
        (p.office_commute_seconds == null || p.office_commute_seconds <= maxOffice) &&
        (p.listing_price == null || p.listing_price <= maxPrice)
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
        const pin = new PinElement({
            glyphText: (p.beds || '?').toString(),
            glyphColor: 'white',
            background: recent ? '#e65100' : 'green',
            borderColor:  recent ? '#bf360c' : '#2e7d32',
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

        const infoContent = `
            <div style="max-width:280px;font-family:sans-serif;font-size:13px;line-height:1.5;">
                <a href="${p.detail_url}" target="_blank" style="font-size:1.1em;font-weight:700;color:#0066cc;">${fmt(p.listing_price)}</a>
                <span style="margin-left:6px;color:#555;">${p.beds || '?'} bed · ${p.baths || '?'} bath · ${tenureText}${floorText}</span><br>
                <span style="color:#333;">${p.address || ''}</span><br>
                ${updatedText}<br>
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
        item.className = 'list-item';
        item.innerHTML = `
            <h3><a href="${p.detail_url}" target="_blank" style="color:inherit;text-decoration:none;">${fmt(p.listing_price)}</a></h3>
            <p style="margin:2px 0;">${p.address || ''}</p>
            <p style="margin:2px 0;color:#888;">${p.beds || '?'} bed · ${p.baths || '?'} bath · ${tenureText}${floorText}</p>
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
