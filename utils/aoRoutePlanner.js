const axios = require('axios');
const { buildDirectMailOpening } = require('./aoMessageTemplates');
const { normalizeDueDate } = require('./aoQueueFormat');
const { PLACES_FEATURES } = require('./placesCostAttribution');
const { geocodeAddress: tracedGeocodeAddress } = require('./placesApi');

const ANCHOR_OFFICE_DEFAULT = 'Manchester, NH';
const ANCHOR_OFFICE_COORDS = { lat: 42.9956, lng: -71.4548 };

const PLACEHOLDER_ADDRESS = /^(tbd|n\/a|none|unknown|address needed|—|-)$/i;

function isUsableAddress(address) {
  const text = String(address || '').trim();
  if (!text || text.length < 5) return false;
  if (PLACEHOLDER_ADDRESS.test(text)) return false;
  return true;
}

function haversineKm(lat1, lng1, lat2, lng2) {
  const toRad = deg => (deg * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

async function geocodeAddress(address) {
  const text = String(address || '').trim();
  if (!isUsableAddress(text)) return null;

  if (process.env.GOOGLE_PLACES_KEY) {
    try {
      const traced = await tracedGeocodeAddress({
        address: text,
        apiKey: process.env.GOOGLE_PLACES_KEY,
        record: {
          caller: 'aoRoutePlanner.js',
          feature: PLACES_FEATURES.GEOCODE,
        },
      });
      const loc = traced.data?.results?.[0]?.geometry?.location;
      if (loc?.lat != null && loc?.lng != null) {
        return { lat: Number(loc.lat), lng: Number(loc.lng), source: 'google' };
      }
    } catch (err) {
      console.warn('[ao-route] Google geocode failed:', err.message);
    }
  }

  try {
    const { data } = await axios.get('https://nominatim.openstreetmap.org/search', {
      params: {
        q: text.includes('NH') || text.includes('New Hampshire') ? text : `${text}, Manchester, NH`,
        format: 'json',
        limit: 1,
        countrycodes: 'us',
      },
      headers: { 'User-Agent': 'Pulseforge-AO-RoutePlanner/1.0' },
      timeout: 8000,
    });
    if (Array.isArray(data) && data[0]?.lat && data[0]?.lon) {
      return { lat: Number(data[0].lat), lng: Number(data[0].lon), source: 'nominatim' };
    }
  } catch (err) {
    console.warn('[ao-route] Nominatim geocode failed:', err.message);
  }

  return null;
}

function resolveStartPoint({ startPointType, startLat, startLng, startAddress, anchorOfficeAddress }) {
  if (startPointType === 'current_location' && startLat != null && startLng != null) {
    return { lat: Number(startLat), lng: Number(startLng), label: 'Current location' };
  }
  if (startPointType === 'custom' && startLat != null && startLng != null) {
    return { lat: Number(startLat), lng: Number(startLng), label: startAddress || 'Custom start' };
  }
  if (startPointType === 'anchor_office') {
    return {
      lat: ANCHOR_OFFICE_COORDS.lat,
      lng: ANCHOR_OFFICE_COORDS.lng,
      label: anchorOfficeAddress || ANCHOR_OFFICE_DEFAULT,
      needsGeocode: false,
    };
  }
  return null;
}

function attachDistances(stops, startPoint) {
  if (!startPoint?.lat || !startPoint?.lng) {
    return stops.map(s => ({ ...s, distance_km: null }));
  }
  return stops.map(stop => ({
    ...stop,
    distance_km: stop.lat != null && stop.lng != null
      ? haversineKm(startPoint.lat, startPoint.lng, stop.lat, stop.lng)
      : null,
  }));
}

function sortStopsByMode(stops, startPoint, sortMode) {
  const withDistance = attachDistances(stops, startPoint);

  if (sortMode === 'manual') {
    return [...withDistance].sort((a, b) => a.sequence - b.sequence);
  }

  const geocoded = withDistance.filter(s => s.distance_km != null);
  const ungeocoded = withDistance.filter(s => s.distance_km == null);

  if (sortMode === 'farthest_first') {
    geocoded.sort((a, b) => b.distance_km - a.distance_km);
  } else if (sortMode === 'closest_first') {
    geocoded.sort((a, b) => a.distance_km - b.distance_km);
  } else if (sortMode === 'shortest_route') {
    return greedyNearestNeighbor([...geocoded], startPoint).concat(ungeocoded);
  }

  return geocoded.concat(ungeocoded);
}

function greedyNearestNeighbor(stops, startPoint) {
  if (!startPoint?.lat || !startPoint?.lng || !stops.length) return stops;

  const remaining = [...stops];
  const ordered = [];
  let current = { lat: startPoint.lat, lng: startPoint.lng };

  while (remaining.length) {
    let bestIdx = 0;
    let bestDist = Infinity;
    for (let i = 0; i < remaining.length; i += 1) {
      const s = remaining[i];
      if (s.lat == null || s.lng == null) continue;
      const dist = haversineKm(current.lat, current.lng, s.lat, s.lng);
      if (dist < bestDist) {
        bestDist = dist;
        bestIdx = i;
      }
    }
    const next = remaining.splice(bestIdx, 1)[0];
    ordered.push(next);
    current = { lat: next.lat, lng: next.lng };
  }
  return ordered;
}

const NAV_APPS = Object.freeze(['google_maps', 'waze', 'apple_maps', 'ask_every_time']);

function buildNavigateUrl(address, app = 'google_maps') {
  const text = String(address || '').trim();
  if (!isUsableAddress(text)) return null;
  const q = encodeURIComponent(text);
  if (app === 'waze') return `https://waze.com/ul?q=${q}&navigate=yes`;
  if (app === 'apple_maps') return `http://maps.apple.com/?daddr=${q}`;
  return `https://www.google.com/maps/dir/?api=1&destination=${q}`;
}

function buildNavigateUrls(address) {
  if (!isUsableAddress(address)) return null;
  return {
    google_maps: buildNavigateUrl(address, 'google_maps'),
    waze: buildNavigateUrl(address, 'waze'),
    apple_maps: buildNavigateUrl(address, 'apple_maps'),
  };
}

function buildMapsNavigateUrl(address) {
  return buildNavigateUrl(address, 'google_maps');
}

function formatSourceBadge(attributionSource, campaignName) {
  if (attributionSource === 'direct_mail_campaign') {
    return `Direct Mail${campaignName ? ` · ${campaignName}` : ''}`;
  }
  if (attributionSource === 'ao_field_visit') return 'Field Visit';
  return attributionSource || 'Follow-up';
}

function buildStopBrief(row) {
  const attributionSource = row.attributionSource || row.attribution_source;
  const campaignName = row.campaignName || row.campaign_name;
  const originalVisitNote = row.originalVisitNote || row.original_visit_note;
  const lastInteractionSummary = row.lastInteractionSummary || row.last_interaction_summary;
  const nextAction = row.nextAction || row.next_action;

  if (attributionSource === 'direct_mail_campaign') {
    const campaign = campaignName || 'Campaign 001';
    return `They received ${campaign} direct mail. Your goal is to confirm it reached the right person and find who handles cleaning or facilities vendors.`;
  }
  if (originalVisitNote) return originalVisitNote;
  if (lastInteractionSummary) return lastInteractionSummary;
  if (nextAction) return `Next action: ${nextAction}`;
  return 'Follow up in person and identify who handles cleaning or facility vendors.';
}

function buildSuggestedOpening(row) {
  const attributionSource = row.attributionSource || row.attribution_source;
  const aoName = row.aoName || row.ao_name;
  const suggestedMessage = row.suggestedMessage || row.suggested_message;
  if (suggestedMessage) return suggestedMessage;
  if (attributionSource === 'direct_mail_campaign') {
    return buildDirectMailOpening(aoName);
  }
  return 'Hey, I\'m with Anchor Cleaning. We\'re local and help businesses keep their spaces consistently clean. Quick question: who usually handles your cleaning or facilities vendors?';
}

function formatContactLine({ contactName, contactTitle, contactPhone, contactEmail }) {
  if (!contactName) return 'No contact yet';
  const parts = [contactName];
  if (contactTitle) parts.push(contactTitle);
  if (contactPhone) parts.push(contactPhone);
  if (contactEmail) parts.push(contactEmail);
  return parts.join(' · ');
}

function buildNextStopDebrief(nextStop, aoName) {
  if (!nextStop) {
    return '\n\nYou\'re done with this route. Nice work. Check your queue or start another route.';
  }

  const opening = buildSuggestedOpening(nextStop);
  const brief = buildStopBrief(nextStop);
  const contact = formatContactLine(nextStop);
  const address = isUsableAddress(nextStop.address) ? nextStop.address : 'Address needed';

  return [
    '',
    `Your next stop is ${nextStop.business_name}.`,
    '',
    `Address: ${address}`,
    '',
    `Contact: ${contact}`,
    '',
    `Brief: ${brief}`,
    '',
    `Opening: '${opening}'`,
    '',
    'Actions: Navigate / Log This Visit / Call Instead / Skip / Move Later',
  ].join('\n');
}

function normalizePhoneForTel(phone) {
  const raw = String(phone || '').trim();
  if (!raw) return null;
  const digits = raw.replace(/[^\d+]/g, '');
  if (digits.replace(/\D/g, '').length < 7) return null;
  return digits.startsWith('+') ? digits : digits.replace(/\D/g, '');
}

function buildTelUrl(phone) {
  const normalized = normalizePhoneForTel(phone);
  if (!normalized) return null;
  return `tel:${normalized}`;
}

function buildPhoneFollowUpDebrief(task, aoName) {
  if (!task) return '';

  const opening = buildSuggestedOpening(task);
  const brief = buildStopBrief(task);
  const contact = formatContactLine(task);
  const phone = task.contact_phone || null;
  const telUrl = buildTelUrl(phone);

  return [
    '',
    `Your next phone follow-up is ${task.business_name}.`,
    '',
    phone ? `Phone: ${phone}` : 'Phone: not saved yet',
    '',
    `Contact: ${contact}`,
    '',
    `Brief: ${brief}`,
    '',
    `Opening: '${opening}'`,
    '',
    telUrl
      ? 'Actions: Call Now / Log Call With Max / Move Later / Escalate'
      : 'Actions: Log Call With Max / Move Later / Escalate',
  ].join('\n');
}

function buildWorkCompleteDebrief() {
  return '\n\nYou\'re done with this route. Nice work. Check your queue or start another route.';
}

function enrichStopRow(row, aoName) {
  const address = row.address || row.lead_address || null;
  return {
    id: row.stop_id || row.id,
    route_id: row.route_id,
    task_id: row.task_id,
    lead_id: row.lead_id,
    sequence: row.sequence,
    status: row.stop_status || row.status || 'pending',
    address,
    address_usable: isUsableAddress(address),
    lat: row.lat != null ? Number(row.lat) : null,
    lng: row.lng != null ? Number(row.lng) : null,
    business_name: row.business_name,
    due_date: normalizeDueDate(row.due_date),
    priority: row.priority,
    next_action: row.next_action,
    attribution_source: row.attribution_source,
    campaign_name: row.campaign_name || null,
    source_badge: formatSourceBadge(row.attribution_source, row.campaign_name),
    contact_name: row.contact_name || null,
    contact_title: row.contact_title || null,
    contact_phone: row.contact_phone || null,
    contact_email: row.contact_email || null,
    contact_line: formatContactLine(row),
    original_visit_note: row.original_visit_note || null,
    last_interaction_summary: row.last_interaction_summary || null,
    suggested_message: row.suggested_message || null,
    brief: buildStopBrief(row),
    opening: buildSuggestedOpening({ ...row, aoName }),
    navigate_url: isUsableAddress(address) ? buildMapsNavigateUrl(address) : null,
    navigate_urls: buildNavigateUrls(address),
  };
}

module.exports = {
  ANCHOR_OFFICE_DEFAULT,
  ANCHOR_OFFICE_COORDS,
  NAV_APPS,
  isUsableAddress,
  haversineKm,
  geocodeAddress,
  resolveStartPoint,
  sortStopsByMode,
  greedyNearestNeighbor,
  buildNavigateUrl,
  buildNavigateUrls,
  buildMapsNavigateUrl,
  formatSourceBadge,
  buildStopBrief,
  buildSuggestedOpening,
  buildNextStopDebrief,
  normalizePhoneForTel,
  buildTelUrl,
  buildPhoneFollowUpDebrief,
  buildWorkCompleteDebrief,
  enrichStopRow,
};
