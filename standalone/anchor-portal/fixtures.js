'use strict';
const zlib = require('node:zlib');
const DEMO_NAME = 'Phillips Exeter Academy — Demo Building';
const DEMO_NOTICE = 'Synthetic sample data and illustrative images only. This demonstration does not represent actual service at Phillips Exeter Academy.';
const ZONES = [
  { title: 'Construction Zone A — Classroom Renovation', items: ['Illustrative dust removal', 'Illustrative floor detail'] },
  { title: 'Renovation Zone B — Corridor', items: ['Illustrative debris check', 'Illustrative glass detail'] },
  { title: 'Construction Zone C — Restroom Fit-out', items: ['Illustrative fixture wipe', 'Illustrative final inspection'] },
];

function crc32(bytes) {
  let crc = 0xffffffff;
  for (const b of bytes) { crc ^= b; for (let i = 0; i < 8; i++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0); }
  return (crc ^ 0xffffffff) >>> 0;
}
function chunk(type, bytes) {
  const head = Buffer.alloc(4); head.writeUInt32BE(bytes.length);
  const body = Buffer.concat([Buffer.from(type), bytes]);
  const tail = Buffer.alloc(4); tail.writeUInt32BE(crc32(body));
  return Buffer.concat([head, body, tail]);
}
// Original deterministic raster illustration, no downloaded/client imagery.
function illustrativePhoto(index = 0) {
  const width = 320; const height = 200;
  const raw = Buffer.alloc((width * 3 + 1) * height);
  const colors = [[50, 130, 160], [75, 145, 95], [170, 120, 65]];
  for (let y = 0; y < height; y++) for (let x = 0; x < width; x++) {
    let rgb = y > 130 ? [175, 183, 190] : [225, 234, 240];
    if (x > 40 && x < 150 && y > 35 && y < 125) rgb = colors[index % colors.length];
    if ((x > 180 && x < 275 && y > 70 && y < 145) || (y > 175 && (x + index * 9) % 30 < 15)) rgb = [235, 180, 55];
    const offset = y * (width * 3 + 1) + 1 + x * 3;
    raw[offset] = rgb[0]; raw[offset + 1] = rgb[1]; raw[offset + 2] = rgb[2];
  }
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(width); ihdr.writeUInt32BE(height, 4); ihdr[8] = 8; ihdr[9] = 2;
  const png = Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), chunk('IHDR', ihdr), chunk('tEXt', Buffer.from(`Description\0${DEMO_NOTICE}`)), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
  return { data_url: `data:image/png;base64,${png.toString('base64')}`, caption: `Synthetic illustration ${index + 1}; original procedural facility-zone drawing, not an Exeter photograph.` };
}

async function seedDemo(pool, hashes, expiresAt) {
 const schema = require('../../utils/anchorPortalSchema').createPortalSchema(pool);
    const users = {};
    for (const [key, role, tenant] of [['client', 'facility_client', 10], ['operator', 'manager', 10]]) {
      // Hashes must be provisioned separately; no password defaults.
      const { rows } = await pool.query(`INSERT INTO users (name,email,password_hash,role,client_id,valid_until) VALUES ($1,$2,$5,$3,$4,$6) RETURNING *`, [`Synthetic ${key}`, `${key}@exeter-fixture.invalid`, role, tenant, hashes[key], expiresAt]);
      users[key] = rows[0];
    }
    const { rows: locations } = await pool.query(`INSERT INTO anchor_portal_locations (operator_client_id,name,slug,address,is_demo) VALUES (10,$1,'synthetic-exeter-demo',$2,true) RETURNING *`, [DEMO_NAME, DEMO_NOTICE]);
    const location = locations[0];
    for (const key of ['client']) await pool.query('INSERT INTO anchor_portal_user_locations VALUES ($1,$2)', [users[key].id, location.id]);
    await schema.insertScopeForLocation(location.id, ZONES);
    const visits = [];
    for (let i = 0; i < 3; i++) {
      const { rows } = await pool.query(`INSERT INTO anchor_portal_visits (location_id,cleaner_id,scheduled_at,started_at,completed_at,status,visit_notes) VALUES ($1,$2,NOW()-($3::int * INTERVAL '1 day'),NOW()-($3::int * INTERVAL '1 day'),NOW()-($3::int * INTERVAL '1 day'),'completed',$4) RETURNING *`, [location.id, null, i + 1, `Sample report ${i + 1}: ${ZONES[i].title}. ${DEMO_NOTICE}`]);
      const visit = rows[0]; visits.push(visit);
      await pool.query('UPDATE anchor_portal_visits SET completed_by=$1 WHERE id=$2', [users.operator.id, visit.id]);
      await schema.copyScopeToVisitItems(visit.id, location.id);
      await pool.query(`UPDATE anchor_portal_visit_items SET outcome='completed',completed_at=NOW() WHERE visit_id=$1`, [visit.id]);
      const photo = illustrativePhoto(i);
      await pool.query(`INSERT INTO anchor_portal_evidence (visit_id,mime_type,data_url,caption) VALUES ($1,'image/png',$2,$3)`, [visit.id, photo.data_url, photo.caption]);
    }
    const { rows: issues } = await pool.query(`INSERT INTO anchor_portal_issues (location_id,reported_by_user_id,source,title,description,room_category,status,resolution,resolved_at) VALUES ($1,$2,'client','Sample open issue','Synthetic sample: detail dust check pending.',$3,'open',NULL,NULL),($1,$2,'client','Sample resolved issue','Synthetic sample: fixture detail check.',$4,'resolved','Synthetic sample resolution: illustrative detail pass verified.',NOW()) RETURNING *`, [location.id, users.client.id, ZONES[0].title, ZONES[2].title]);
    const photo = illustrativePhoto(2);
    // Backend migration supplies purpose; do not silently omit proof metadata.
    await pool.query(`INSERT INTO anchor_portal_evidence (issue_id,mime_type,data_url,caption,purpose) VALUES ($1,'image/png',$2,$3,'resolution')`, [issues[1].id, photo.data_url, photo.caption]);
return { users, location, visits, issues };
}

module.exports = { seedDemo, illustrativePhoto, DEMO_NAME, DEMO_NOTICE, ZONES };
