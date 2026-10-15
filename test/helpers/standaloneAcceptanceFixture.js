'use strict';
const { illustrativePhoto, DEMO_NAME, DEMO_NOTICE, ZONES } = require('../../standalone/anchor-portal/fixtures');
async function seed(pool, hashes) {
 const schema = require('../../utils/anchorPortalSchema').createPortalSchema(pool);
    const users = {};
    for (const [key, role, tenant] of [['client', 'facility_client', 10], ['operator', 'manager', 10], ['cleaner', 'cleaner', 10], ['unassigned', 'facility_client', 10], ['foreignOperator', 'manager', 11], ['foreignClient', 'facility_client', 11]]) {
      // Hashes must be provisioned separately; no password defaults.
      const { rows } = await pool.query(`INSERT INTO users (name,email,password_hash,role,client_id) VALUES ($1,$2,$5,$3,$4) RETURNING *`, [`Synthetic ${key}`, `${key}@exeter-fixture.invalid`, role, tenant, hashes[key]]);
      users[key] = rows[0];
    }
    const { rows: locations } = await pool.query(`INSERT INTO anchor_portal_locations (operator_client_id,name,slug,address,is_demo) VALUES (10,$1,'synthetic-exeter-demo',$2,true),(11,'Synthetic Foreign Building','synthetic-foreign','Synthetic isolation fixture',true),(10,'Synthetic Unassigned Building','synthetic-unassigned','Synthetic isolation fixture',true) RETURNING *`, [DEMO_NAME, DEMO_NOTICE]);
    const location = locations[0]; const foreignLocation = locations[1]; const unassignedLocation = locations[2];
    for (const key of ['client', 'cleaner']) await pool.query('INSERT INTO anchor_portal_user_locations VALUES ($1,$2)', [users[key].id, location.id]);
    await pool.query('INSERT INTO anchor_portal_user_locations VALUES ($1,$2)', [users.foreignClient.id, foreignLocation.id]);
    await schema.insertScopeForLocation(location.id, ZONES);
    const visits = [];
    for (let i = 0; i < 3; i++) {
      const { rows } = await pool.query(`INSERT INTO anchor_portal_visits (location_id,cleaner_id,scheduled_at,started_at,completed_at,status,visit_notes) VALUES ($1,$2,NOW()-($3::int * INTERVAL '1 day'),NOW()-($3::int * INTERVAL '1 day'),NOW()-($3::int * INTERVAL '1 day'),'completed',$4) RETURNING *`, [location.id, users.cleaner.id, i + 1, `Sample report ${i + 1}: ${ZONES[i].title}. ${DEMO_NOTICE}`]);
      const visit = rows[0]; visits.push(visit);
      await pool.query('UPDATE anchor_portal_visits SET completed_by=$1 WHERE id=$2', [users.operator.id, visit.id]);
      await schema.copyScopeToVisitItems(visit.id, location.id);
      await pool.query(`UPDATE anchor_portal_visit_items SET outcome='completed',completed_at=NOW() WHERE visit_id=$1`, [visit.id]);
      const photo = illustrativePhoto(i);
      await pool.query(`INSERT INTO anchor_portal_evidence (visit_id,mime_type,data_url,caption) VALUES ($1,'image/png',$2,$3)`, [visit.id, photo.data_url, photo.caption]);
    }
    const { rows: foreignVisits } = await pool.query(`INSERT INTO anchor_portal_visits (location_id,status,visit_notes) VALUES ($1,'scheduled','Synthetic foreign visit') RETURNING *`, [foreignLocation.id]);
    const { rows: issues } = await pool.query(`INSERT INTO anchor_portal_issues (location_id,reported_by_user_id,source,title,description,room_category,status,resolution,resolved_at) VALUES ($1,$2,'client','Sample open issue','Synthetic sample: detail dust check pending.',$3,'open',NULL,NULL),($1,$2,'client','Sample resolved issue','Synthetic sample: fixture detail check.',$4,'resolved','Synthetic sample resolution: illustrative detail pass verified.',NOW()) RETURNING *`, [location.id, users.client.id, ZONES[0].title, ZONES[2].title]);
    const photo = illustrativePhoto(2);
    // Backend migration supplies purpose; do not silently omit proof metadata.
    await pool.query(`INSERT INTO anchor_portal_evidence (issue_id,mime_type,data_url,caption,purpose) VALUES ($1,'image/png',$2,$3,'resolution')`, [issues[1].id, photo.data_url, photo.caption]);
return { users, location, foreignLocation, unassignedLocation, visits, issues };
}

async function initialize(pool, marker, hashes) {
 await require('../../standalone/anchor-portal/initialize').initializeSchema(pool, marker);
 const fixture = await seed(pool, hashes);
 await pool.query("UPDATE portal_demo_marker SET expires_at=NOW()+INTERVAL '48 hours'");
 await pool.query("UPDATE users SET valid_until=NOW()+INTERVAL '48 hours'");
 return fixture;
}
module.exports = { initialize };
