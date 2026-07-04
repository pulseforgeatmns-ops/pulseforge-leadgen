require('dotenv').config();

let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => { input += chunk; });
process.stdin.on('end', async () => {
  try {
    const context = JSON.parse(input || '{}');
    if (Number(context.client_id) !== Number(process.env.ACTIVE_CLIENT_ID)) {
      throw new Error('Autorun child client context mismatch');
    }
    const { run } = require('../emmettAgent');
    const result = await run(context);
    console.log(`EMMETT_AUTORUN_RESULT=${JSON.stringify(result || {})}`);
    process.exit(0);
  } catch (err) {
    console.error(err.stack || err.message);
    process.exit(1);
  }
});
