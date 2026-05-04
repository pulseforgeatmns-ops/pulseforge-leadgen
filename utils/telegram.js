const axios = require('axios');

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID   = process.env.TELEGRAM_CHAT_ID;
const DASHBOARD = 'https://pulseforge-leadgen-production.up.railway.app';

const AGENT_NAMES = {
  linkedin:        'Link',
  facebook:        'Faye',
  facebook_page:   'Paige',
  google_business: 'Paige',
  google_review:   'Vera',
};

const CHANNEL_LABELS = {
  linkedin:        'LinkedIn',
  facebook:        'Facebook',
  facebook_page:   'Facebook Page',
  google_business: 'Google Business',
  google_review:   'Google Review',
};

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '…' : str;
}

async function sendTelegramNotification(item) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.log('Telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — skipping notification');
    return;
  }

  const agent   = AGENT_NAMES[item.channel]   || item.channel;
  const channel = CHANNEL_LABELS[item.channel] || item.channel;
  const context = truncate(item.post_content, 60);
  const comment = truncate(item.comment, 200);

  const text = [
    `🔔 New approval needed — ${agent}`,
    ``,
    `Channel: ${channel}`,
    ``,
    context,
    ``,
    comment,
    ``,
    `→ Review: ${DASHBOARD}`,
  ].join('\n');

  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      chat_id: CHAT_ID,
      text,
    });
  } catch (err) {
    console.error('Telegram notification failed:', err.response?.data?.description || err.message);
  }
}

module.exports = { sendTelegramNotification };
