'use strict';
// Independent loopback fixture server. Never imports server.js, dotenv, DB or agents.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const localFiles = ['index.html', 'app.mjs', 'store.mjs', 'fixtures.mjs', 'workspace.css'];
const files = new Map(localFiles.map(name => ['/' + name, path.join(__dirname, name)]));
files.set('/', path.join(__dirname, 'index.html'));
for (const name of ['command-deck/spatial-deck.js', 'command-deck/command-deck.css', 'shared/tokens.css']) {
  files.set('/' + name, path.join(__dirname, '../../public', name));
}
const mime = { '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.css': 'text/css' };
function createServer() {
  return http.createServer((req, res) => {
    res.setHeader('Content-Security-Policy', "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'none'; img-src 'self'; font-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'");
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
    const file = files.get(new URL(req.url, 'http://localhost').pathname);
    if (!file) { res.writeHead(404); return res.end('Fixture asset not found'); }
    res.setHeader('Content-Type', mime[path.extname(file)] + '; charset=utf-8');
    if (req.method === 'HEAD') return res.end();
    fs.createReadStream(file).on('error', () => { res.destroy(); }).pipe(res);
  });
}
if (require.main === module) {
  const port = Number(process.env.PORT || 4173);
  createServer().listen(port, '127.0.0.1', () => console.log(`Living Workspace fixtures: http://127.0.0.1:${port}`));
}
module.exports = { createServer };
