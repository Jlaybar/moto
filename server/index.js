import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import url, { fileURLToPath } from 'node:url';

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const CLIENTS = new Set();

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendSse(res, payload) {
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function broadcast(payload) {
  for (const client of CLIENTS) {
    try {
      sendSse(client, payload);
    } catch (e) {
      // Best-effort; remove broken clients
      try { client.end(); } catch {}
      CLIENTS.delete(client);
    }
  }
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => {
      data += chunk;
      // Prevent overly large bodies
      if (data.length > 1e6) {
        req.destroy();
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => {
      if (!data) return resolve(undefined);
      try { resolve(JSON.parse(data)); }
      catch (e) { reject(new Error('Invalid JSON')); }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const method = req.method || 'GET';

  // CORS preflight
  if (method === 'OPTIONS') {
    setCors(res);
    res.statusCode = 204;
    res.end();
    return;
  }

  // Health check
  if (method === 'GET' && parsed.pathname === '/healthz') {
    setCors(res);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // SSE events stream
  if (method === 'GET' && parsed.pathname === '/events') {
    setCors(res);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });

    // Initial hello
    sendSse(res, { type: 'system', text: 'Conectado al stream de chat' });

    CLIENTS.add(res);

    // Heartbeat to keep connections alive behind proxies
    const heartbeat = setInterval(() => {
      try { res.write(`: ping\n\n`); } catch {}
    }, 30000);

    req.on('close', () => {
      clearInterval(heartbeat);
      CLIENTS.delete(res);
    });
    return;
  }

  // Post a message
  if (method === 'POST' && parsed.pathname === '/message') {
    setCors(res);
    try {
      const body = await parseBody(req);
      const user = body?.user ? String(body.user).slice(0, 32) : 'anon';
      const text = body?.text ? String(body.text).slice(0, 1000) : '';
      if (!text.trim()) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'text requerido' }));
        return;
      }
      const msg = { type: 'message', user, text, ts: Date.now() };
      broadcast(msg);
      res.writeHead(202, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message || 'invalid body' }));
    }
    return;
  }

  // Clear chat for all clients
  if (method === 'POST' && parsed.pathname === '/clear') {
    setCors(res);
    broadcast({ type: 'clear' });
    res.writeHead(202, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // Serve demo client
  if (method === 'GET' && (parsed.pathname === '/' || parsed.pathname === '/index.html')) {
    setCors(res);
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    const filePath = path.join(__dirname, '..', 'public', 'index.html');
    try {
      const content = fs.readFileSync(filePath);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(content);
    } catch {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('index.html no encontrado');
    }
    return;
  }

  // Not found
  setCors(res);
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat backend escuchando en http://localhost:${PORT}`);
  console.log('Endpoints:');
  console.log('  GET  /healthz');
  console.log('  GET  /events   (SSE stream)');
  console.log('  POST /message  {"user","text"}');
  console.log('  POST /clear    (limpiar chat)');
});
