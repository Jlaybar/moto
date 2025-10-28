import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import cookieSession from 'cookie-session';
import crypto from 'node:crypto';
import { google } from 'googleapis';
// Prisma removed: DB handled by Flask service

const PORT = 3000;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Prisma/DB bootstrap removed. Node only serves chat/Gmail.

// SSE clients
const CLIENTS = new Set();
const app = express();
// Prisma client removed

// CORS middleware (simple, sin dependencia externa)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: '1mb' }));
// ----------------------------------------------------
//          SERVICIO GMAIL INI 
// ----------------------------------------------------
// Session para OAuth (usa cookie-session)
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
if (!process.env.SESSION_SECRET) {
  console.warn('[WARN] SESSION_SECRET no definido. Usando secreto temporal solo para desarrollo. Define SESSION_SECRET en .env.');
}
app.use(cookieSession({
  name: 'sess',
  keys: [SESSION_SECRET],
  httpOnly: true,
  sameSite: 'lax',
  secure: process.env.NODE_ENV === 'production',
  maxAge: 7 * 24 * 60 * 60 * 1000,
}));

// ============ Gmail OAuth + API ============
const CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || '';
// Por defecto, usa /auth/callback para alinearse con la configuración típica
const REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || `http://localhost:${PORT}/auth/callback`;
const OAUTH_CALLBACK_PATH = (() => { try { return new URL(REDIRECT_URI).pathname || '/auth/callback'; } catch { return '/auth/callback'; } })();
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/gmail.send',
  // Agrega modify si quieres marcar como leído: 'https://www.googleapis.com/auth/gmail.modify'
];

function getOAuth2Client(tokens) {
  const oAuth2Client = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI);
  if (tokens) oAuth2Client.setCredentials(tokens);
  return oAuth2Client;
}

// 1) Iniciar login
app.get('/gmail/auth/login', (req, res) => {
  if (!CLIENT_ID || !CLIENT_SECRET) return res.status(500).send('Config OAuth faltante');
  const oAuth2Client = getOAuth2Client();
  const url = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
    prompt: 'consent',
  });
  res.redirect(url);
});

// 2) Callback OAuth (ruta dinámica según REDIRECT_URI). Además exponemos /gmail/auth/callback como alias.
async function oauthCallbackHandler(req, res) {
  const code = req.query.code;
  const oAuth2Client = getOAuth2Client();
  try {
    const { tokens } = await oAuth2Client.getToken(code);
    req.session.tokens = tokens;
    res.redirect('/');
  } catch (e) {
    console.error(e);
    res.status(500).send('OAuth error');
  }
}
app.get(OAUTH_CALLBACK_PATH, oauthCallbackHandler);
if (OAUTH_CALLBACK_PATH !== '/gmail/auth/callback') {
  app.get('/gmail/auth/callback', oauthCallbackHandler);
}

// 3) Enviar correo (solo OAuth2 Gmail API)
app.post('/gmail/send', async (req, res) => {
  try {
    const to = String(req.body?.to || '').trim();
    const subject = String(req.body?.subject || '').trim();
    const message = String(req.body?.message || '');
    if (!to || !subject) return res.status(400).json({ error: 'to/subject requeridos' });

    if (!req.session.tokens) return res.status(401).send('Not authenticated');
    const auth = getOAuth2Client(req.session.tokens);
    const gmail = google.gmail({ version: 'v1', auth });

    const raw = [
      `To: ${to}`,
      `Subject: ${subject}`,
      'Content-Type: text/plain; charset="UTF-8"',
      'MIME-Version: 1.0',
      '',
      message,
    ].join('\r\n');

    const encoded = Buffer.from(raw)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    const result = await gmail.users.messages.send({ userId: 'me', requestBody: { raw: encoded } });
    return res.json({ id: result.data.id, status: 'sent' });
  } catch (e) {
    console.error(e);
    res.status(500).send('Send failed');
  }
});

// 4) Listar últimos correos recibidos (inbox)
app.get('/gmail/messages', async (req, res) => {
  try {
    if (!req.session.tokens) return res.status(401).send('Not authenticated');
    const auth = getOAuth2Client(req.session.tokens);
    const gmail = google.gmail({ version: 'v1', auth });

    const list = await gmail.users.messages.list({
      userId: 'me',
      labelIds: ['INBOX'],
      maxResults: 10,
    });

    const messages = [];
    if (list.data.messages?.length) {
      for (const m of list.data.messages) {
        const full = await gmail.users.messages.get({
          userId: 'me',
          id: m.id,
          format: 'metadata',
          metadataHeaders: ['From', 'Subject', 'Date'],
        });
        const headers = Object.fromEntries((full.data.payload.headers || []).map(h => [h.name, h.value]));
        messages.push({
          id: full.data.id,
          snippet: full.data.snippet,
          from: headers.From || '',
          subject: headers.Subject || '',
          date: headers.Date || '',
        });
      }
    }
    res.json(messages);
  } catch (e) {
    console.error(e);
    res.status(500).send('Fetch failed');
  }
});

// 5) Obtener cuerpo de un mensaje (texto plano)
app.get('/gmail/messages/:id', async (req, res) => {
  try {
    if (!req.session.tokens) return res.status(401).send('Not authenticated');
    const auth = getOAuth2Client(req.session.tokens);
    const gmail = google.gmail({ version: 'v1', auth });

    const msg = await gmail.users.messages.get({ userId: 'me', id: req.params.id, format: 'full' });

    function findPlain(part) {
      if (!part) return null;
      if (part.mimeType === 'text/plain' && part.body?.data) return part.body.data;
      if (part.parts) {
        for (const p of part.parts) {
          const found = findPlain(p);
          if (found) return found;
        }
      }
      return null;
    }

    const data = findPlain(msg.data.payload) || msg.data.snippet || '';
    const decoded = Buffer.from(data.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
    res.json({ id: msg.data.id, body: decoded });
  } catch (e) {
    console.error(e);
    res.status(500).send('Read failed');
  }
});

app.get('/gmail/me', (req, res) => {
  res.json({ authenticated: Boolean(req.session.tokens) });
});
// ----------------------------------------------------
//          SERVICIO CHAT
// ----------------------------------------------------

// Health
app.get('/healthz', (req, res) => {
  res.json({ ok: true });
});

// SSE stream
app.get('/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.write(`data: ${JSON.stringify({ type: 'system', text: 'Conectado al stream de chat' })}\n\n`);
  CLIENTS.add(res);
  const heartbeat = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 30000);
  req.on('close', () => { clearInterval(heartbeat); CLIENTS.delete(res); });
});

function broadcast(payload) {
  for (const client of CLIENTS) {
    try { client.write(`data: ${JSON.stringify(payload)}\n\n`); }
    catch { try { client.end(); } catch {} CLIENTS.delete(client); }
  }
}

// Chat message
app.post('/message', (req, res) => {
  const user = req.body?.user ? String(req.body.user).slice(0, 32) : 'anon';
  const text = req.body?.text ? String(req.body.text).slice(0, 1000) : '';
  if (!text.trim()) return res.status(400).json({ error: 'text requerido' });
  const msg = { type: 'message', user, text, ts: Date.now() };
  broadcast(msg);
  res.status(202).json({ ok: true });
});

// Clear chat
app.post('/clear', (req, res) => {
  broadcast({ type: 'clear' });
  res.status(202).json({ ok: true });
});

// DB endpoints removed. Use Flask service on port 5000 for DB APIs.

// Serve index: prefer public/index.html, fallback to docs/index.html
app.get(['/', '/index.html'], (req, res) => {
  const publicIndex = path.join(__dirname, '..', 'public', 'index.html');
  const docsIndex = path.join(__dirname, '..', 'docs', 'index.html');
  let filePath = publicIndex;
  if (!fs.existsSync(publicIndex) && fs.existsSync(docsIndex)) filePath = docsIndex;
  if (fs.existsSync(filePath)) res.sendFile(filePath);
  else res.status(404).send('index.html no encontrado');
});

// 404
app.use((req, res) => {
  res.status(404).json({ error: 'not found' });
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat backend escuchando en http://localhost:${PORT}`);
  console.log('Endpoints:');
  console.log('  GET    /healthz');
  console.log('  GET    /events   (SSE stream)');
  console.log('  POST   /message  {"user","text"}');
  console.log('  POST   /clear    (limpiar chat)');
  console.log('  GET    /gmail/auth/login        (OAuth inicio)');
  console.log('  GET    /gmail/auth/callback     (OAuth callback)');
  console.log('  POST   /gmail/send              {"to","subject","message"}');
  console.log('  GET    /gmail/messages          (listar últimos)');
  console.log('  GET    /gmail/messages/:id      (cuerpo texto)');
  console.log('  GET    /gmail/me                (estado auth)');
});

// Graceful shutdown
process.on('SIGINT', async () => { server.close(() => process.exit(0)); });
process.on('SIGTERM', async () => { server.close(() => process.exit(0)); });
