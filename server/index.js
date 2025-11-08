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
// Estáticos: servir /public y /data
app.use('/public', express.static(path.join(__dirname, '..', 'public')));
app.use('/data', express.static(path.join(__dirname, '..', 'data')));
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

// DB endpoints removed. Use Flask service on port 5000 for DB APIs.

// ----------------------------------------------------
//          MODELO: precio vs km (JSON por marca/modelo)
// ----------------------------------------------------
// Devuelve el JSON existente en data/model/{marca}/{modelo}.json
// Ejemplos:
//   GET /api/model/honda/x-adv
//   GET /api/plot_price_km_by_year_json?marca=honda&modelo=x-adv
function resolveModelPath(marca, modelo) {
  const safeMarca = String(marca || '').trim().toLowerCase();
  // Normalizamos modelo: minúsculas, espacios->'_', múltiples separadores -> '-'
  let safeModelo = String(modelo || '').trim().toLowerCase();
  safeModelo = safeModelo
    .replace(/\s+/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/[^a-z0-9_\-\.]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^[-_]+|[-_]+$/g, '');
  // Construimos ruta
  const filePath = path.join(__dirname, '..', 'data', 'moto', 'model', safeMarca, `${safeModelo}.json`);
  return filePath;
}

app.get('/api/moto/model/:marca/:modelo', (req, res) => {
  try {
    const filePath = resolveModelPath(req.params.marca, req.params.modelo);
    if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'not found' });
    res.sendFile(filePath);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'failed to read model json' });
  }
});

// Ruta legacy eliminada: /api/plot_price_km_by_year_json (usar /api/model/:marca/:modelo)

// Índice de modelos estimados: devuelve data/model/models_index.json (o fallback a data/models_index.json)
app.get('/api/moto_models_index', (req, res) => {
  try {
    const filePath = path.join(__dirname, '..', 'data', 'moto', 'model', 'models_index.json');
    if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'models index not found' });
    res.sendFile(filePath);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'failed to read models index' });
  }
});

// Serve index: preferir public/index.html; fallback a docs/index.html
app.get(['/', '/index.html'], (req, res) => {
  const publicIndex = path.join(__dirname, '..', 'public', 'index.html');
  if (fs.existsSync(publicIndex)) return res.sendFile(publicIndex);
  const docsIndex = path.join(__dirname, '..', 'docs', 'index.html');
  if (fs.existsSync(docsIndex)) return res.sendFile(docsIndex);
  return res.status(404).send('index.html no encontrado');
});

// Serve modelo.html desde /modelo y /modelo.html
app.get(['/modelo', '/modelo.html'], (req, res) => {
  const page = path.join(__dirname, '..', 'public', 'modelo.html');
  if (fs.existsSync(page)) return res.sendFile(page);
  return res.status(404).send('public/modelo.html no encontrado');
});

// Serve seleccion.html desde /seleccion y /seleccion.html
app.get(['/seleccion', '/seleccion.html'], (req, res) => {
  const page = path.join(__dirname, '..', 'public', 'seleccion.html');
  if (fs.existsSync(page)) return res.sendFile(page);
  return res.status(404).send('public/seleccion.html no encontrado');
});

// Ruta a mensaje: redirige al servicio Python (puerto 5000)
app.get(['/mensaje', '/mensaje.html'], (req, res) => {
  const target = `http://localhost:5000/public/mensaje.html`;
  return res.redirect(target);
});

// Página de chat SSE clásica en /chat
app.get(['/chat', '/chat.html'], (req, res) => {
  const publicIndex = path.join(__dirname, '..', 'public', 'index.html');
  if (fs.existsSync(publicIndex)) return res.sendFile(publicIndex);
  return res.status(404).send('public/index.html no encontrado');
});

// 404
app.use((req, res) => {
  res.status(404).json({ error: 'not found' });
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat backend escuchando en http://localhost:${PORT}`);
  console.log('Endpoints:');
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
