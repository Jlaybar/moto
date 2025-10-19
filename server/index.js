import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { PrismaClient } from '@prisma/client';

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Ensure SQLite path exists and set default DATABASE_URL if missing
const dataDir = path.join(process.cwd(), 'prisma');
const dbFile = path.join(dataDir, 'dev.db');
try { fs.mkdirSync(dataDir, { recursive: true }); } catch {}
if (!process.env.DATABASE_URL) {
  process.env.DATABASE_URL = 'file:' + dbFile.replace(/\\/g, '/');
}

// SSE clients
const CLIENTS = new Set();
const app = express();
const prisma = new PrismaClient();

// CORS middleware (simple, sin dependencia externa)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: '1mb' }));

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

// Prisma-based user endpoints
app.get('/users', async (req, res) => {
  try {
    const users = await prisma.user.findMany({ select: { id: true, email: true, createdAt: true } });
    res.json(users);
  } catch (e) {
    res.status(500).json({ error: 'db_error', detail: String(e?.message || e) });
  }
});

app.get('/users/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
  try {
    const user = await prisma.user.findUnique({ where: { id }, select: { id: true, email: true, createdAt: true } });
    if (!user) return res.status(404).json({ error: 'not found' });
    res.json(user);
  } catch (e) {
    res.status(500).json({ error: 'db_error', detail: String(e?.message || e) });
  }
});

app.post('/users', async (req, res) => {
  const email = String(req.body?.email || '').trim();
  const password = String(req.body?.password || '').trim();
  if (!email || !password) return res.status(400).json({ error: 'email/password requeridos' });
  try {
    const user = await prisma.user.create({ data: { email, password } });
    res.status(201).json({ id: user.id, email: user.email, createdAt: user.createdAt });
  } catch (e) {
    res.status(500).json({ error: 'db_error', detail: String(e?.message || e) });
  }
});

// DB health and quick stats
app.get('/db/health', async (req, res) => {
  try {
    // Lightweight connectivity check that works on SQLite
    await prisma.$queryRaw`SELECT 1`;
    res.json({ ok: true, url: process.env.DATABASE_URL || null });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'db_error', detail: String(e?.message || e) });
  }
});

app.get('/users/count', async (req, res) => {
  try {
    const count = await prisma.user.count();
    res.json({ count });
  } catch (e) {
    res.status(500).json({ error: 'db_error', detail: String(e?.message || e) });
  }
});

app.delete('/users/:id', async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
  try {
    await prisma.user.delete({ where: { id } });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'db_error', detail: String(e?.message || e) });
  }
});

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
  console.log('  GET    /users');
  console.log('  GET    /users/:id');
  console.log('  POST   /users    {"email","password"}');
  console.log('  DELETE /users/:id');
});

// Graceful shutdown
process.on('SIGINT', async () => { await prisma.$disconnect(); server.close(() => process.exit(0)); });
process.on('SIGTERM', async () => { await prisma.$disconnect(); server.close(() => process.exit(0)); });

