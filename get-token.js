import 'dotenv/config';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { google } from 'googleapis';

function assertEnv(name) {
  if (!process.env[name]) {
    console.error(`Falta variable en .env: ${name}`);
    process.exit(1);
  }
}

// Requisitos
assertEnv('GOOGLE_CLIENT_ID');
assertEnv('GOOGLE_CLIENT_SECRET');

const CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/auth/callback';

// Scopes por defecto: solo envío. Puedes ampliar si quieres leer: gmail.readonly
const SCOPES = (process.env.GMAIL_SCOPES
  ? process.env.GMAIL_SCOPES.split(',').map(s => s.trim()).filter(Boolean)
  : ['https://www.googleapis.com/auth/gmail.send']
);

const oAuth2Client = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI);

const loginHint = process.env.GMAIL_LOGIN_HINT; // opcional: fuerza sugerencia de cuenta
const url = oAuth2Client.generateAuthUrl({
  access_type: 'offline',
  prompt: 'consent select_account',
  scope: SCOPES,
  response_type: 'code',
  include_granted_scopes: true,
  ...(loginHint ? { login_hint: loginHint } : {}),
});

console.log('1) Abre esta URL en tu navegador y autoriza:');
console.log(url + '\n');

function openInBrowser(targetUrl) {
  try {
    if (process.platform === 'win32') {
      // En Windows, & y otros metacaracteres rompen la URL si no se entrecomilla
      const quoted = '"' + targetUrl + '"';
      spawn('cmd', ['/c', 'start', '', quoted], { stdio: 'ignore', detached: true, windowsVerbatimArguments: true });
    } else if (process.platform === 'darwin') {
      spawn('open', [targetUrl], { stdio: 'ignore', detached: true });
    } else {
      spawn('xdg-open', [targetUrl], { stdio: 'ignore', detached: true });
    }
  } catch (e) {
    console.warn('No se pudo abrir el navegador automáticamente:', e?.message || e);
  }
}

// Si el REDIRECT_URI apunta a localhost, montamos un servidor temporal para capturar el callback
const redirect = new URL(REDIRECT_URI);
const isLocal = ['localhost', '127.0.0.1'].includes(redirect.hostname);

if (!isLocal) {
  // Intento de apertura automática aunque no sea localhost
  openInBrowser(url);
  console.log('Tu REDIRECT_URI no apunta a localhost. Copia el "code" de la URL final tras autorizar,');
  console.log('y pégalo aquí para continuar.');
  process.stdout.write('\nIntroduce el code: ');
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', async (chunk) => {
    const code = chunk.toString().trim();
    if (!code) return;
    try {
      const { tokens } = await oAuth2Client.getToken(code);
      console.log('\nTokens obtenidos:');
      console.log(JSON.stringify(tokens, null, 2));
      if (tokens.refresh_token) {
        console.log('\nAñade a tu .env:');
        console.log(`GMAIL_REFRESH_TOKEN="${tokens.refresh_token}"`);
      } else {
        console.log('\nNo se recibió refresh_token. Asegúrate de usar prompt=consent y access_type=offline, y que no haya sido ya concedido antes.');
      }
      process.exit(0);
    } catch (e) {
      console.error('Error intercambiando el code:', e?.response?.data || e?.message || String(e));
      process.exit(1);
    }
  });
} else {
  const server = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url && req.url.startsWith(redirect.pathname)) {
      const urlObj = new URL(req.url, `${redirect.protocol}//${redirect.host}`);
      const code = urlObj.searchParams.get('code');
      if (!code) {
        res.statusCode = 400;
        res.end('Falta code en la URL');
        return;
      }
      try {
        const { tokens } = await oAuth2Client.getToken(code);
        res.statusCode = 200;
        res.setHeader('Content-Type', 'text/plain; charset=utf-8');
        res.end('Autorización completada. Puedes cerrar esta ventana. Revisa la terminal.');
        console.log('\nTokens obtenidos:');
        console.log(JSON.stringify(tokens, null, 2));
        if (tokens.refresh_token) {
          console.log('\nAñade a tu .env:');
          console.log(`GMAIL_REFRESH_TOKEN="${tokens.refresh_token}"`);
        } else {
          console.log('\nNo se recibió refresh_token. Asegúrate de usar prompt=consent y access_type=offline, y que no haya sido ya concedido antes.');
        }
      } catch (e) {
        console.error('Error intercambiando el code:', e?.response?.data || e?.message || String(e));
      } finally {
        server.close(() => process.exit(0));
      }
    } else {
      res.statusCode = 404;
      res.end('Not Found');
    }
  });

  const port = Number(redirect.port || (redirect.protocol === 'https:' ? 443 : 80));
  server.listen(port, redirect.hostname, () => {
    console.log(`2) Esperando el callback en ${redirect.href}`);
    console.log('(Asegúrate de que no esté corriendo tu servidor en ese puerto durante este proceso)');
    // Abrir navegador cuando el listener ya está arriba
    openInBrowser(url);
  });
}
