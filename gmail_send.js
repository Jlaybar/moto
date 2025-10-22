import 'dotenv/config';
import { google } from 'googleapis';

// Lee exclusivamente desde .env (sin placeholders). Falla si faltan.
const CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.GMAIL_REFRESH_TOKEN; // Necesario para CLI
const REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/auth/callback';

function assertEnv(name) {
  if (!process.env[name]) {
    console.error(`Falta variable en .env: ${name}`);
    process.exit(1);
  }
}

assertEnv('GOOGLE_CLIENT_ID');
assertEnv('GOOGLE_CLIENT_SECRET');
assertEnv('GMAIL_REFRESH_TOKEN');

const oAuth2Client = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI);
oAuth2Client.setCredentials({ refresh_token: REFRESH_TOKEN });

async function sendMail() {
  try {
    const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });

    // Personaliza estos valores o pásalos vía env si prefieres
    const to = process.env.GMAIL_TO || 'jlaybar@dominio.com';
    const subject = process.env.GMAIL_SUBJECT || 'Prueba desde Node CLI';
    const body = process.env.GMAIL_BODY || 'Hola! 👋 Este correo fue enviado desde Node.js usando la API de Gmail.';

    const messageParts = [
      `To: ${to}`,
      `Subject: ${subject}`,
      'Content-Type: text/plain; charset=utf-8',
      'MIME-Version: 1.0',
      '',
      body,
    ];

    const encodedMessage = Buffer.from(messageParts.join('\r\n'))
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');

    const result = await gmail.users.messages.send({
      userId: 'me',
      requestBody: { raw: encodedMessage },
    });

    console.log('✅ Enviado:', result.data.id);
  } catch (err) {
    const detail = err?.response?.data || err?.message || String(err);
    console.error('❌ Error enviando correo:', detail);
    process.exitCode = 1;
  }
}

sendMail();
