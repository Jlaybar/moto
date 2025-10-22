import 'dotenv/config';
import { google } from 'googleapis';

// Lee de .env o usa placeholders para que los rellenes
const CLIENT_ID = process.env.GOOGLE_CLIENT_ID || 'TU_CLIENT_ID.apps.googleusercontent.com';
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || 'TU_CLIENT_SECRET';
const REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/auth/callback';
const REFRESH_TOKEN = process.env.GMAIL_REFRESH_TOKEN || 'TU_REFRESH_TOKEN'; // Necesario para CLI

const oAuth2Client = new google.auth.OAuth2(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI);
oAuth2Client.setCredentials({ refresh_token: REFRESH_TOKEN });

async function sendMail() {
  try {
    const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });

    // Personaliza estos valores o pásalos vía env si prefieres
    const to = process.env.GMAIL_TO || 'destinatario@dominio.com';
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

