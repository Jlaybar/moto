#!/usr/bin/env python3
"""
gmail_send.py - Envío de correos usando Gmail API
"""

import os
import sys
import base64
from email.mime.text import MIMEText

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Si modifica estos SCOPES, elimine el archivo token.json.
# Igualamos los scopes a los usados por el backend Node para evitar invalid_scope
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
]


def assert_env(name: str):
    """Verifica que una variable de entorno exista"""
    if not os.getenv(name):
        print(f'Falta variable en .env: {name}')
        sys.exit(1)


def load_credentials() -> Credentials:
    """Carga o genera credenciales para la API de Gmail"""
    creds = None

    # El archivo token.json almacena los tokens de acceso y actualización
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # Si no hay credenciales válidas, permite que el usuario inicie sesión
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Para uso en CLI con refresh token preexistente (no interactivo)
            refresh_token = os.getenv('GMAIL_REFRESH_TOKEN')
            client_id = os.getenv('GOOGLE_CLIENT_ID')
            client_secret = os.getenv('GOOGLE_CLIENT_SECRET')

            if refresh_token and client_id and client_secret:
                # Construir credenciales con refresh token sin declarar scopes
                creds = Credentials(
                    token=None,
                    refresh_token=refresh_token,
                    token_uri='https://oauth2.googleapis.com/token',
                    client_id=client_id,
                    client_secret=client_secret,
                )
                # Refrescar el token para obtener access_token
                creds.refresh(Request())
            else:
                # Flujo de autenticación interactivo (requiere credentials.json)
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            # Guarda las credenciales para la próxima ejecución
            with open('token.json', 'w', encoding='utf-8') as token:
                token.write(creds.to_json())

    return creds


def create_message(to: str, subject: str, body: str):
    """Crea un mensaje MIME para enviar"""
    message = MIMEText(body, 'plain', 'utf-8')
    message['to'] = to
    message['subject'] = subject
    return {
        'raw': base64.urlsafe_b64encode(message.as_bytes()).decode('ascii')
    }


def send_mail():
    """Envía un correo usando Gmail API"""
    try:
        # Cargar variables de entorno desde .env (si existe)
        load_dotenv()

        # Verificar variables de entorno requeridas cuando no existe token.json
        if not os.path.exists('token.json'):
            assert_env('GOOGLE_CLIENT_ID')
            assert_env('GOOGLE_CLIENT_SECRET')
            assert_env('GMAIL_REFRESH_TOKEN')

        # Cargar credenciales
        creds = load_credentials()

        # Crear servicio Gmail
        service = build('gmail', 'v1', credentials=creds)

        # Personaliza estos valores o pásalos vía variables de entorno
        to = os.getenv('GMAIL_TO', os.getenv('GMAIL_USER', 'destinatario@example.com'))
        subject = os.getenv('GMAIL_SUBJECT', 'Prueba desde Python CLI')
        body = os.getenv('GMAIL_BODY', 'Hola! Este correo fue enviado desde Python usando la API de Gmail.')

        # Crear y enviar mensaje
        message = create_message(to, subject, body)
        result = service.users().messages().send(userId='me', body=message).execute()

        print(f'Enviado. ID: {result["id"]}')

    except HttpError as err:
        error_detail = err.content.decode('utf-8') if getattr(err, 'content', None) else str(err)
        print(f'Error enviando correo: {error_detail}')
        sys.exit(1)
    except Exception as err:
        print(f'Error inesperado: {str(err)}')
        sys.exit(1)


if __name__ == '__main__':
    send_mail()
