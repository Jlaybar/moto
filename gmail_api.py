#!/usr/bin/env python3
"""
gmail_send.py - Envío de correos y gestión de mensajes usando Gmail API
"""

import os
import base64
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sys
from flask import Flask, jsonify, request

app = Flask(__name__)

# Si modifica estos SCOPES, elimine el archivo token.json.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly'
]

def assert_env(name):
    """Verifica que una variable de entorno exista"""
    if not os.getenv(name):
        print(f'Falta variable en .env: {name}')
        sys.exit(1)

def load_credentials():
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
            # Para uso en CLI con refresh token preexistente
            refresh_token = os.getenv('GMAIL_REFRESH_TOKEN')
            client_id = os.getenv('GOOGLE_CLIENT_ID')
            client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
            
            if refresh_token and client_id and client_secret:
                creds = Credentials(
                    token=None,
                    refresh_token=refresh_token,
                    token_uri='https://oauth2.googleapis.com/token',
                    client_id=client_id,
                    client_secret=client_secret,
                    scopes=SCOPES
                )
                # Refrescar el token
                creds.refresh(Request())
            else:
                # Flujo de autenticación interactivo
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Guarda las credenciales para la próxima ejecución
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    
    return creds

def get_gmail_service():
    """Obtiene el servicio de Gmail"""
    creds = load_credentials()
    return build('gmail', 'v1', credentials=creds)

def create_message(to, subject, body):
    """Crea un mensaje MIME para enviar"""
    message = MIMEText(body, 'plain', 'utf-8')
    message['to'] = to
    message['subject'] = subject
    return {
        'raw': base64.urlsafe_b64encode(
            message.as_bytes()
        ).decode('ascii')
    }

def decode_message_body(data):
    """Decodifica el cuerpo del mensaje desde base64"""
    if 'data' in data:
        return base64.urlsafe_b64decode(data['data']).decode('utf-8')
    return ''

def get_message_text_parts(parts):
    """Extrae el texto de las partes del mensaje"""
    text_parts = []
    for part in parts:
        if part['mimeType'] == 'text/plain':
            text_parts.append(decode_message_body(part['body']))
        elif part['mimeType'] == 'text/html':
            # Opcional: podrías convertir HTML a texto
            text_parts.append(decode_message_body(part['body']))
        elif 'parts' in part:
            # Mensajes multipart
            text_parts.extend(get_message_text_parts(part['parts']))
    return text_parts

# Endpoints de la API
@app.route('/gmail/messages', methods=['GET'])
def list_messages():
    """Lista los últimos mensajes del buzón"""
    try:
        service = get_gmail_service()
        
        # Parámetros opcionales
        max_results = request.args.get('maxResults', default=10, type=int)
        label_ids = request.args.get('labelIds', default='INBOX')
        
        # Obtener lista de mensajes
        result = service.users().messages().list(
            userId='me',
            maxResults=max_results,
            labelIds=label_ids
        ).execute()
        
        messages = result.get('messages', [])
        messages_list = []
        
        # Obtener detalles de cada mensaje
        for msg in messages:
            message_detail = service.users().messages().get(
                userId='me', 
                id=msg['id'],
                format='metadata',
                metadataHeaders=['Subject', 'From', 'Date']
            ).execute()
            
            # Extraer headers importantes
            headers = {h['name']: h['value'] for h in message_detail.get('payload', {}).get('headers', [])}
            
            messages_list.append({
                'id': msg['id'],
                'threadId': msg.get('threadId'),
                'snippet': message_detail.get('snippet', ''),
                'subject': headers.get('Subject', ''),
                'from': headers.get('From', ''),
                'date': headers.get('Date', ''),
                'labelIds': message_detail.get('labelIds', [])
            })
        
        return jsonify({
            'status': 'success',
            'count': len(messages_list),
            'messages': messages_list
        })
        
    except HttpError as err:
        return jsonify({
            'status': 'error',
            'message': f'Error de Gmail API: {err}'
        }), 500
    except Exception as err:
        return jsonify({
            'status': 'error',
            'message': f'Error inesperado: {str(err)}'
        }), 500

@app.route('/gmail/messages/<message_id>', methods=['GET'])
def get_message(message_id):
    """Obtiene el cuerpo completo de un mensaje específico"""
    try:
        service = get_gmail_service()
        
        # Obtener mensaje completo
        message = service.users().messages().get(
            userId='me', 
            id=message_id,
            format='full'
        ).execute()
        
        # Extraer headers
        headers = {h['name']: h['value'] for h in message.get('payload', {}).get('headers', [])}
        
        # Extraer cuerpo del mensaje
        body_text = ''
        payload = message.get('payload', {})
        
        if 'parts' in payload:
            # Mensaje multipart
            text_parts = get_message_text_parts(payload['parts'])
            body_text = '\n'.join(text_parts)
        elif 'body' in payload and 'data' in payload['body']:
            # Mensaje simple
            body_text = decode_message_body(payload['body'])
        
        response_data = {
            'status': 'success',
            'message': {
                'id': message['id'],
                'threadId': message.get('threadId'),
                'snippet': message.get('snippet', ''),
                'subject': headers.get('Subject', ''),
                'from': headers.get('From', ''),
                'to': headers.get('To', ''),
                'date': headers.get('Date', ''),
                'body': body_text,
                'labelIds': message.get('labelIds', [])
            }
        }
        
        return jsonify(response_data)
        
    except HttpError as err:
        return jsonify({
            'status': 'error',
            'message': f'Error de Gmail API: {err}'
        }), 500
    except Exception as err:
        return jsonify({
            'status': 'error',
            'message': f'Error inesperado: {str(err)}'
        }), 500

@app.route('/gmail/send', methods=['POST'])
def send_mail_api():
    """Envía un correo a través de la API"""
    try:
        service = get_gmail_service()
        
        # Obtener datos del request
        data = request.get_json()
        to = data.get('to', os.getenv('GMAIL_TO', 'jlaybar@gmail.com'))
        subject = data.get('subject', 'Prueba desde Python API')
        body = data.get('body', 'Hola1 👋 Este correo fue enviado desde Python usando la API de Gmail.')
        
        # Crear y enviar mensaje
        message = create_message(to, subject, body)
        result = service.users().messages().send(
            userId='me', 
            body=message
        ).execute()
        
        return jsonify({
            'status': 'success',
            'message_id': result['id'],
            'message': 'Correo enviado exitosamente'
        })
        
    except HttpError as err:
        return jsonify({
            'status': 'error',
            'message': f'Error enviando correo: {err}'
        }), 500
    except Exception as err:
        return jsonify({
            'status': 'error',
            'message': f'Error inesperado: {str(err)}'
        }), 500

# Función CLI original (para compatibilidad)
def send_mail_cli():
    """Envía un correo usando Gmail API (modo CLI)"""
    try:
        # Verificar variables de entorno requeridas
        assert_env('GOOGLE_CLIENT_ID')
        assert_env('GOOGLE_CLIENT_SECRET')
        assert_env('GMAIL_REFRESH_TOKEN')
        
        service = get_gmail_service()
        
        # Personaliza estos valores o pásalos vía env si prefieres
        to = os.getenv('GMAIL_TO', 'jlaybar@gmail.com')
        subject = os.getenv('GMAIL_SUBJECT', 'Prueba desde Python CLI')
        body = os.getenv('GMAIL_BODY', 'Hola2 👋 Este correo fue enviado desde Python usando la API de Gmail.')
        
        # Crear y enviar mensaje
        message = create_message(to, subject, body)
        result = service.users().messages().send(
            userId='me', 
            body=message
        ).execute()
        
        print(f'✅ Enviado: {result["id"]}')
        
    except HttpError as err:
        error_detail = err.content.decode('utf-8') if err.content else str(err)
        print(f'❌ Error enviando correo: {error_detail}')
        sys.exit(1)
    except Exception as err:
        print(f'❌ Error inesperado: {str(err)}')
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'cli':
        # Modo CLI: python gmail_send.py cli
        send_mail_cli()
    else:
        # Modo servidor web
        print("🚀 Servidor Gmail API iniciado en http://localhost:5000")
        print("📧 Endpoints disponibles:")
        print("   GET  /gmail/messages          (listar últimos mensajes)")
        print("   GET  /gmail/messages/:id      (obtener cuerpo del mensaje)")
        print("   POST /gmail/send              (enviar correo)")
        app.run(debug=True, host='0.0.0.0', port=5000)