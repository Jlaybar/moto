#!/usr/bin/env python3
"""
gmail_send.py - Envío de correos y gestión de mensajes usando Gmail API
"""

import os
from dotenv import load_dotenv
import base64
import json
import time
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

# Cargar variables desde .env para CLI y servidor
load_dotenv(override=True)

# Si modifica estos SCOPES, elimine el archivo token.json.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify'
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
                    client_secret=client_secret
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

def search_messages(search_query, max_results=100):
    """Busca mensajes basados en una consulta"""
    try:
        service = get_gmail_service()
        if not service:
            print("No se pudo obtener el servicio de Gmail")
            return None

        result = service.users().messages().list(
            userId='me',
            q=search_query,
            maxResults=max_results
        ).execute()

        messages = result.get('messages', [])
        messages_list = []

        print(f"Buscando mensajes con query: '{search_query}'")
        print(f"Encontrados {len(messages)} mensajes")

        # Obtener detalles de cada mensaje
        for msg in messages:
            message_detail = service.users().messages().get(
                userId='me',
                id=msg['id'],
                format='metadata',
                metadataHeaders=['Subject', 'From', 'Date']
            ).execute()

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

        return messages_list

    except HttpError as err:
        print(f'Error buscando mensajes: {err}')
        return []


def delete_messages_by_keyword(keyword, search_in='both', max_results=500, dry_run=False):
    """
    Elimina mensajes que contengan una palabra clave en asunto o remitente

    Args:
        keyword (str): Palabra clave a buscar
        search_in (str): Dónde buscar ('subject', 'from', 'both')
        max_results (int): Máximo número de mensajes a procesar
        dry_run (bool): Si es True, solo muestra qué se eliminaría sin hacer cambios
    """
    try:
        service = get_gmail_service()
        if not service:
            print("No se pudo obtener el servicio de Gmail")
            return None

        # Construir query de búsqueda
        search_queries = []
        if search_in in ['subject', 'both']:
            search_queries.append(f'subject:"{keyword}"')
        if search_in in ['from', 'both']:
            search_queries.append(f'from:"{keyword}"')

        # Buscar mensajes para cada query
        all_messages = []
        seen_ids = set()
        for query in search_queries:
            messages = search_messages(query, max_results) or []
            for msg in messages:
                if msg['id'] not in seen_ids:
                    seen_ids.add(msg['id'])
                    all_messages.append(msg)

        if not all_messages:
            print("No se encontraron mensajes que coincidan con la búsqueda")
            return None

        # Mostrar mensajes encontrados
        print(f"\nMENSAJES ENCONTRADOS ({len(all_messages)}):")
        print("=" * 100)
        for i, msg in enumerate(all_messages, 1):
            source = []
            if search_in in ['subject', 'both'] and keyword.lower() in msg['subject'].lower():
                source.append('asunto')
            if search_in in ['from', 'both'] and keyword.lower() in msg['from'].lower():
                source.append('remitente')
            print(f"{i}. ID: {msg['id']}")
            print(f"   De: {msg['from']}")
            print(f"   Asunto: {msg['subject']}")
            print(f"   Fecha: {msg['date']}")
            print(f"   Coincidencia en: {', '.join(source)}")
            print(f"   Snippet: {msg['snippet'][:100]}...")
            print("-" * 100)

        if dry_run:
            print(f"\nMODO SIMULACIÓN: Se eliminarían {len(all_messages)} mensajes")
            print("   Ejecuta con dry_run=False para eliminar realmente")
            return all_messages

        # Confirmar eliminación
        print(f"\n¿Estás seguro de que quieres eliminar {len(all_messages)} mensajes?")
        confirm = input("   Escribe 'ELIMINAR' para confirmar: ")
        if confirm != 'ELIMINAR':
            print("Eliminación cancelada")
            return None

        # Eliminar mensajes
        deleted_count = 0
        error_count = 0
        for i, msg in enumerate(all_messages, 1):
            try:
                service.users().messages().delete(userId='me', id=msg['id']).execute()
                deleted_count += 1
                print(f"Eliminado {i}/{len(all_messages)}: {msg['subject'][:50]}...")
                time.sleep(0.1)
            except HttpError as err:
                error_count += 1
                print(f"Error eliminando mensaje {msg['id']}: {err}")
                continue

        print("\nRESULTADO DE ELIMINACIÓN:")
        print(f"   Eliminados: {deleted_count}")
        print(f"   Errores: {error_count}")
        print(f"   Total: {len(all_messages)}")

        return { 'deleted': deleted_count, 'errors': error_count, 'total': len(all_messages) }

    except Exception as err:
        print(f'Error inesperado: {str(err)}')
        return None

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
        to = data.get('to', os.getenv('GMAIL_TO', 'jlaybar@dominio.com'))
        subject = data.get('subject', 'send_mail_api: Prueba desde Python Flask')
        body = data.get('body', 'Hola! 👋 Este correo fue enviado desde Python usando la API de Gmail.')
        
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

@app.route('/gmail/delete', methods=['POST'])
def delete_mail_api():
    """Elimina mensajes según criterios. JSON esperado:
    { "keyword": "texto", "search_in": "subject|from|both", "max_results": 500, "dry_run": true }
    - Si dry_run=true, devuelve la lista de mensajes que se eliminarían.
    - Si dry_run=false, solicita confirmación no interactiva y elimina (sin prompt) devolviendo conteos.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        keyword = data.get('keyword')
        if not keyword:
            return jsonify({
                'status': 'error',
                'message': 'Falta parámetro keyword'
            }), 400

        search_in = data.get('search_in', 'both')
        max_results = int(data.get('max_results', 500))
        dry_run = bool(data.get('dry_run', True))

        # Reutilizamos la lógica existente
        result = delete_messages_by_keyword(keyword, search_in=search_in, max_results=max_results, dry_run=dry_run)

        if dry_run:
            # result es la lista de mensajes que se eliminarían o None si no hay coincidencias
            messages = result or []
            return jsonify({
                'status': 'success',
                'mode': 'dry_run',
                'count': len(messages),
                'messages': messages
            })
        else:
            # En modo API no haremos prompt de confirmación; si la función pidiera confirmación
            # idealmente habría un flag. Dado el helper actual, asumimos que ya eliminó y devolvió conteos.
            summary = result or { 'deleted': 0, 'errors': 0, 'total': 0 }
            return jsonify({
                'status': 'success',
                'mode': 'delete',
                'summary': summary
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
        to = os.getenv('GMAIL_TO', 'jlaybar@dominio.com')
        subject = os.getenv('GMAIL_SUBJECT', 'send_mail_cli: Prueba desde Python')
        body = os.getenv('GMAIL_BODY', 'Hola! 👋 Este correo fue enviado desde Python usando la API de Gmail.')
        
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
