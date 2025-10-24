# db_api.py
import sqlite3
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# Función de conexión a la base de datos (puede ser reemplazada para otros gestores)
def get_db_connection(data_db='dev.db'):
    """Función de conexión que puede ser reemplazada para otros gestores de BD"""
    conn = sqlite3.connect(data_db)
    conn.row_factory = sqlite3.Row  # Para obtener resultados como diccionarios
    return conn

@app.route('/db/read', methods=['GET'])
def db_read():
    """Ejecuta consultas SELECT y retorna resultados"""
    text_sql = request.args.get('text_sql')
    data_db = request.args.get('data_db', 'dev.db')
    
    if not text_sql:
        return jsonify({'error': 'Se requiere el parámetro text_sql'}), 400
    
    try:
        conn = get_db_connection(data_db)
        cursor = conn.cursor()
        cursor.execute(text_sql)
        results = cursor.fetchall()
        
        # Convertir resultados a lista de diccionarios
        rows = [dict(row) for row in results]
        conn.close()
        
        return jsonify({'success': True, 'data': rows, 'count': len(rows)})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/db/exe', methods=['POST'])
def db_exe():
    """Ejecuta consultas que no retornan datos (CREATE, INSERT, UPDATE, DELETE)"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Se requiere JSON en el body'}), 400
    
    text_sql = data.get('text_sql')
    data_db = data.get('data_db', 'dev.db')
    
    if not text_sql:
        return jsonify({'error': 'Se requiere el parámetro text_sql'}), 400
    
    try:
        conn = get_db_connection(data_db)
        cursor = conn.cursor()
        cursor.execute(text_sql)
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Consulta ejecutada correctamente'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/db/update', methods=['POST'])
def db_update():
    """Actualiza registros en una tabla"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Se requiere JSON en el body'}), 400
    
    tabla = data.get('tabla')
    campo = data.get('campo')
    valor = data.get('valor')
    condicion_sql = data.get('condicion_sql')
    data_db = data.get('data_db', 'dev.db')
    
    if not all([tabla, campo, valor is not None, condicion_sql]):
        return jsonify({'error': 'Se requieren tabla, campo, valor y condicion_sql'}), 400
    
    # Construir la consulta SQL usando db_exe internamente
    text_sql = f"UPDATE {tabla} SET {campo} = ? WHERE {condicion_sql}"
    
    try:
        # Usar db_exe internamente
        conn = get_db_connection(data_db)
        cursor = conn.cursor()
        cursor.execute(text_sql, (valor,))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Registro actualizado correctamente'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/db/insert', methods=['POST'])
def db_insert():
    """Inserta un nuevo registro en una tabla"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Se requiere JSON en el body'}), 400
    
    tabla = data.get('tabla')
    json_valores = data.get('json_valores')
    data_db = data.get('data_db', 'dev.db')
    
    if not tabla or not json_valores:
        return jsonify({'error': 'Se requieren tabla y json_valores'}), 400
    
    try:
        # Si json_valores es string, convertirlo a dict
        if isinstance(json_valores, str):
            valores_dict = json.loads(json_valores)
        else:
            valores_dict = json_valores
        
        # Construir la consulta SQL
        campos = ', '.join(valores_dict.keys())
        placeholders = ', '.join(['?' for _ in valores_dict])
        valores = list(valores_dict.values())
        
        text_sql = f"INSERT INTO {tabla} ({campos}) VALUES ({placeholders})"
        
        # Usar db_exe internamente
        conn = get_db_connection(data_db)
        cursor = conn.cursor()
        cursor.execute(text_sql, valores)
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Registro insertado correctamente'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/db/delete', methods=['POST'])
def db_delete():
    """Elimina registros basados en una condición SQL"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Se requiere JSON en el body'}), 400
    
    tabla = data.get('tabla')
    condicion_sql = data.get('condicion_sql')
    data_db = data.get('data_db', 'dev.db')
    
    if not tabla or not condicion_sql:
        return jsonify({'error': 'Se requieren tabla y condicion_sql'}), 400
    
    # Construir la consulta SQL usando db_exe internamente
    text_sql = f"DELETE FROM {tabla} WHERE {condicion_sql}"
    
    try:
        # Usar db_exe internamente
        conn = get_db_connection(data_db)
        cursor = conn.cursor()
        cursor.execute(text_sql)
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Registros eliminados correctamente'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/db/delete_pk', methods=['POST'])
def db_delete_pk():
    """Elimina un registro basado en su clave primaria"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Se requiere JSON en el body'}), 400
    
    tabla = data.get('tabla')
    pk = data.get('pk')
    valor = data.get('valor')
    data_db = data.get('data_db', 'dev.db')
    
    if not all([tabla, pk, valor is not None]):
        return jsonify({'error': 'Se requieren tabla, pk y valor'}), 400
    
    # Construir la consulta SQL usando db_exe internamente
    text_sql = f"DELETE FROM {tabla} WHERE {pk} = ?"
    
    try:
        # Usar db_exe internamente
        conn = get_db_connection(data_db)
        cursor = conn.cursor()
        cursor.execute(text_sql, (valor,))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Registro eliminado correctamente'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)