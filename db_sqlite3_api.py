
# db_api.py
# Flask API for executing SQLite (or pluggable DB) operations.
# NOTE: This exposes raw SQL endpoints. Use carefully and secure behind auth in production.

from __future__ import annotations

import os
import re
import sqlite3
from typing import Any, Dict, List, Tuple, Optional

from flask import Flask, request, jsonify

app = Flask(__name__)

# ------------------------------
# Config & Pluggable Connection
# ------------------------------

# Environment variables (defaults for SQLite)
DB_BACKEND = os.getenv("DB_BACKEND", "sqlite").lower()
SQLITE_PATH = os.getenv("DB_FILE_PATH", "dev.db")

def get_db_connection(db_path: Optional[str] = None):
    """
    Connection factory. Replace/extend this to support other DB engines.
    Currently supports SQLite.

    For other engines, implement branches like:
    - if DB_BACKEND == "postgres": return psycopg2.connect(...)
    - if DB_BACKEND == "mysql": return pymysql.connect(...)
    """
    backend = DB_BACKEND

    if backend == "sqlite":
        path = db_path or SQLITE_PATH
        # ensure parent exists if relative
        return sqlite3.connect(path)
    else:
        raise NotImplementedError(f"DB_BACKEND '{backend}' not implemented yet.")

# ------------------------------
# Helpers
# ------------------------------

_IDENTIFIER_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def validate_identifier(name: str, what: str = "identifier") -> str:
    """
    Basic whitelist validation for table/column identifiers to reduce SQL injection risk.
    Does NOT validate SQL expressions like condition clauses.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RX.match(name):
        raise ValueError(f"Invalid {what}: {name!r}")
    return name

def rows_to_dicts(cursor, rows: List[Tuple]) -> List[Dict[str, Any]]:
    columns = [col[0] for col in cursor.description] if cursor.description else []
    result = []
    for row in rows:
        if columns:
            result.append({col: row[idx] for idx, col in enumerate(columns)})
        else:
            # No columns (e.g., PRAGMA or statements without rows)
            result.append({"value": row})
    return result

# ------------------------------
# Core DB Call Utilities (SQLite direct)
# ------------------------------

# ------------------------------
# Higher-level Helpers (sqlite3 direct)
# ------------------------------

def db_tables(data_db: str = SQLITE_PATH) -> List[str]:
    """
    Devuelve el listado de tablas de la base de datos (excluyendo tablas internas de SQLite).
    """
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def db_table_schema(tabla: str, data_db: str = SQLITE_PATH) -> List[Dict[str, Any]]:
    """
    Devuelve la lista de campos (columnas) de una tabla.
    Retorna una lista de diccionarios con: cid, name, type, notnull, dflt_value, pk.
    """
    t = validate_identifier(tabla, "table name")
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({t})")
        rows = cur.fetchall()
        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        return [
            {
                "cid": r[0],
                "name": r[1],
                "type": r[2],
                "notnull": bool(r[3]),
                "dflt_value": r[4],
                "pk": bool(r[5]),
            }
            for r in rows
        ]
    finally:
        conn.close()

def db_update(tabla: str, campo: str, valor: Any, condicion_sql: str, data_db: str = SQLITE_PATH) -> Dict[str, Any]:
    """
    UPDATE <tabla> SET <campo> = ? WHERE <condicion_sql>
    """
    t = validate_identifier(tabla, "table name")
    c = validate_identifier(campo, "column name")
    # NOTE: condicion_sql is treated as a raw SQL clause.
    sql = f"UPDATE {t} SET {c} = ? WHERE {condicion_sql}"
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(sql, (valor,))
        conn.commit()
        res = {"rowcount": cur.rowcount}
    finally:
        conn.close()
    return res

def db_insert(tabla: str, json_valores: Dict[str, Any], data_db: str = SQLITE_PATH) -> Dict[str, Any]:
    """
    INSERT INTO <tabla> (<cols...>) VALUES (<placeholders...>)
    """
    t = validate_identifier(tabla, "table name")
    if not isinstance(json_valores, dict) or not json_valores:
        raise ValueError("json_valores debe ser un objeto con al menos una clave.")
    cols = [validate_identifier(k, "column name") for k in json_valores.keys()]
    vals = list(json_valores.values())
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT INTO {t} ({col_list}) VALUES ({placeholders})"
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(sql, vals)
        conn.commit()
        res = {"rowcount": cur.rowcount, "lastrowid": getattr(cur, "lastrowid", None)}
    finally:
        conn.close()
    return res

def db_delete(tabla: str, condicion_sql: str, data_db: str = SQLITE_PATH) -> Dict[str, Any]:
    """
    DELETE FROM <tabla> WHERE <condicion_sql>
    """
    t = validate_identifier(tabla, "table name")
    sql = f"DELETE FROM {t} WHERE {condicion_sql}"
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        res = {"rowcount": cur.rowcount}
    finally:
        conn.close()
    return res

def db_delete_pk(tabla: str, pk: str, valor: Any, data_db: str = SQLITE_PATH) -> Dict[str, Any]:
    """
    DELETE FROM <tabla> WHERE <pk> = ?
    """
    t = validate_identifier(tabla, "table name")
    p = validate_identifier(pk, "primary key column")
    sql = f"DELETE FROM {t} WHERE {p} = ?"
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(sql, (valor,))
        conn.commit()
        res = {"rowcount": cur.rowcount}
    finally:
        conn.close()
    return res

# ------------------------------
# Flask Routes
# ------------------------------
@app.route("/db/update", methods=["POST"])
def route_db_update():
    """
    POST /db/update
    JSON: {"tabla": "...", "campo": "...", "valor": <any>, "condicion_sql": "id = 1", "db": "<optional_db_path>"}
    """
    payload = request.get_json(silent=True) or {}
    try:
        tabla = payload["tabla"]
        campo = payload["campo"]
        valor = payload["valor"]
        condicion_sql = payload["condicion_sql"]
        data_db = payload.get("db", SQLITE_PATH)
    except KeyError as ke:
        return jsonify({"error": f"Falta el campo requerido: {ke}"}), 400
    try:
        result = db_update(tabla, campo, valor, condicion_sql, data_db=data_db)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/db/insert", methods=["POST"])
def route_db_insert():
    """
    POST /db/insert
    JSON: {"tabla": "...", "valores": {"col1": v1, "col2": v2, ...}, "db": "<optional_db_path>"}
    """
    payload = request.get_json(silent=True) or {}
    try:
        tabla = payload["tabla"]
        valores = payload["valores"]
        data_db = payload.get("db", SQLITE_PATH)
    except KeyError as ke:
        return jsonify({"error": f"Falta el campo requerido: {ke}"}), 400
    try:
        result = db_insert(tabla, valores, data_db=data_db)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/db/delete", methods=["POST"])
def route_db_delete():
    """
    POST /db/delete
    JSON: {"tabla": "...", "condicion_sql": "id > 5", "db": "<optional_db_path>"}
    """
    payload = request.get_json(silent=True) or {}
    try:
        tabla = payload["tabla"]
        condicion_sql = payload["condicion_sql"]
        data_db = payload.get("db", SQLITE_PATH)
    except KeyError as ke:
        return jsonify({"error": f"Falta el campo requerido: {ke}"}), 400
    try:
        result = db_delete(tabla, condicion_sql, data_db=data_db)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/db/delete_pk", methods=["POST"])
def route_db_delete_pk():
    """
    POST /db/delete_pk
    JSON: {"tabla": "...", "pk": "...", "valor": <any>, "db": "<optional_db_path>"}
    """
    payload = request.get_json(silent=True) or {}
    try:
        tabla = payload["tabla"]
        pk = payload["pk"]
        valor = payload["valor"]
        data_db = payload.get("db", SQLITE_PATH)
    except KeyError as ke:
        return jsonify({"error": f"Falta el campo requerido: {ke}"}), 400
    try:
        result = db_delete_pk(tabla, pk, valor, data_db=data_db)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Health check
@app.route("/health", methods=["GET"])
def health():
    try:
        # Try opening a connection
        conn = get_db_connection(SQLITE_PATH)
        conn.close()
        return jsonify({"status": "ok", "backend": DB_BACKEND, "db": SQLITE_PATH})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # For local testing
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
