
# db_api.py
# Flask API for executing SQLite (or pluggable DB) operations.
# NOTE: This exposes raw SQL endpoints. Use carefully and secure behind auth in production.

from __future__ import annotations

import os
import re
import json
import sqlite3
import pandas as pd
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
        # Configure connection to reduce 'database is locked' errors under concurrency
        # - timeout: wait for locks
        # - PRAGMAs: WAL journal for readers+writes, busy_timeout for extra safety, FK enforcement
        conn = sqlite3.connect(path, timeout=float(os.getenv("DB_TIMEOUT", "10")))
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            # Ignore PRAGMA failures; keep connection usable
            pass
        return conn
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

def db_read(
    tabla: str,
    campos: Optional[List[str]] = None,
    condicion_sql: Optional[str] = None,
    data_db: str = SQLITE_PATH,
) -> Dict[str, Any]:
    """
    SELECT <campos> FROM <tabla> [WHERE <condicion_sql>]
    - tabla: nombre de la tabla
    - campos: lista de columnas o None/'*' para todas
    - condicion_sql: cláusula WHERE cruda (sin "WHERE")
    """
    t = validate_identifier(tabla, "table name")
    # Build column list
    if not campos or (isinstance(campos, list) and len(campos) == 0):
        col_expr = "*"
    else:
        # Allow caller to pass '*' as a single-item list or string
        if isinstance(campos, list):
            if len(campos) == 1 and campos[0] == "*":
                col_expr = "*"
            else:
                safe_cols = [validate_identifier(c, "column name") for c in campos]
                col_expr = ", ".join(safe_cols)
        else:
            # Unexpected type
            raise ValueError("'campos' debe ser una lista de nombres de columna o estar vacío para '*'.")

    sql = f"SELECT {col_expr} FROM {t}"
    if condicion_sql:
        sql += f" WHERE {condicion_sql}"

    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description] if cur.description else []
        return {
            "columns": columns,
            "rows": rows,
            "rowcount": len(rows),
        }
    finally:
        conn.close()

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

def db_insert(
    tabla: str,
    json_valores: Dict[str, Any],
    data_db: str = SQLITE_PATH,
    update_on_conflict: bool = False,
    conflict_cols: Optional[List[str]] = None,
) -> Dict[str, Any]:
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
    base_sql = f"INSERT INTO {t} ({col_list}) VALUES ({placeholders})"

    # Optional UPSERT clause
    sql = base_sql
    if update_on_conflict:
        if not conflict_cols:
            raise ValueError("Debe proporcionar 'conflict_cols' cuando 'update_on_conflict' es True.")
        conflict_cols_safe = [validate_identifier(c, "conflict column") for c in conflict_cols]
        # Actualizar todas las columnas proporcionadas excepto las de conflicto
        update_cols = [c for c in cols if c not in conflict_cols_safe]
        if not update_cols:
            raise ValueError("No hay columnas para actualizar en el UPSERT (todas están en conflict_cols).")
        set_expr = ", ".join([f"{c} = excluded.{c}" for c in update_cols])
        sql = base_sql + f" ON CONFLICT ({', '.join(conflict_cols_safe)}) DO UPDATE SET {set_expr}"
    conn = get_db_connection(data_db)
    try:
        cur = conn.cursor()
        cur.execute(sql, vals)
        conn.commit()
        return {"rowcount": cur.rowcount, "lastrowid": getattr(cur, "lastrowid", None)}
    except sqlite3.IntegrityError as ie:
        # Rollback to release write lock and re-raise as ValueError with friendly message
        try:
            conn.rollback()
        except Exception:
            pass
        raise ValueError(f"Violación de integridad (UNIQUE/FOREIGN KEY): {ie}")
    except Exception:
        # Ensure rollback on any other failure to avoid lingering locks
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

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

def json_to_dataframe(json_data):
    """
    Convierte un JSON con formato específico a DataFrame de pandas
    
    Args:
        json_data: Puede ser un diccionario Python o string JSON con el formato:
                  {'columns': [lista_columnas], 'rows': [lista_tuplas], 'rowcount': n}
    
    Returns:
        pandas.DataFrame: DataFrame con los datos del JSON
    """
    # Si el input es un string, convertirlo a diccionario
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    
    # Crear DataFrame directamente desde las columnas y filas
    df = pd.DataFrame(json_data['rows'], columns=json_data['columns'])
    
    return df
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
        update_on_conflict = bool(payload.get("update_on_conflict", False))
        conflict_cols = payload.get("conflict_cols")
        if conflict_cols is not None and not isinstance(conflict_cols, list):
            return jsonify({"error": "'conflict_cols' debe ser una lista de nombres de columnas."}), 400
    except KeyError as ke:
        return jsonify({"error": f"Falta el campo requerido: {ke}"}), 400
    try:
        result = db_insert(
            tabla,
            valores,
            data_db=data_db,
            update_on_conflict=update_on_conflict,
            conflict_cols=conflict_cols,
        )
        return jsonify(result)
    except ValueError as ve:
        # Likely integrity error (e.g., UNIQUE constraint)
        return jsonify({"error": str(ve)}), 409
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

# Lectura de registros
@app.route("/db/read", methods=["GET"])
def route_db_read():
    """
    GET /db/read?tabla=<nombre>&campos=col1,col2&condicion_sql=<condicion>&db=<optional_db_path>
    - campos: lista separada por comas; si se omite o es '*', devuelve todas
    - condicion_sql: se concatena como WHERE <condicion_sql>
    """
    tabla = request.args.get("tabla")
    campos_param = request.args.get("campos")
    condicion_sql = request.args.get("condicion_sql")
    data_db = request.args.get("db", SQLITE_PATH)

    if not tabla:
        return jsonify({"error": "Falta el parámetro requerido: 'tabla'."}), 400

    # Parse campos
    if not campos_param or campos_param.strip() == "*":
        campos = []  # interpret as '*'
    else:
        campos = [c.strip() for c in campos_param.split(",") if c.strip()]

    try:
        result = db_read(tabla, campos=campos, condicion_sql=condicion_sql, data_db=data_db)
        # Convert rows to dicts for readability
        rows = result["rows"]
        columns = result["columns"]
        dict_rows = [dict(zip(columns, r)) for r in rows] if columns else rows
        return jsonify({
            "columns": columns,
            "rows": dict_rows,
            "rowcount": result["rowcount"],
        })
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Listado de tablas
@app.route("/db/tables", methods=["GET"])
def route_db_tables():
    """
    GET /db/tables?db=<optional_db_path>
    Devuelve la lista de tablas de la base de datos.
    """
    data_db = request.args.get("db", SQLITE_PATH)
    try:
        tables = db_tablas(data_db=data_db)
        return jsonify({"tables": tables, "count": len(tables)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Esquema de una tabla
@app.route("/db/table_schema", methods=["GET"])
def route_db_table_schema():
    """
    GET /db/table_schema?tabla=<nombre>&db=<optional_db_path>
    Devuelve el esquema (columnas) de la tabla indicada.
    """
    tabla = request.args.get("tabla")
    data_db = request.args.get("db", SQLITE_PATH)
    if not tabla:
        return jsonify({"error": "Falta el parámetro requerido: 'tabla'."}), 400
    try:
        # validate_identifier también se aplicará en db_table_schema
        validate_identifier(tabla, "table name")
        schema = db_table_schema(tabla, data_db=data_db)
        return jsonify({"tabla": tabla, "schema": schema, "count": len(schema)})
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
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
