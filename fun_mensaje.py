import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

# Directorio base del proyecto (carpeta que contiene este archivo)
APP_DIR = Path(__file__).resolve().parent

# Ruta a la base de datos (se puede sobreescribir con la variable de entorno DB_PATH)
DB_PATH = Path(os.getenv("DB_PATH", APP_DIR / "dev.db"))

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def ensure_schema():
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mensajes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            correo TEXT NOT NULL,
            asunto TEXT NOT NULL,
            body TEXT NOT NULL,
            ip TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()


def insert_mensaje(correo: str, asunto: str, body: str, ip: str | None = None, created_at: str | None = None) -> int:
    """Inserta un mensaje y devuelve el id creado."""
    if created_at is None:
        created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO mensajes (correo, asunto, body, ip, created_at) VALUES (?, ?, ?, ?, ?)",
        (correo, asunto, body, ip, created_at),
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id


__all__ = [
    "APP_DIR",
    "DB_PATH",
    "EMAIL_RE",
    "get_db",
    "ensure_schema",
    "insert_mensaje",
]
