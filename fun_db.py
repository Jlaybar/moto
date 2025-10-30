import sqlite3
import math
import pandas as pd
import numpy as  np
import os
from typing import Any, Dict


SQLITE_PATH = os.getenv('DB_FILE_PATH', 'dev.db')

# DDL solo se ejecuta si no existe la tabla
DDL = """
CREATE TABLE IF NOT EXISTS data_moto (
    id          INTEGER PRIMARY KEY,
    url         TEXT    NOT NULL,
    title       TEXT    NOT NULL,
    km          REAL,
    price       INTEGER,
    year        INTEGER,
    imgUrl      TEXT,
    provinceId  INTEGER,
    hp          REAL,
    marca       TEXT,
    modelo      TEXT,
    UNIQUE (url)
);
"""

UPSERT_SQL = """
INSERT INTO data_moto (id, url, title, km, price, year, imgUrl, provinceId, hp, marca, modelo)
VALUES (:id, :url, :title, :km, :price, :year, :imgUrl, :provinceId, :hp, :marca, :modelo)
ON CONFLICT(id) DO UPDATE SET
    url        = excluded.url,
    title      = excluded.title,
    km         = excluded.km,
    price      = excluded.price,
    year       = excluded.year,
    imgUrl     = excluded.imgUrl,
    provinceId = excluded.provinceId,
    hp         = excluded.hp,
    marca      = excluded.marca,
    modelo     = excluded.modelo;
"""

def _is_nan(v: Any) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))

def _to_float_or_none(v: Any):
    if _is_nan(v): return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None

def _to_int_or_none(v: Any):
    if _is_nan(v): return None
    try:
        return int(float(v))
    except Exception:
        return None

# --------------------------------------------------------------------------------------------
def insert_motos_from_json(items_json, marca: str, modelo: str, db_path: str=SQLITE_PATH) -> Dict[str, int]:

    df = pd.DataFrame(items_json)
    df["marca"] = marca
    df["modelo"] = modelo

    expected_cols = ["id","url","title","km","price","year","imgUrl","provinceId","hp","marca","modelo"]
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {missing}")

    df = df[expected_cols].replace({np.nan: None}).copy()

    # Normalización rápida
    def _to_int(x):
        try:
            return int(x) if x not in (None, "", "None") and not pd.isna(x) else None
        except Exception:
            return None
    def _to_float(x):
        try:
            return float(x) if x not in (None, "", "None") and not pd.isna(x) else None
        except Exception:
            return None
    def _to_str(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        s = str(x).strip()
        return s if s else None

    df["id"]         = df["id"].map(_to_str)
    df["url"]        = df["url"].map(_to_str)
    df["title"]      = df["title"].map(_to_str)
    df["km"]         = df["km"].map(_to_int)
    df["price"]      = df["price"].map(_to_int)
    df["year"]       = df["year"].map(_to_int)
    df["imgUrl"]     = df["imgUrl"].map(_to_str)
    df["provinceId"] = df["provinceId"].map(_to_str)
    df["hp"]         = df["hp"].map(_to_float)
    df["marca"]      = df["marca"].map(_to_str)
    df["modelo"]     = df["modelo"].map(_to_str)

    rows, skipped = [], 0
    for _, r in df.iterrows():
        d = r.to_dict()
        if not d["id"] or not d["url"] or not d["title"]:
            skipped += 1
            continue
        rows.append((
            d["id"], d["url"], d["title"], d["km"], d["price"],
            d["year"], d["imgUrl"], d["provinceId"], d["hp"], d["marca"], d["modelo"]
        ))
    if not rows:
        return {"inserted": 0, "updated": 0, "skipped": skipped, "total": skipped}

    # DDL "nueva" por si la tabla no existe aún (incluye created_at/updated_at)
    base_cols = "(id, url, title, km, price, year, imgUrl, provinceId, hp, marca, modelo)"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA journal_mode = WAL;")
        cur.execute("PRAGMA synchronous = NORMAL;")

        # Crea si no existe
        cur.execute(DDL)

        # --- Asegura columnas faltantes en tablas antiguas ---
        cur.execute("PRAGMA table_info(data_moto);")
        existing_cols = {row[1] for row in cur.fetchall()}

        # Asegura columna 'marca' si falta en esquemas antiguos
        if "marca" not in existing_cols:
            cur.execute("ALTER TABLE data_moto ADD COLUMN marca TEXT;")
            existing_cols.add("marca")
        # Asegura columna 'modelo' si falta en esquemas antiguos
        if "modelo" not in existing_cols:
            cur.execute("ALTER TABLE data_moto ADD COLUMN modelo TEXT;")
            existing_cols.add("modelo")

        # Añade columnas si faltan (compatibles con SQLite: sin DEFAULT de función)
        added_cols = []
        for col in ("created_at", "updated_at"):
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE data_moto ADD COLUMN {col} TEXT;")
                added_cols.append(col)
        if added_cols:
            # Inicializa valores para no dejar NULL si no quieres
            now_sql = "datetime('now')"
            if "created_at" in added_cols:
                cur.execute(f"UPDATE data_moto SET created_at = COALESCE(created_at, {now_sql});")
            if "updated_at" in added_cols:
                cur.execute(f"UPDATE data_moto SET updated_at = COALESCE(updated_at, {now_sql});")
            # refrescamos conjunto de columnas
            cur.execute("PRAGMA table_info(data_moto);")
            existing_cols = {row[1] for row in cur.fetchall()}

        # Índices útiles para lecturas
        cur.execute("CREATE INDEX IF NOT EXISTS idx_data_moto_price ON data_moto(price);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_data_moto_year  ON data_moto(year);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_data_moto_km    ON data_moto(km);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_data_moto_prov  ON data_moto(provinceId);")
        # índice por expresión (puede no estar disponible en SQLite muy antiguo)
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_data_moto_title_ci ON data_moto(LOWER(title));")
        except sqlite3.OperationalError:
            pass  # omite si la versión no soporta índices por expresión

        # --- UPSERT dinámico: sólo toca updated_at si existe ---
        set_parts = [
            "url = excluded.url",
            "title = excluded.title",
            "km = excluded.km",
            "price = excluded.price",
            "year = excluded.year",
            "imgUrl = excluded.imgUrl",
            "provinceId = excluded.provinceId",
            "hp = excluded.hp",
            "marca = excluded.marca",
            "modelo = excluded.modelo",
        ]
        if "updated_at" in existing_cols:
            set_parts.append("updated_at = datetime('now')")

        UPSERT_SQL = f"""
        INSERT INTO data_moto {base_cols}
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            {", ".join(set_parts)};
        """

        # Métricas inserted/updated
        existing_ids = set()
        ids = [r[0] for r in rows]
        for i in range(0, len(ids), 900):
            chunk = ids[i:i+900]
            q = ",".join("?" for _ in chunk)
            cur.execute(f"SELECT id FROM data_moto WHERE id IN ({q})", chunk)
            existing_ids.update(x[0] for x in cur.fetchall())

        cur.executemany(UPSERT_SQL, rows)
        conn.commit()

        inserted = sum(1 for r in rows if r[0] not in existing_ids)
        updated  = len(rows) - inserted
        return {"inserted": inserted, "updated": updated, "skipped": skipped, "total": len(rows) + skipped}
    finally:
        conn.close()
