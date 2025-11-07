import os
import sqlite3
from dotenv import load_dotenv


def main():
    load_dotenv(override=True)
    path = os.getenv("DB_FILE_PATH", "data/dev.db")
    print(f"DB_FILE_PATH effective: {path}")
    if not os.path.exists(path):
        print("ERROR: DB file does not exist")
        return 2
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        tables = [r[0] for r in cur.fetchall()]
        print(f"Tables ({len(tables)}): {', '.join(tables) if tables else '-'}")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

