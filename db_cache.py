import sqlite3
import json


class DatabaseCache:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path, timeout=30.0)

    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS db_metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                code TEXT PRIMARY KEY,
                data TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monsters (
                code TEXT PRIMARY KEY,
                data TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS resources (
                code TEXT PRIMARY KEY,
                data TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS maps (
                query_key TEXT PRIMARY KEY,
                data TEXT
            )
        """)
        conn.commit()
        conn.close()

    def get_version(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM db_metadata WHERE key = 'game_version'")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None

    def set_version(self, version):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('game_version', ?)", (version,))
        conn.commit()
        conn.close()

    def clear_cache(self, version):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS db_metadata")
        cursor.execute("DROP TABLE IF EXISTS items")
        cursor.execute("DROP TABLE IF EXISTS monsters")
        cursor.execute("DROP TABLE IF EXISTS resources")
        cursor.execute("DROP TABLE IF EXISTS maps")
        conn.commit()
        conn.close()
        self._init_db()
        self.set_version(version)

    def get_item(self, code):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT data FROM items WHERE code = ?", (code,))
        row = cursor.fetchone()
        conn.close()
        return json.loads(row[0]) if row else None

    def set_item(self, code, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO items (code, data) VALUES (?, ?)", (code, json.dumps(data)))
        conn.commit()
        conn.close()

    def get_monster(self, code):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT data FROM monsters WHERE code = ?", (code,))
        row = cursor.fetchone()
        conn.close()
        return json.loads(row[0]) if row else None

    def set_monster(self, code, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO monsters (code, data) VALUES (?, ?)", (code, json.dumps(data)))
        conn.commit()
        conn.close()

    def get_resource(self, code):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT data FROM resources WHERE code = ?", (code,))
        row = cursor.fetchone()
        conn.close()
        return json.loads(row[0]) if row else None

    def set_resource(self, code, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO resources (code, data) VALUES (?, ?)", (code, json.dumps(data)))
        conn.commit()
        conn.close()

    def get_maps(self, query_key):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT data FROM maps WHERE query_key = ?", (query_key,))
        row = cursor.fetchone()
        conn.close()
        return json.loads(row[0]) if row else None

    def set_maps(self, query_key, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO maps (query_key, data) VALUES (?, ?)", (query_key, json.dumps(data)))
        conn.commit()
        conn.close()
