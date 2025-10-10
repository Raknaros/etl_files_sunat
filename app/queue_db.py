import sqlite3
from datetime import datetime
from app.config import config

class QueueDB:
    def __init__(self, db_path=None):
        self.db_path = db_path or config.QUEUE_DB_PATH

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def create_table(self):
        with self._get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDIENTE',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error_message TEXT
                )
            ''')

    def insert_task(self, file_name, file_id):
        created_at = updated_at = datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.execute('''
                INSERT INTO tasks (file_name, file_id, created_at, updated_at)
                VALUES (?, ?, ?, ?)
            ''', (file_name, file_id, created_at, updated_at))

    def get_pending_tasks(self):
        with self._get_connection() as conn:
            cursor = conn.execute('SELECT * FROM tasks WHERE status = "PENDIENTE"')
            return cursor.fetchall()

    def update_task_status(self, task_id, status, error_message=None):
        updated_at = datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.execute('''
                UPDATE tasks SET status = ?, updated_at = ?, error_message = ? WHERE id = ?
            ''', (status, updated_at, error_message, task_id))

# Instancia global
queue_db = QueueDB()