import sqlite3
import os
import logging
from datetime import datetime

class MigrationManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.migrations_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger = logging.getLogger(__name__)
        self.setup_migrations_table()

    def setup_migrations_table(self):
        """
        Create table to track which migrations have been run
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_name TEXT NOT NULL UNIQUE,
                applied_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN DEFAULT 1
            )
        ''')

        conn.commit()
        conn.close()

    def get_applied_migrations(self):
        """
        Get list of migrations that have already been applied
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT migration_name FROM schema_migrations WHERE success = 1')
        applied = [row[0] for row in cursor.fetchall()]

        conn.close()
        return set(applied)
    
    def get_pending_migrations(self):
        """
        Get list of migrations that need to be applied
        """

        # Find all migration files
        migration_files = []
        for filename in os.listdir(self.migrations_dir):
            if filename.endswith('.py') and filename.startswith(('001_', '002_', '003_')):
                migration_files.append(filename[:-3]) # Remove .py extension

        migration_files.sort() # Ensure proper order

        return [m for m in migration_files if m not in self.get_applied_migrations()]
    
    def apply_migration(self, migration_name):
        """
        Apply a single migration
        """
        module_name = f"migrations.{migration_name}"

        try:
            # Dynamically import migration module
            migration_module = __import__(module_name, fromlist = [migration_name])

            if not hasattr(migration_module, 'up'):
                raise ValueError(f"Migration {migration_name} missing 'up' function")
            
            # Apply migration
            conn = sqlite3.connect(self.db_path)
            try:
                migration_module.up(conn)

                # Record successful migration
                cursor = conn.cursor()
                cursor.execute('''
                               INSERT INTO schema_migrations (migration_name) VALUES (?)
                ''', (migration_name,))

                conn.commit()
                self.logger.info(f"Applied migration: {migration_name}")
                return True
            
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Migration {migration_name} failed: {e}")

                # Record failed migration
                cursor = conn.cursor()
                cursor.execute('''
                    INSER INTO schema_migrations (migration_name, success) VALUES (?, 0)
                ''', (migration_name,))
                conn.commit()

                raise
            finally:
                conn.close()

        except Exception as e:
            self.logger.error(f"Could not load migration {migration_name}: {e}")
            return False