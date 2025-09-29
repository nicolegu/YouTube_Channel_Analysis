import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from migrations.migration_manager import MigrationManager
import logging

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)

    db_path = 'youtube_metrics.db'
    manager = MigrationManager(db_path)

    if manager.migrate():
        print("Migrations completed successfully!")
    else:
        print("Migration failed!")
        sys.exit(1)