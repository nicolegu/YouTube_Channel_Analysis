def up(conn):
    """
    Add video tracking strategy columns to tracking_config table
    """
    cursor = conn.cursor()

    # Check if columns already exist (safety check)
    cursor.execute("PRAGMA table_info(tracking_config)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'video_tracking_strategy' not in columns:
        cursor.execute('''
                       ALTER TABLE tracking_config
                       ADD COLUMN video_tracking_strategy TEXT DEFAULT 'time_based'
                    ''')
    
    if 'video_tracking_days' not in columns:
        cursor.execute('''
            ALTER TABLE tracking_config
            ADD COLUMN video_tracking_days INTEGER DEFAULT 30
        ''')

    if 'min_video_age_days' not in columns:
        cursor.execute('''
            ALTER TABLE tracking_config
            ADD COLUMN min_video_age_days INTEGER DEFAULT 3
        ''')

    cursor.execute('''
        UPDATE tracking_config
        ET video_tracking_strategy = 'time_based',
           video_tracking_days = 30,
           min_video_age_days = 3
        WHERE video_tracking_strategy IS NULL
        ''')
    
def down(conn):
    """
    Rollback migration (SQLite doesn't support DROP COLUMN easily)
    Pass for now
    """
    pass


