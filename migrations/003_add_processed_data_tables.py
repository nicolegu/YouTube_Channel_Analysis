def up(conn):
    """
    Create tables for processed video data and engagement metrics
    """
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            title_cleaned TEXT,
            duration_seconds INTEGER,
            product_categories TEXT,
            content_types TEXT,
            brands_mentioned TEXT,
            emojis TEXT,
            last_processed DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES video_metrics(video_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS video_engagement_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            engagement_rate REAL,
            views_since_last_check INTEGER,
            FOREIGN KEY (video_id) REFERENCES video_metrics(video_id)
        )
    ''')

    # Indices for performance
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_processed_videos_channel
        ON processed_videos(channel_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_engagement_video_timestamp
        ON video_engagement_metrics(video_id, timestamp)
    ''')

def down(conn):
    """
    Remove processed data tables
    """
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS video_engagement_metrics')
    cursor.execute('DROP TABLE IF EXISTS processed_videos')