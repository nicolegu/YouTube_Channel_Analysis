def up(conn):
    """
    Add comments table
    """
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id TEXT NOT NULL UNIQUE,
            video_id TEXT NOT NULL,
            author_name TEXT NOT NULL,
            author_channel_id TEXT NOT NULL,
            comment_text TEXT,
            like_count INTEGER,
            published_at TEXT,
            updated_at TEXT,
            reply_count INTEGER,
            is_reply BOOLEAN DEFAULT 0,
            parent_comment_id TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
    ''')

    # Add indices for performance
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_comments_video_id
        ON comments(video_id)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_comments_published
        ON comments(published_at)
    ''')

def down(conn):
    """
    Remove comments table
    """
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS comments')


