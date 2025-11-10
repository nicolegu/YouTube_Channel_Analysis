def up(conn):
    """
    Add video description column to video_metrics table
    """
    cursor = conn.cursor()

    # Check if the column already exists
    cursor.execute('PRAGMA table_info(video_metrics)')
    columns = [col[1] for col in cursor.fetchall()]

    if 'description' not in columns:
        cursor.execute('''
            ALTER TABLE video_metrics
            ADD COLUMN description TEXT
        ''')

def down(conn):
    """
    Pass for now
    """
    pass