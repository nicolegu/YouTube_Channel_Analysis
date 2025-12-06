def up(conn):
    """
    Add question_cluster_id column to the comments table
    Add cluster label table
    """
    cursor = conn.cursor()

    # Check if the column already exists
    cursor.execute('PRAGMA table_info(comments)')
    columns = [col[1] for col in cursor.fetchall()]

    if 'question_cluster_id' not in columns:
        cursor.execute("""
           ALTER TABLE comments
           ADD COLUMN question_cluster_id TEXT DEFAULT '-1'
        """)
    
    # Add cluster label table 
    cursor.execute('''
        CREATE TABLE question_cluster_labels (
            cluster_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            label TEXT NOT NULL,
            example_questions TEXT,
            questions_count INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
    ''')

def down(conn):
    """
    Remove cluster label table
    """
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS question_cluster_labels')

