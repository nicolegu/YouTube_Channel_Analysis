def up(conn):
    """
    Add question_topic column to comments table
    """
    cursor = conn.cursor()

    # Check if column already exists
    cursor.execute('PRAGMA table_info(comments)')
    columns = [col[1] for col in cursor.fetchall()]

    if 'question_topic' not in columns:
        cursor.execute("""
            ALTER TABLE comments
            ADD COLUMN question_topic TEXT DEFAULT 'general'
        """)
        print('Add question_topic column to comments table')

    else:
        print('question_topic column already exists')

def down(conn):
    """
    Pass for now
    """
    pass