def up(conn):
    """
    Add comment analysis columns to comments table
    """
    cursor = conn.cursor()

    # Check if columns already exist
    cursor.execute('PRAGMA table_info(comments)')
    columns = [col[1] for col in cursor.fetchall()]

    if 'sentiment' not in columns:
        cursor.execute('''
            ALTER TABLE comments
            ADD COLUMN sentiment TEXT
        ''')

    if 'purchase_intent' not in columns:
        cursor.execute('''
            ALTER TABLE comments
            ADD COLUMN purchase_intent BOOLEAN DEFAULT 0
        ''')
    
    if 'is_question' not in columns:
        cursor.execute('''
            ALTER TABLE comments
            ADD COLUMN is_question BOOLEAN DEFAULT 0
        ''')
    
    if 'emojis' not in columns:
        cursor.execute('''
            ALTER TABLE comments
            ADD COLUMN emojis TEXT
        ''')
