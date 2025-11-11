def up(conn):
    """
    Add brands_mentioned and product_categories columns to the comments table 
    """
    cursor = conn.cursor()

    # Check if the columns already exist
    cursor.execute('PRAGMA table_info(comments)')
    columns = [col[1] for col in cursor.fetchall()]

    if 'brands_mentioned' not in columns:
        cursor.execute('''
            ALTER TABLE comments
            ADD COLUMN brands_mentioned TEXT
        ''')

    if 'product_categories' not in columns:
        cursor.execute('''
            ALTER TABLE comments
            ADD COLUMN product_categories TEXT
        ''')


def down(conn):
    """
    Pass for now
    """
    pass