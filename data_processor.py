from nltk.stem import WordNetLemmatizer
import re
import emoji
from datetime import datetime
import logging

# Video classification by title

product_keywords = {
    'stationery': ['stationery'],
    'pens': ['pen', 'fountain pen', 'ballpoint pen', 'gel pen', 'multi pen', 'rollerball pen',
             'brush pen', 'calligraphy pen', 'comic pen', 'manga pen', 'dip pen', 'marker',
             'felt tip pen', 'highlighter', 'stylus pen'],

    'refills_and_inks': ['refill', 'ink', 'fountain pen ink', 'drawing ink', 'calligraphy ink',
                         'comic ink', 'dip pen ink', 'india ink', 'iron gall ink', 'shimmering ink',
                         'waterproof ink', 'nib', 'converter', 'bottle', 'blade', 'replacement', 'tip',
                         'inkwell', 'reservoir', 'cleaner', 'silicone grease', 'filler'],

    'pencils': ['pencil', 'mechanical pencil', 'drafting pencil', 'lead holder', 'lead pointer',
                'wooden pencil', 'pencil lead', 'pencil cap', 'pencil grip', 'pencil sharpener',
                'pencil holder', 'eraser'],

    'paper': ['paper', 'notebook', 'notebook cover', 'binder', 'folder', 'planner', 'journal',
              'sketchbook', 'sticky note', 'notepad', 'envelope', 'letter', 'flash card', 'index card',
              'postcard', 'loose leaf paper', 'comic paper'],

    'crafts': ['tape', 'glue', 'washi tape', 'tape runner', 'clear tape', 'stamp', 'stamp ink pad',
               'stamp cleaner', 'sticker', 'transfer sticker', 'sealing wax', 'wax seal stamp',
               'watercolor', 'acrylic', 'gouache', 'palette', 'coloring book', 'crayon', 'stencil', 'chalk',
               'tape cutter'],

    'cases_and_bags': ['pencil case', 'pen case', 'bag', 'backpack', 'pouch', 'purse', 'case'],

    'office_and_toys': ['bookmark', 'correction tape', 'correction pen', 'ruler', 'scissors', 'paper clip',
                        'desk organizer', 'desk tray', 'stapler', 'keychain', 'plushie', 'binder divider']
}

content_type = {
    'tutorial': ['tutorial', 'how to', 'guide', 'tip', 'diy', 'trick', 'way'],
    'review': ['review', 'unboxing'],
    'haul': ['haul', 'budget'],
    'showcase': ['collection', 'favorite', "what's in", 'techo kaigi'],
    'event': ['event', 'livestream', 'party', 'pen show', 'workshop', 'pop up', 'fest']
}

brands = {'paper': [
              'Hobonichi', 'Kokuyo', 'Midori', "TRAVELER'S COMPANY", 'Maruman',
              'Rhodia', 'Leuchtturm1917', 'Clairefontaine', 'Yamamoto', 'Stalogy',
              'LIFE', 'Tomoe River'],
          'fountain_pens': [
              'Pilot', 'Sailor', 'Platinum', 'LAMY', 'Kaweco', 'Pelikan', 'TWSBI',
              'Faber-Castell', 'Parker', 'BENU', 'Opus 88', 'Visconti'],
          'ink': [
              'Diamine', "Noodler's", 'Robert Oster', 'Herbin', 'Rohrer & Klingner',
              'De Atramentis', 'Colorverse', 'Takeda Jimuki', 'Dominant', 'Nagasawa', 'Monteverde'],
          'pencils_pens': [
              'Uni', 'Pentel', 'Zebra', 'Tombow', 'Sakura', 'Copic', 'Stabilo'],
          'art_supplies': [
              'Staedtler', 'Kuretake', 'Blackwing', 'Speedball', "Caran d'Ache",
              'Koh-I-Noor', 'Deleter', 'Tachikawa', 'Stillman & Birn', 'Winsor & Newton'],
          'bags': ['Lihit Lab', 'Doughnut', 'Sun-Star'],
          'featured_brands': [
              'JetPens', 'Rotring', 'TOOLS to LIVEBY', 'Sanby', 'Hightide', "Mark's",
              'Suatelier', 'Retro 51', 'BIGiDESIGN', 'Rickshaw', 'Kakimori', 'Field Notes'],
          'new_retailers': [
              'Green Flash', 'Sheaffer', 'Wearingeul', 'Cross', 'Clarto', 'Matsubokkuri',
              'OLFA', 'Girologic', 'UGears', 'Journalize', 'Greeting Life', 'Writech']          
}

def preprocess_title(title):
    """
    Extract useful info from title with emoji handling
    """

    title_no_emoji = emoji.replace_emoji(title, replace = '')

    title_cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', title_no_emoji)

    emojis_found = emoji.emoji_list(title)

    return {
        'title_clean': title_cleaned.strip(),
        'emojis': [e['emoji'] for e in emojis_found]
    }

def extract_brands_from_title(title):
    """
    Extract mentioned brands from video title
    """
    title_lower = title.lower()
    brands_found = []

    for category, brands in brands.items():
        for brand in brands:
            # Check for brand mention (case-insensitive)
            if brand.lower() in title_lower:
                brands_found.append({
                    'brand': brand,
                    'category': category
                })

    return brands_found

def categorize_video(title):
    wnl = WordNetLemmatizer()
    title_lower = title.lower()
    words_in_title = title_lower.split()
    words_in_title = [wnl.lemmatize(word) for word in words_in_title]
    title_cleaned = ' '.join(words_in_title)
    products = []
    content_types = []

    for category, keywords in product_keywords.items():
        if any(keyword in title_cleaned for keyword in keywords):
            products.append(category)

    for category, keywords in content_type.items():
        if any(keyword in title_cleaned for keyword in keywords):
            content_types.append(category)

    return {'products': products, 'content_types': content_types,
            'words_in_title': words_in_title, 'title_cleaned': title_cleaned}


a = preprocess_title('Why Your Planner is Failing You 💔📓 + How to Fix it!')
print(a)

categories = categorize_video(a['title_clean'])
print(categories)


# Transfrom duration

def video_duration(duration):
    """
    Convert video length into seconds
    """
    duration = duration[2:] # Remove 'PT'
    time = ''
    total = 0
    for char in duration:

        if char == 'H':
            total += int(time) * 60 * 60
            time = ''
        elif char == 'M':
            total += int(time) * 60
            time = ''
        elif char == 'S':
            total += int(time)
        else:
            time += char
    
    return total

print(video_duration('PT14M44S'))


# Get day of week for publication date
def get_publish_day_of_week(published_at):
    """
    Extract day of week from publish date
    """
    publish_day = published_at[:11]
    day_obj = datetime.strptime(publish_day, '%Y-%m-%d')
    day_of_week = day_obj.strftime('%A')

    return day_of_week

def get_publish_hour(published_at):
    """
    Extract hour from publish date
    """
    publish_hour = published_at[12:14]

    return int(publish_hour)


def validate_raw_data(conn):
    """
    Check for and handle missing critical fields
    """
    cursor = conn.cursor()

    # Find videos with missing essential data
    cursor.execute('''
      SELECT video_id, title, view_count, published_at
        FROM video_metrics
       WHERE title IS NULL
          OR view_count IS NULL
          OR published_at IS NULL
    ''')

    invalid_records = cursor.fetchall()

    if invalid_records:
        logging.warning(f"Found {len(invalid_records)}")


def remove_duplicate_collections(conn):
    """
    Remove duplicate metric snapshots collected at same time
    """
    cursor = conn.cursor()

    # Keep only the latest entry per video per collection time
    cursor.execute('''
       DELETE FROM video_metrics
        WHERE id NOT IN (
            SELECT MAX(id)
              FROM video_metrics
             GROUP BY video_id, datetime(timestamp, 'start of hour')
        )
    ''')

    number_deleted = cursor.rowcount
    conn.commit()
    return number_deleted

def validate_metrics(conn):
    """
    Check for impossible values
    """
    cursor = conn.cursor()

    # Negative counts shouldn't exist
    cursor.execute('''
        SELECT video_id, view_count, like_count, comment_count
          FROM video_metrics
         WHERE view_count < 0
              OR like_count < 0
              OR comment_count < 0
    ''')

    invalid_records = cursor.fetchall()

    if invalid_records:
        logging.warning(f"Found {len(invalid_records)} records with negative metrics")

    # Likes/comments can't exceed views (theoretically)
    cursor.execute('''
        SELECT video_id, view_count, like_count
          FROM video_metrics
         WHERE like_count > view_count
    ''')

    conflict_records = cursor.fetchall()

    if conflict_records:
        logging.warning(f"Found {len(conflict_records)} records with conflict metrics")








