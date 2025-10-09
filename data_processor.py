import sqlite3
from nltk.stem import WordNetLemmatizer
import re
import emoji
from datetime import datetime
import logging
from config import product_keywords, content_type, brands

class YouTubeDataProcessor:
    """
    Process and clean YouTube metrics data
    """

    def __init__(self, db_path = 'youtube_metrics.db'):
        self.db_path = db_path
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format = '%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    # ============ Data Cleaning ============

    def clean_all_data(self):
        """
        Run all data cleaning steps
        """
        self.logger.info('Starting data cleaning...')

        conn = sqlite3.connect(self.db_path)

        try:
            self.validate_raw_data(conn)
            duplicates = self.remove_duplicate_collections(conn)
            self.logger.info(f"Removed {duplicates} duplicate records")

            self.validate_metrics(conn)
            self.clean_text_fields(conn)

            self.logger.info('Data cleaning completed!')
            return True
        
        except Exception as e:
            self.logger.error(f"Data cleaning failed: {e}")
            return False
        
        finally:
            conn.close()

    def validate_raw_data(self, conn):
        """
        Check for missing critical fields
        """
        cursor = conn.cursor()

        cursor.execute('''
            SELECT video_id, title, view_count, published_at
              FROM video_metrics
             WHERE title IS NULL OR title = ''
                OR view_count IS NULL
                OR published_at IS NULL OR published_at = ''
        ''')

        invalid_records_raw = cursor.fetchall()

        if invalid_records_raw:
            self.logger.warning(f"Found {len(invalid_records_raw)} records with missing data")
            for record in invalid_records_raw:
                self.logger.warning(f"Invalid record in raw data: {record}")

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
    
    def validate_metrics(self, conn):
        """
        Check for impossible values
        """
        cursor = conn.cursor()

        # Check negative counts
        cursor.execute('''
            SELECT video_id, view_count, like_count, comment_count
              FROM video_metrics
             WHERE view_count < 0
                  OR like_count < 0
                  OR comment_count < 0
        ''')

        invalid_records_metrics = cursor.fetchall()

        if invalid_records_metrics:
            self.logger.warning(f"Found {len(invalid_records_metrics)} records with negative metrics")

        # Check impossible ratios (likes > views)
        cursor.execute('''
            SELECT video_id, view_count, like_count
              FROM video_metrics
             WHERE like_count > view_count
        ''')

        conflicts = cursor.fetchall()

        if conflicts:
            self.logger.warning(f"Found {len(conflicts)} records where likes > views")

    def clean_text_fields(self, conn):
        """
        Fix encoding issues in titles and comments
        """
        cursor = conn.cursor()
        
        # Clean video titles
        cursor.execute('SELECT video_id, title FROM video_metrics WHERE title is NOT NULL')
        
        updates = 0
        for video_id, title in cursor.fetchall():
            cleaned = title.encode('utf-8', errors = 'ignore').decode('utf-8').strip()

            if cleaned != title:
                cursor.execute(
                    'UPDATE video_metrics SET title = ? WHERE video_id = ?',
                    (cleaned, video_id)
                )
                updates += 1
        
        # Clean comments
        cursor.execute('SELECT comment_id, comment_text FROM comments WHERE comment_text is NOT NULL')

        for comment_id, text in cursor.fetchall():
            cleaned = text.encode('utf-8', errors = 'ignore').decode('utf-8')

            if cleaned != text:
                cursor.execute(
                    'UPDATE comments SET comment_text = ? WHERE comment_id = ?',
                    (cleaned, comment_id)
                )
                updates += 1
        
        if updates > 0:
            self.logger.info(f"Cleaned {updates} text fields")
            conn.commit()

    # ============ VIDEO PROCESSING ============

    def process_all_videos(self):
        """
        Process all unprocessed video
        """
        self.logger.info('Starting video processing...')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Get videos that haven't been processed yet
            cursor.execute('''
                SELECT vm.video_id, vm.channel_id, vm.title, vm.duration, vm.published_at
                  FROM video_metrics vm
                  LEFT JOIN processed_videos pv
                    ON vm.video_id = pv.video_id
                 WHERE pv.video_id IS NULL
                 GROUP BY vm.video_id
            ''')

            videos = cursor.fetchall()
            self.logger.info(f"Processing {len(videos)} new videos")

            for video_id, channel_id, title, duration, published_at in videos:
                try:
                    self.process_single_videos(
                        conn, video_id, channel_id, title, duration, published_at
                    )
                except Exception as e:
                    self.logger.error(f"Failed to process video {video_id}: {e}")
            
            conn.commit()
            self.logger.info("Video processing completed")

        except Exception as e:
            self.logger.error(f"Video processing failed: {e}")
            conn.rollback()

        finally:
            conn.close()

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

def clean_text_fields(conn):
    """
    Fix encoding issues in titles and comments
    """
    cursor = conn.cursor()

    cursor.execute('SELECT video_id, title FROM video_metrics')

    video_titles = cursor.fetchall()

    for video_id, title in video_titles:
        if title:
            cleaned = title.encode('utf-8', errors = 'ignore').decode('utf-8')

            if cleaned != title:
                cursor.execute(
                    'UPDATE video_metrics SET title = ? WHERE video_id = ?',
                    (cleaned, video_id)
                )

    cursor.execute('SELECT comment_id, comment_text FROM comments')

    comment_texts = cursor.fetchall()

    for comment_id, comment in comment_texts:
        if comment:
            cleaned = comment.encode('utf-8', errors = 'ignore').decode('utf-8')

            if cleaned != comment:
                cursor.execute(
                    'UPDATE comments SET comment_text = ? WHERE comment_id = ?',
                    (cleaned, comment_id)
                )

    conn.commit()




