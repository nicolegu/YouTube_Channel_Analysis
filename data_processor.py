import sqlite3
from nltk.stem import WordNetLemmatizer
import re
import emoji
from datetime import datetime
import logging
import isodate
import json
from config import product_keywords, content_types, brands, positive_words, negative_words, purchase_intent

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

    def remove_duplicate_collections(self, conn):
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

    def process_all_videos(self, force_reprocess = False):
        """
        Process all unprocessed video
        """
        self.logger.info('Starting video processing...')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            if force_reprocess:
                # Reprocess all videos
                cursor.execute('''
                    SELECT vm.video_id, vm.channel_id, vm.title, vm.duration, vm.published_at, vm.description
                      FROM video_metrics vm
                     WHERE vm.title IS NOT NULL
                     GROUP BY vm.video_id
                ''')
            else:
                # Get videos that haven't been processed yet
                cursor.execute('''
                    SELECT vm.video_id, vm.channel_id, vm.title, vm.duration, vm.published_at, vm.description
                      FROM video_metrics vm
                      LEFT JOIN processed_videos pv
                        ON vm.video_id = pv.video_id
                     WHERE pv.video_id IS NULL
                     GROUP BY vm.video_id
                ''')

            videos = cursor.fetchall()
            self.logger.info(f"Processing {len(videos)} new videos")

            for video_id, channel_id, title, duration, published_at, description in videos:
                try:
                    self.process_single_videos(
                        conn, video_id, channel_id, title, duration, published_at, description
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

    def process_single_videos(self, conn, video_id, channel_id, title, duration, published_at, description):
        """
        Process a single video and store results
        """
        cursor = conn.cursor()

        # Clean title and extract emojis
        processed = self.preprocess_title(title)

        # Categorize
        categories = self.categorize_video(processed['title_clean'])

        # Extract brands
        brands_found = self.extract_brands_from_content(title, description)

        # Convert duration
        duration_seconds = self.parse_duration(duration) if duration else None

        # Extract temporal features
        publish_day = self.get_publish_day_of_week(published_at)
        publish_hour = self.get_publish_hour(published_at)

        # Store processed data
        cursor.execute('''
            INSERT OR REPLACE INTO processed_videos
            (video_id, channel_id, title_cleaned, duration_seconds,
             product_categories, content_types, brands_mentioned, emojis,
             publish_day_of_week, publish_hour, last_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video_id,
            channel_id,
            processed['title_clean'],
            duration_seconds,
            json.dumps(categories['products']),
            json.dumps(categories['content_types']),
            json.dumps(brands_found),
            json.dumps(processed['emojis']),
            publish_day,
            publish_hour,
            datetime.now()
        ))

    # ============ COMMENT PROCESSING ============

    def process_all_comments(self, force_reprocess=False):
        """
        Process all unprocessed comments
        """
        self.logger.info('Start comment processing...')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            if force_reprocess:
                # Reprocess ALL comments
                cursor.execute('''
                    SELECT comment_id, video_id, comment_text
                      FROM comments
                     WHERE comment_text IS NOT NULL
                ''')
            else:
                # Get comments that haven't been processed
                cursor.execute('''
                    SELECT comment_id, video_id, comment_text
                      FROM comments
                     WHERE sentiment IS NULL
                ''')

            comments = cursor.fetchall()
            self.logger.info(f"Processing {len(comments)} new comments")

            for comment_id, video_id, comment_text in comments:
                try:
                    self.process_single_comment(
                        conn, comment_id, video_id, comment_text
                    )
                except Exception as e:
                    self.logger.error(f"Failed to process comment {comment_id}: {e}")
            
            conn.commit()
            self.logger.info("Comment processing completed")

        except Exception as e:
            self.logger.error(f"Comment processing failed: {e}")
            conn.rollback()

        finally:
            conn.close()


    def process_single_comment(self, conn, comment_id, video_id, comment_text):
        """
        Process a single comment and store results
        """
        cursor = conn.cursor()
        
        # Get results of comment analysis
        results = self.analyze_comment_sentiment(comment_text)
        
        # Store results
        cursor.execute('''
            UPDATE comments
            SET sentiment = ?,
                purchase_intent = ?,
                is_question = ?,
                emojis = ?
            WHERE comment_id = ?
                 AND video_id = ?
        ''', (results['sentiment'],
              results['purchase_intent'],
              results['is_question'],
              json.dumps(results['emojis']),
              comment_id,
              video_id
        ))

    
    # ============ TEXT PROCESSING HELPERS ============

    def preprocess_title(self, title):
        """
        Extract useful info from title with emoji handling
        """
        if not title:
            return {'title_clean': '', 'emojis': []}

        title_no_emoji = emoji.replace_emoji(title, replace = '')
        title_cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', title_no_emoji)
        emojis_found = emoji.emoji_list(title)

        return {
            'title_clean': title_cleaned.strip(),
            'emojis': [e['emoji'] for e in emojis_found]
        }
    
    def categorize_video(self, title):
        """
        Categorize video by product type and content type
        """
        if not title:
            return {'products': [], 'content_types': []}
        
        wnl = WordNetLemmatizer()
        title_lower = title.lower()
        words_in_title = title_lower.split()
        words_in_title = [wnl.lemmatize(word) for word in words_in_title]
        title_cleaned = ' '.join(words_in_title)
        products = []
        found_content_types = []
        
        # Check product categories
        for category, keywords in product_keywords.items():
            if any(keyword in title_cleaned for keyword in keywords):
                products.append(category)
        
        # Check content types
        for category, keywords in content_types.items():
            if any(keyword in title_cleaned for keyword in keywords):
                found_content_types.append(category)

        return {'products': products, 'content_types': found_content_types}
    
    def extract_brands_from_title(self, title):
        """
        Extract mentioned brands from video title
        """
        if not title:
            return []
        
        title_lower = title.lower()
        brands_found = []

        for category, brand_list in brands.items():
            for brand in brand_list:
            # Check for brand mention (case-insensitive)
                if brand.lower() in title_lower:
                    brands_found.append({
                        'brand': brand,
                        'category': category
                    })

        return brands_found
    
    def extract_brands_from_content(self, description, title):
        """
        Extract mentioned brands from both title and description
        """
        combined_text = f"{title or ''} {description or ''}".lower()

        if not combined_text.strip():
            return []
        
        # Split into words for exact matching
        words = re.findall(r'\b\w+\b', combined_text)
        words_set = set(words)

        brands_found = []
        brands_seen = set()

        for category, brand_list in brands.items():
            for brand in brand_list:
                brand_lower = brand.lower()

                # Skip if we've already found this brand
                if brand in brands_seen:
                    continue
                
                # Check multi-word brands 
                if ' ' in brand_lower or "'" in brand_lower:
                    if brand_lower in combined_text:
                        brands_found.append({
                            'brand': brand,
                            'category': category
                        })

                        brands_seen.add(brand)

                elif brand_lower in words_set:
                    brands_found.append({
                        'brand': brand,
                        'category': category
                    })

                    brands_seen.add(brand)

        return brands_found

    # Transfrom duration
    def parse_duration(self, duration):
        """
        Convert ISO 8601 duration to seconds
        """
        try:
            return int(isodate.parse_duration(duration).total_seconds())
        except:
            self.logger.warning(f"Failed to parse duration: {duration}")
            return None
        
    # Get day of week for publication date
    def get_publish_day_of_week(self, published_at):
        """
        Extract day of week from publish date
        """
        try:
            dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            return dt.strftime('%A')
        except:
            self.logger.warning(f"Failed to parse date: {published_at}")
            return None

    def get_publish_hour(self, published_at):
        """
        Extract hour from publish date
        """
        try:
            dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            return dt.hour
        except:
            return None
        
    def analyze_comment_sentiment(self, comment_text):
        """
        Simple rule-based sentiment
        """
        if not comment_text:
            return {
                'sentiment': 'neutral',
                'purchase_intent': False,
                'is_question': False,
                'emojis': []
            }
        
        comment_no_emoji = emoji.replace_emoji(comment_text, replace = '')
        comment_cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', comment_no_emoji)
        emojis_found = emoji.emoji_list(comment_text)

        text_lower = comment_cleaned.lower()
        words = text_lower.split()
        words_set = set(words)

        # Sentiment scoring
        positive_count = sum(1 for word in words if word in positive_words)
        negative_count = sum(1 for word in words if word in negative_words)

        if positive_count > negative_count:
            sentiment = 'positive'
        elif negative_count > positive_count:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'

        has_purchase_intent = any(word in words_set for word in purchase_intent)
        is_question = '?' in comment_text

        return {
            'sentiment': sentiment,
            'purchase_intent': has_purchase_intent,
            'is_question': is_question,
            'emojis': [e['emoji'] for e in emojis_found]
        }



# ============ ENGAGEMENT METRICS ============

    def calculate_engagement_metrics(self):
        """
        Calculate engagement metrics for all videos
        """
        self.logger.info('Calculating engagement metrics...')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
    
        try:
            # Get latest metrics for each video
            cursor.execute('''
                SELECT video_id, view_count, like_count, comment_count, timestamp
                  FROM video_metrics
                 WHERE view_count > 0
            ''')

            for video_id, views, likes, comments, timestamp in cursor.fetchall():
                if views > 0:
                    engagement_rate = (likes + comments) / views
                    like_rate = likes / views
                    comment_rate = comments / views
                    
                    # Check if this snapshot already exists
                    cursor.execute('''
                        SELECT id FROM video_engagement_metrics
                         WHERE video_id = ? AND timestamp = ?
                    ''', (video_id, timestamp))

                    if not cursor.fetchone(): # Only insert if not already stored
                        cursor.execute('''
                            INSERT INTO video_engagement_metrics
                            (video_id, timestamp, engagement_rate, like_rate, comment_rate)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (video_id, timestamp, engagement_rate, like_rate, comment_rate))

            conn.commit()
            self.logger.info("Engagement metrics calculated")
    
        except Exception as e:
            self.logger.error(f"Failed to calculate engagement metrics: {e}")
            conn.rollback()
    
        finally:
            conn.close()

# ============ MAIN PROCESSING PIPELINE ============

    def run_full_pipeline(self):
        """
        Run complete data processing pipeline
        """
        self.logger.info('=' * 50)
        self.logger.info('Starting full data processing pipeline')
        self.logger.info('=' * 50)

        # Step 1: Clean data
        if not self.clean_all_data():
            self.logger.error('Data cleaning failed, stopping pipeline')
            return False
        
        # Step 2: Process videos
        self.process_all_videos()

        # Step 3: Process comments
        self.process_all_comments(force_reprocess=True)

        # Step 4: Calculate metrics
        self.calculate_engagement_metrics()

        self.logger.info('Pipeline completed successfully')
        return True




