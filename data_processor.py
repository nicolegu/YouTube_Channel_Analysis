import sqlite3
from nltk.stem import WordNetLemmatizer
import re
import emoji
from datetime import datetime
import logging
import isodate
import json
from sentence_transformers import SentenceTransformer
import pandas as pd
from sklearn.cluster import DBSCAN
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
        processed_title = self.preprocess_text(title)

        # Categorize
        categories = self.categorize_text(processed_title['text_clean'])

        # Extract brands
        combined_text = f"{title or ''} {description or ''}"
        brands_found = self.extract_brands_from_text(combined_text)

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
            processed_title['text_clean'],
            duration_seconds,
            json.dumps(categories['products']),
            json.dumps(categories['content_types']),
            json.dumps(brands_found),
            json.dumps(processed_title['emojis']),
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

        # Extract brands from comment
        brands_in_comment = self.extract_brands_from_text(comment_text)

        # Product category extraction
        processed_comment = self.preprocess_text(comment_text)
        products_in_comment = self.categorize_text(processed_comment['text_clean'])
        
        # Store results
        cursor.execute("""
            UPDATE comments
            SET sentiment = ?,
                purchase_intent = ?,
                is_question = ?,
                emojis = ?,
                brands_mentioned = ?,
                product_categories = ?
            WHERE comment_id = ?
                 AND video_id = ?
        """, (results['sentiment'],
              results['purchase_intent'],
              results['is_question'],
              json.dumps(results['emojis']),
              json.dumps(brands_in_comment),
              json.dumps(products_in_comment['products']),
              comment_id,
              video_id
        ))
  
    def cluster_questions(self, channel_id = None, force_recluster = False):
        """
        Cluster questions by semantic similarity using sentence transformers
        Store cluster labels in comments table
        """      
        self.logger.info('Starting question clustering...')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Load sentence transformer model
            self.logger.info('Loading sentence transformer model...')
            model = SentenceTransformer('all_MiniLM-L6-v2')

            # Get list of channels to process
            if channel_id:
                channels = [(channel_id,)] # single element tuple
            else:
                cursor.execute("""
                    SELECT DISTINCT channel_id
                      FROM tracking_config
                     WHERE active = 1
                """)
                channels = cursor.fetchall()

            # Process each channel separately
            for (ch_id,) in channels:
                cursor.execute("""
                    SELECT channel_name
                      FROM tracking_config
                     WHERE channel_id = ?
                """, (ch_id,))
                channel_name = cursor.fetchall()[0]

                self.logger.info(f'\n{"="*50}')
                self.logger.info(f'Clustering questions for: {channel_name}')
                self.logger.info(f'{"="*50}')

                if force_recluster:
                    # Get all questions
                    cursor.execute("""
                        SELECT DISTINCT c.comment_id, c.comment_text
                          FROM comments c
                          JOIN processed_videos pv
                            ON c.video_id = pv.video_id
                         WHERE c.is_question = 1
                              AND pv.channel_id = ?
                    """, (ch_id,))
            
                else:
                    # Get only questions without cluster assignments
                    cursor.execute("""
                        SELECT DISTINCT c.comment_id, c.comment_text
                          FROM comments c
                          JOIN processed_videos pv
                            ON c.video_id = pv.video_id
                         WHERE c.is_question = 1
                              AND pv.channel_id = ?
                              AND (question_cluster_id IS NULL OR question_cluster_id = -1)
                    """, (ch_id,))
            
                questions = cursor.fetchall()

                if not questions:
                    self.logger.info(f'No questions to cluster for {channel_name}')
                    return
                
                if len(questions) < 5:
                    self.logger.info(f'Too few questions ({len(questions)}) for {channel_name}, skipping')
                    continue

                self.logger.info(f'Clustering {len(questions)} questions for {channel_name}...')

                # Extract comment IDs and texts
                comment_ids = [q[0] for q in questions]
                comment_texts = [q[1] for q in questions]

                # Generate embeddings
                self.logger.info('Generate embeddings...')
                embeddings = model.encode(comment_texts, show_progress_bar = True)

                # Perform clustering
                self.logger.info('Performing DBSCAN clustering...')
                clustering = DBSCAN(
                    eps = 0.5,      # Distance threshold for grouping
                    min_samples = 2, # Minimum questions per cluster
                    metric = 'cosine' # Use cosine similarity
                ).fit(embeddings)

                cluster_labels = clustering.labels_

                # Prefix cluster IDs with channel id to make them unique across channels
                # Format: "{channel_id}_{cluster_id}"
                cluster_labels_with_channelid = [
                    f"{ch_id}_{label}" if label != -1 else "-1"
                    for label in cluster_labels
                ]

                # Count clusters
                n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
                n_noise = list(cluster_labels).count(-1)

                self.logger.info(f'Found {n_clusters} clusters for {channel_name}')
                self.logger.info(f'{n_noise} questions marked as noise (unclustered)')
                
                # Store cluster labels
                for comment_id, cluster_id in zip(comment_ids, cluster_labels_with_channelid):
                    cursor.execute("""
                        UPDATE comments
                           SET question_cluster_id = ?
                         WHERE comment_id = ?
                    """, (cluster_id, comment_id))
                
                conn.commit()
                
                # Generate descriptive labels for clusters (pass original numeric labels)
                self._generate_cluster_labels(cursor, ch_id, channel_name, cluster_labels, comment_texts)
                conn.commit()

            self.logger.info('\nQuestions clustering completed for all channels')

        except Exception as e:
            self.logger.error(f'Question clustering failed: {e}')
            conn.rollback()

        finally:
            conn.close()

    def _generate_cluster_labels(self, cursor, channel_id, channel_name, cluster_labels, comment_texts):
        """
        Generate descriptive labels for each cluster based on content
        Store in a separate cluster_labels table
        """
        from collections import Counter
        import re

        unique_clusters = set(cluster_labels)
        unique_clusters.discard(-1) # Remove noise cluster

        stopwords = {'the', 'a', 'an', 'is', 'it', 'to', 'and', 'or', 'of', 'in', 'on', 
                     'for', 'with', 'this', 'that', 'what', 'where', 'how', 'can', 'do',
                     'does', 'i', 'you', 'your', 'my', 'are', 'have', 'be', 'been'}
        
        for cluster_id in sorted(unique_clusters):
            # Get all questions in this cluster
            indices = [i for i, label in enumerate(cluster_labels) if label == cluster_id]
            cluster_questions = [comment_texts[i] for i in indices]

            # Extract keywords from all questions in cluster
            all_words = []
            for question in cluster_questions:
                words = re.findall(r'\b[a-z]{3,}\b', question.lower())
                all_words.extend([w for w in words if w not in stopwords])

            # Find most common words
            word_counts = Counter(all_words)
            top_keywords = [word for word, count in word_counts.most_common(3)]

            # Create label from top keywords
            if top_keywords:
                label = ' + '.join(top_keywords).title()
            else:
                label = f"Topic {cluster_id}"

            # Store 3 example questions
            examples = cluster_questions[:3]
            examples_json = json.dumps(examples)

            # Create unique cluster_id with channel prefix
            unique_cluster_id = f"{channel_id}_{cluster_id}"

            # Insert or update cluster label
            cursor.execute("""
                INSERT OR REPLACE INTO question_cluster_labels
                (cluster_id, channel_id, channel_name, label, example_questions, question_count)
                VALUES(?, ?, ?, ?, ?, ?)
            """, (unique_cluster_id, channel_id, channel_name, label, examples_json, len(indices)))
            
            # Log cluster info with examples
            self.logger.info(f'Cluster {cluster_id} ({len(indices)} questions): {label}')
            for ex in examples:
                self.logger.info(f'{ex[:100]}...')

   
    # ============ TEXT PROCESSING HELPERS ============

    def preprocess_text(self, text):
        """
        Extract useful info from text with emoji handling
        Return cleaned text and emojis
        """
        if not text:
            return {'text_clean': '', 'emojis': []}

        text_no_emoji = emoji.replace_emoji(text, replace = '')
        text_cleaned = re.sub(r'[^\w\s\']', '', text_no_emoji)
        emojis_found = emoji.emoji_list(text)

        return {
            'text_clean': text_cleaned.strip(),
            'emojis': [e['emoji'] for e in emojis_found]
        }
    
    def categorize_text(self, text):
        """
        Categorize text by product type and content type
        """
        if not text:
            return {'products': [], 'content_types': []}
        
        wnl = WordNetLemmatizer()
        text_lower = text.lower()
        words_in_text = text_lower.split()
        words_in_text = [wnl.lemmatize(word) for word in words_in_text]
        text_cleaned = ' '.join(words_in_text)

        products = []
        found_content_types = []
        
        # Check product categories
        for category, keywords in product_keywords.items():
            if any(keyword in text_cleaned for keyword in keywords):
                products.append(category)
        
        # Check content types
        for category, keywords in content_types.items():
            if any(keyword in text_cleaned for keyword in keywords):
                found_content_types.append(category)

        return {'products': products, 'content_types': found_content_types}
        
    def extract_brands_from_text(self, text):
        """
        Extract brands from any text (title, description, comment)
        Return list of {"brands": str, "category": str}
        """
        if not text:
            return []
        
        text_lower = text.lower()

        # Split into words for exact matching
        words = re.findall(r'\b\w+\b', text_lower)
        words_set = set(words)

        brands_found = []
        brands_seen = set()

        for category, brand_list in brands.items():
            for brand in brand_list:
                brand_lower = brand.lower()

                # Skip if already found
                if brand in brands_seen:
                    continue

                # Multi-word brands (e.g., "TRAVELER'S COMPANY")
                if ' ' in brand_lower or "'" in brand_lower:
                    if brand_lower in text_lower:
                        brands_found.append({
                            'brand': brand,
                            'category': category
                        })
                        brands_seen.add(brand)
                # Single-word brands with exact matching
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
        
        processed_comment = self.preprocess_text(comment_text)
        text_lower = processed_comment['text_clean'].lower()
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
            'emojis': processed_comment['emojis']
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
        self.process_all_videos(force_reprocess=True)

        # Step 3: Process comments
        self.process_all_comments(force_reprocess=True)

        # Step 4: Calculate metrics
        self.calculate_engagement_metrics()

        self.logger.info('Pipeline completed successfully')
        return True




