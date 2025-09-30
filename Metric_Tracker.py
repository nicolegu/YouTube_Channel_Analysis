import requests
import sqlite3
import json
import time
import schedule
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import logging
import threading


class YouTubeMetricsTracker:
    def __init__(self, api_key, max_retries = 3, retry_delay = 60, db_path = 'youtube_metrics.db'):
        """
        Initialize the YouTube metrics tracker
        """
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.db_path = db_path
        self.setup_logging()
        self.setup_database()

    def setup_logging(self):
        """
        Setup logging for tracking operations
        """
        logging.basicConfig(
            level = logging.INFO,
            format = '%(asctime)s - %(levelname)s - %(message)s',
            handlers = [logging.FileHandler('youtube_tracker.log'),
                        logging.StreamHandler()
                        ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_database(self):
        """
        Create database tables for storing metrics over time
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Channel metrics table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS channel_metrics (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           channel_id TEXT NOT NULL,
                           channel_name TEXT,
                           timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                           subscriber_count INTEGER,
                           video_count INTEGER,
                           view_count INTEGER,
                           custom_url TEXT,
                           country TEXT,
                           published_at TEXT
                       )
                       ''')
        
        # Video metrics table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS video_metrics (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           video_id TEXT NOT NULL,
                           channel_id TEXT NOT NULL,
                           title TEXT,
                           timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                           view_count INTEGER,
                           like_count INTEGER,
                           comment_count INTEGER,
                           duration TEXT,
                           published_at TEXT)
                       ''')
        # Comments table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS comments (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           comment_id TEXT NOT NULL,
                           video_id TEXT NOT NULL,
                           author_name TEXT NOT NULL,
                           author_channel_id TEXT NOT NULL,
                           comment_text TEXT,
                           like_count INTEGER,
                           published_at TEXT,
                           updated_at TEXT,
                           reply_count INTEGER,
                           is_reply BOOLEAN DEFAULT 0,
                           parent_comment_id TEXT)
                       ''')
        
        # Channel config table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS tracking_config (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           channel_id TEXT NOT NULL UNIQUE,
                           channel_name TEXT,
                           track_videos BOOLEAN DEFAULT 0,
                           max_videos_to_track INTEGER DEFAULT 10,
                           added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                           last_updated DATETIME,
                           active BOOLEAN DEFAULT 1)
                       ''')
        
        conn.commit()
        conn.close()
        self.logger.info("Database setup completed")
    
    def add_channel_to_tracking(self, channel_identifier, track_videos = True, max_videos = 50):
        """
        Add a channel to the tracking list
        """
        # Get channel info
        channel_info = self.get_channel_info(channel_identifier)
        if not channel_info:
            self.logger.error(f"Could not find channel: {channel_identifier}")
            return False
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                           INSERT OR REPLACE INTO tracking_config
                           (channel_id, channel_name, track_videos, max_videos_to_track, last_updated)
                           VALUES (?, ?, ?, ?, ?)
                           ''', (
                               channel_info['id'],
                               channel_info['snippet']['title'],
                               track_videos,
                               max_videos,
                               datetime.now()
                           ))
            conn.commit()
            self.logger.info(f"Added channel to tracking: {channel_info['snippet']['title']}")

            self.collect_channel_metrics(channel_info['id'])

            return True
        
        except Exception as e:
            self.logger.error(f"Error adding channel to tracking: {e}")
            return False
        
        finally:
            conn.close()

    def get_channel_info_with_retry(self, channel_identifier):
        """
        Get channel info with retry logic for network errors
        """
        for attempt in range(self.max_retries):
            try:
                return self.get_channel_info(channel_identifier)
            except (ConnectionError, TimeoutError, requests.exceptions.RequestException) as e:
                self.logger.warning(f"API call failed (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error("Max retries reached for API call.")
                    return None

    def get_channel_info(self, channel_identifier):
        """
        Get basic channel information
        """
        if channel_identifier.startswith('UC') and len(channel_identifier) == 24:
            channel_id = channel_identifier
        elif channel_identifier.startswith('http'):
            channel_id = self.extract_channel_id_from_url(channel_identifier)
        else:
            channel_id = self.search_channel_by_name(channel_identifier)
        
        if not channel_id:
            return None
        
        url = f"{self.base_url}/channels"
        params = {
            'key': self.api_key,
            'id': channel_id,
            'part': 'snippet,statistics,contentDetails'
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'items' in data and data['items']:
                return data['items'][0]
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise

    def collect_metrics_with_retry(self, channel_identifier):
        """
        Collect metrics with retry logic
        """
        for attempt in range(self.max_retries):
            try:
                return self.collect_all_tracked_channels()
            except (ConnectionError, TimeoutError, requests.exceptions.RequestException) as e:
                self.logger.warning(f"Connection failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error("Max retries reached. Skipping this collection cycle.")
                    return None

    def extract_channel_id_from_url(self, url):
        """
        Extract channel ID from URL
        """
        if '/channel/' in url:
            return url.split('/channel/')[-1].split('/')[0]
        elif '/c/' in url:
            channel_name = url.split('/c/')[-1].split('/')[0]
            return self.search_channel_by_name(channel_name)
        elif '/@' in url:
            handle = url.split('/@')[-1].split('/')[0]
            return self.search_channel_by_name(handle)
        # Add other URL parsing logic as needed
        else:
            return None
    
    def search_channel_by_name(self, name):
        """
        Search for channel by name
        """
        url = f"{self.base_url}/search"
        params = {
            'key': self.api_key,
            'q': name,
            'type': 'channel',
            'part': 'id',
            'maxResults': 1
        }

        response = requests.get(url, params = params)
        data = response.json()

        if 'items' in data and data['items']:
            return data['items'][0]['id']['channelId']
        return None
    
    def collect_channel_metrics(self, channel_id):
        """
        Collect and store current channel metrics
        """
        try:
            channel_info = self.get_channel_info(channel_id)
            if not channel_info:
                self.logger.error(f"Could not fetch data for channel: {channel_id}")
                return False
            
            snippet = channel_info.get('snippet', {})
            statistics = channel_info.get('statistics', {})

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                           INSERT OR REPLACE INTO channel_metrics
                           (channel_id, channel_name, subscriber_count, video_count, view_count,
                            custom_url, country, published_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ''', (
                               channel_id,
                               snippet.get('title', ''),
                               int(statistics.get('subscriberCount', 0)),
                               int(statistics.get('videoCount', 0)),
                               int(statistics.get('viewCount', 0)),
                               snippet.get('customUrl', ''),
                               snippet.get('country', ''),
                               snippet.get('publishedAt', '')
                           ))
            
            conn.commit()
            conn.close()

            self.logger.info(f"Collected metrics for channel: {snippet.get('title', channel_id)}")

            # Also collect video metrics if enabled
            self.collect_video_metrics(channel_id)

            return True
        
        except Exception as e:
            self.logger.error(f"Error collecting channel metrics: {e}")
            return False
        
    def update_tracking_strategy(self, channel_id, strategy):
        """
        Update tracking strategy with validation
        Alternative way to impose restrictions after initializing tables in SQLite
        """
        valid_strategies = {'time_based', 'recent_count', 'hybrid'}

        if strategy not in valid_strategies:
            raise ValueError(f"Invalid strategy: {strategy}. Must be one of {valid_strategies}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE tracking_config
            SET video_tracking_strategy = ?
            WHERE channel_id = ?
        ''', (strategy, channel_id))

        conn.commit()
        conn.close()

        
    def get_videos_to_track(self, channel_id):
        """
        Get videos that should be tracked based on strategy
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get channel config
        cursor.execute('''
                       SELECT video_tracking_strategy, video_tracking_days, max_videos_to_track
                         FROM tracking_config
                        WHERE channel_id = ?
                       ''', (channel_id,))
        
        config = cursor.fetchone()

        if config and len(config) >= 3:
            strategy, days, max_videos = config[0] or 'time_based', config[1] or 30, config[2] or 50
        else:
            strategy, days, max_videos = 'time_based', 30, 50

        if strategy == 'time_based':
            # Track videos published within timeframe
            query = '''
                SELECT video_id FROM video_metrics
                 WHERE channel_id = ?
                      AND published_at >= datetime('now', '-' || ? || ' days')
                 ORDER BY published_at DESC
            '''
            cursor.execute(query, (channel_id, days))

        elif strategy == 'recent_count':
            # Track most recent videos: fixed number
            query = '''
                SELECT video_id FROM video_metrics
                 WHERE channel_id = ?
                 ORDER BY published_at DESC
                 LIMIT ?
            '''
            cursor.execute(query, (channel_id, max_videos))

        elif strategy == 'hybrid':
            # Use time window but cap at max_videos_to_track
            query = '''
                SELECT video_id FROM video_metrics
                 WHERE channel_id = ?
                      AND published_at >= datetime('now', '-' || ? || ' days')
                 ORDER BY published_at DESC
                 LIMIT ?
            '''
            cursor.execute(query, (channel_id, days, max_videos))

        else:
            # Invalid strategy, default to time_based
            self.logger.warning(f"Invalid startegy '{strategy}', defaulting to time_based")
            query = '''
                SELECT video_id FROM video_metrics
                 WHERE channel_id = ?
                      AND published_at >= datetime('now', '-' || ? || ' days')
                 ORDER BY published_at DESC

            '''
            cursor.execute(query, (channel_id, days))
        
        videos = cursor.fetchall()
        conn.close()
        return [v[0] for v in videos]
        
    def collect_video_metrics(self, channel_id):
        """
        Collect metrics for videos based on tracking strategy
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if we should track videos for this channel
        cursor.execute('''
                       SELECT track_videos, video_tracking_strategy, video_tracking_days, max_videos_to_track
                         FROM tracking_config
                        WHERE channel_id = ? AND active = 1
                       ''', (channel_id,))
        
        config = cursor.fetchone() # Return the next row query result
        if not config or not config[0]: # Check if there is an additional channel to track
            conn.close()
            return
        
        # Handle pre-migration databases
        if len(config) >= 4:
            track_videos, strategy, days, max_videos = config
            strategy = strategy or 'recent_count'
            days = days or 30
            max_videos = max_videos or 50
        else:
            track_videos, max_videos = config[0], config[1]
            strategy = 'recent_count'
            days = 30

        conn.close()

        try:
            # Get channel's upload playlist
            channel_info = self.get_channel_info(channel_id)
            if not channel_info:
                return
            
            uploads_playlist = channel_info['contentDetails']['relatedPlaylists']['uploads']
            
            # Fetch ALL recent videos from API (or a resonable number)
            api_fetch_limit = max(max_videos, 50)

            # Get recent videos
            url = f"{self.base_url}/playlistItems"
            params = {
                'key': self.api_key,
                'playlistId': uploads_playlist,
                'part': 'contentDetails',
                'maxResults': api_fetch_limit
            }

            response = requests.get(url, params = params)
            data = response.json()

            if 'items' not in data:
                return
            
            video_ids = [item['contentDetails']['videoId'] for item in data['items']]

            # Get detailed video info for all fetched videos
            video_details = self.get_video_details(video_ids)

            videos_to_store = []
            cutoff_date = datetime.now() - timedelta(days = days)

            if strategy == 'time_based':
                # Store videos published within the time window
                for video in video_details:
                    published_str = video.get('snippet', {}).get('publishedAt', '')
                    if published_str:
                        published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                        if published_date >= cutoff_date:
                            videos_to_store.append(video)

            elif strategy == 'recent_count':
                # Store only the N most recent videos
                videos_to_store = video_details[:max_videos]

            elif strategy == 'hybrid':
                # Store videos within time window, capped at max count
                for video in video_details:
                    published_str = video.get('snippet', {}).get('publishedAt', '')
                    if published_str:
                        published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                        if published_date >= cutoff_date:
                            videos_to_store.append(video)
                            if len(videos_to_store) >= max_videos:
                                break
            else:
                self.logger.warning(f"Invalid startegy '{strategy}', defaulting to recent_count")
                videos_to_store = video_details[:max_videos]

            # Store video metrics
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            for video in video_details:
                snippet = video.get('snippet', {})
                statistics = video.get('statistics', {})
                content_details = video.get('contentDetails', {})

                cursor.execute('''
                               INSERT OR REPLACE INTO video_metrics
                               (video_id, channel_id, title, view_count, like_count,
                                comment_count, duration, published_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                video['id'],
                                channel_id,
                                snippet.get('title', ''),
                                int(statistics.get('viewCount', 0)),
                                int(statistics.get('likeCount', 0)),
                                int(statistics.get('commentCount', 0)),
                                content_details.get('duration', ''),
                                snippet.get('publishedAt', '')
                            ))
            conn.commit()
            conn.close()

            self.logger.info(f"Collected metrics for {len(video_details)} videos")

        except Exception as e:
            self.logger.error(f"Error collecting video metrics: {e}")
            return False
        
    def get_video_details(self, video_ids):
        """
        Get detailed information for videos
        """
        video_details = []

        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            ids_string = ','.join(batch_ids)

            url = f"{self.base_url}/videos"
            params = {
                'key': self.api_key,
                'id': ids_string,
                'part': 'snippet,statistics,contentDetails'
            }

            response = requests.get(url, params=params)
            data = response.json()

            if 'items' in data:
                video_details.extend(data['items'])

            time.sleep(0.1) # Rate limiting

        return video_details
    
    def get_video_comments(self, video_id, max_results = 100, order = 'time', include_replies = False):
        """
        Collect and store comments for a specific video
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        comments_collected = 0
        next_page_token = None

        self.logger.info(f"Collecting comments for video: {video_id} (max: {max_results})")

        try:
            while comments_collected < max_results:
                batch_size = min(100, max_results - comments_collected)

                url = f"{self.base_url}/commentThreads"
                params = {
                    'key': self.api_key,
                    'videoId': video_id,
                    'part': 'snippet,replies',
                    'maxResults': batch_size,
                    'order': order,
                    'textFormat': 'plainText'
                }

                if next_page_token:
                    params['pageToken'] = next_page_token

                response = requests.get(url, params = params, timeout = 30)
                response.raise_for_status()
                data = response.json()

                if 'items' not in data or not data['items']:
                    self.logger.info("No more comments found")
                    break

                for item in data['items']:
                    try:
                        top_comment = item['snippet']['topLevelComment']['snippet']
                        comment_id = item['snippet']['topLevelComment']['id']

                        cursor.execute('''
                                       INSERT OR REPLACE INTO comments
                                       (comment_id, video_id, author_name, author_channel_id, comment_text,
                                        like_count, published_at, updated_at, reply_count, is_reply, parent_comment_id)
                                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                       ''', (
                                           comment_id,
                                           video_id,
                                           top_comment['authorDisplayName'],
                                           top_comment.get('authorChannelId', {}).get('value', ''),
                                           top_comment['textDisplay'],
                                           top_comment['likeCount'],
                                           top_comment['publishedAt'],
                                           top_comment['updatedAt'],
                                           item['snippet']['totalReplyCount'],
                                           False,
                                           None
                                       ))
                        comments_collected += 1

                        # Handle replies if requested and available
                        if include_replies and item['snippet']['totalReplyCount'] > 0:
                            if 'replies' in item and 'comments' in item['replies']:
                                for reply in item['replies']['comments']:
                                    if comments_collected >= max_results:
                                        break

                                reply_snippet = reply['snippet']

                                cursor.execute('''
                                               INSERT OR REPLACE INTO comments
                                               (comment_id, video_id, author_name, author_channel_id, comment_text,
                                                like_count, published_at, updated_at, reply_count, is_reply, parent_comment_id)
                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                               ''', (
                                                   reply['id'],
                                                   video_id,
                                                   reply_snippet['authorDisplayName'],
                                                   reply_snippet.get('authorChannelId', {}).get('value', ''),
                                                   reply_snippet['textDisplay'],
                                                   reply_snippet['likeCount'],
                                                   reply_snippet['publishedAt'],
                                                   reply_snippet['updatedAt'],
                                                   0,
                                                   True,
                                                   comment_id
                                               ))
                                comments_collected += 1

                                if comments_collected >= max_results:
                                    break
                    except Exception as e:
                        self.logger.warning(f"Error processing comment: {e}")
                        continue
                conn.commit()

                next_page_token = data.get('nextPageToken')
                if not next_page_token:
                    self.logger.info("Reached end of available comments")
                    break

                self.logger.info(f"Collected {comments_collected} comments so far...")
                time.sleep(0.1)

        except requests.exceptions.RequestException as e:
                self.logger.error(f"API error collecting comments: {e}")
                return False
        except Exception as e:
                self.logger.error(f"Unexpected error collecting comments: {e}")
                return False
        finally:
                conn.close()

        self.logger.info(f"Successfully collected {comments_collected} comments for video {video_id}")
        return comments_collected
    
    def collect_comments_for_tracked_videos(self, days_back = 7, max_comments_per_video = 50):
        """
        Collect comments for recent videos from all tracked channels
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get recent videos from tracked channels
        cursor.execute('''
                       SELECT DISTINCT vm.video_id, vm.title, vm.channel_id
                         FROM video_metrics vm
                         JOIN tracking_config tc ON vm.channel_id = tc.channel_id
                        WHERE tc.active = 1
                             AND vm.published_at >= datetime('now', '-' || ? || ' days')
                        ORDER BY vm.timestamp DESC
                       ''', (days_back,))
        
        videos = cursor.fetchall()
        conn.close()

        self.logger.info(f"Collecting comments for {len(videos)} recent videos")

        for video_id, title in videos:
            self.logger.info(f"Collecting comments for: {title}")
            self.get_video_comments(video_id, max_comments_per_video)
            time.sleep(1)

        return len(videos)
    
    def collect_all_tracked_channels(self):
        """
        Collect metrics for all active tracked channels
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT channel_id, channel_name
              FROM tracking_config
              WHERE active = 1
            ''')
        
        channels = cursor.fetchall()
        conn.close()

        self.logger.info(f"Starting collection for {len(channels)} tracked channels")

        for channel_id, channel_name in channels:
            self.logger.info(f"Collecting metrics for {channel_name}")
            self.collect_channel_metrics(channel_id)
            time.sleep(1)

        self.logger.info("Collection completed for all tracked channels")

    def collect_metrics_with_retry(self):
        """
        Collect metrics with retry logic
        """
        for attempt in range(self.max_retries):
            try:
                return self.collect_all_tracked_channels()
            except (ConnectionError, TimeoutError, requests.exceptions.RequestException) as e:
                self.logger.info(f"Connection failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error("Max retries reached. Skipping this collection cycle.")
                    return None

    def start_automated_collection(self, interval_hours = 1, run_immediately = True):
        """
        Start automated metric collection
        """
        def job():
            self.logger.info("Running scheduled collection...")
            self.collect_metrics_with_retry()

        # Schedule the job
        schedule.every(interval_hours).hours.do(job)

        self.logger.info(f"Automated collection scheduled every {interval_hours} hours")
        
        # Optionally run immediately
        if run_immediately:
            self.logger.info("Running initial collections...")
            self.collect_metrics_with_retry()

        self.logger.info("Starting automated scheduler...")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("Automated collection stopped by user")
        except Exception as e:
            self.logger.error(f"Scheduler error: {e}")

    def start_automated_collection_background(self, interval_hours = 1, run_immediately = True):
        """
        Start automated collection in a background thread
        Returns the thread object so you can control it
        """
        def scheduler_worker():
            self.start_automated_collection(interval_hours, run_immediately)

        scheduler_thread = threading.Thread(target = scheduler_worker, daemon = True)
        scheduler_thread.start()
        self.logger.info("Started automated collection in background")
        return scheduler_thread

    def stop_automated_collection(self):
        """
        Stop all scheduled jobs
        """
        schedule.clear()
        self.logger.info("Stopped automated collection")

    def export_data(self, channel_id, output_format = '.csv', days = None):
        """
        Export tracking data
        """
        conn = sqlite3.connect(self.db_path)

        # Base query
        base_query = '''
                     SELECT * FROM channel_metrics
                      WHERE channel_id = ?
                     '''
        
        params = [channel_id]
        if days:
            base_query += " AND timestamp >= datetime('now', '-' || ? || ' days')"

            params = [channel_id, days]
        
        base_query += " ORDER BY timestamp"

        df = pd.read_sql_query(base_query, conn, params = params, parse_dates=['timestamp'])
        conn.close()

        if df.empty:
            self.logger.warning(f"No data to export for channel {channel_id}")
            return None
        
        # Generate filename
        channel_name = df.iloc[0]['channel_name'].replace(' ', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if output_format.lower() == '.csv':
            filename = f"{channel_name}_metrics_{timestamp}.csv"
            df.to_csv(filename, index=False)

        elif output_format.lower() == '.json':
            filename = f"{channel_name}_metrics_{timestamp}.json"
            df.to_json(filename, orient='records', date_format='iso')
        
        else:
            self.logger.error(f"Unsupported format: {output_format}")
            return None
        
        self.logger.info(f"Data exported to: {filename}")
        return filename
