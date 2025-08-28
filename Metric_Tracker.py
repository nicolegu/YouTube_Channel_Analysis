import requests
import sqlite3
import json
import time
import schedule
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import logging


class YouTubeMetricsTracker:
    def __init__(self, api_key, db_path = 'youtube_metrics.db'):
        """
        Initialize the YouTube metrics tracker
        """
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.db_path = db_path
        self.setup_database()
        self.setup_logging()

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
    
    def add_channel_to_tracking(self, channel_identifier, track_videos = True, max_videos = 10):
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

        response = requests.get(url, params = params)
        data = response.json()

        if 'items' in data and data['items']:
            return data['items'][0]
        return None
    
    def extract_channel_id_from_url(self, url):
        """
        Extract channel ID from URL
        """
        if '/channel/' in url:
            return url.split('/channel/')[-1].split('/')[0]
        # Add other URL parsing logic as needed
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
                           INSERT INTO channel_metrics
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
        
    def collect_video_metrics(self, channel_id):
        """
        Collect metrics for top videos of a channel
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if we should track videos for this channel
        cursor.execute('''
                       SELECT track_videos, max_videos_to_track
                         FROM tracking_config
                        WHERE channel_id = ? AND active = 1
                       ''', (channel_id,))
        
        config = cursor.fetchone() # Return the next row query result
        if not config or not config[0]:
            conn.close()
            return
        
        max_videos = config[1]
        conn.close()

        try:
            # Get channel's upload playlist
            channel_info = self.get_channel_info(channel_id)
            if not channel_info:
                return
            
            uploads_playlist = channel_info['contentDetails']['relatedPlaylists']['uploads']

            # Get recent videos
            url = f"{self.base_url}/playlistItems"
            params = {
                'key': self.api_key,
                'playlistId': uploads_playlist,
                'part': 'contentDetails',
                'maxResults': max_videos
            }

            response = requests.get(url, params = params)
            data = response.json()

            if 'items' not in data:
                return
            
            video_ids = [item['contentDetails']['videoId'] for item in data['items']]

            # Get detailed video info
            video_details = self.get_video_details(video_ids)

            # Store video metrics
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            for video in video_details:
                snippet = video.get('snippet', {})
                statistics = video.get('statistics', {})
                content_details = video.get('contentDetails', {})

                cursor.execute('''
                               INSERT INTO video_metrics
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

    def setup_automated_collection(self, interval_hours = 12):
        """
        Setup automated metric collection
        """
        def job():
            self.logger.info("Running scheduled collection...")
            self.collect_all_tracked_channels()

        # Schedule the job
        schedule.every(interval_hours).hours.do(job)

        self.logger.info(f"Automated collection scheduled every {interval_hours} hours")
        self.logger.info(f"Run tracker.run_scheduler() to start the automated collection")

    def run_scheduler(self):
        """
        Run the automated scheduler
        """
        self.logger.info("Starting automated scheduler...")
        while True:
            schedule.run_pending()
            time.sleep(60)

    def export_data(self, channel_id, output_format = 'csv', days = None):
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
            base_query += " AND timestamp >= datetime('now', '-{} days')".format(days)
        
        base_query += " ORDER BY timestamp"

        df = pd.read_sql_query(base_query, conn, parse_dates=['timestamp'])
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
