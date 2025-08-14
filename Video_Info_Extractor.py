import requests
import json
from datetime import datetime
import pandas as pd
import time
import os

class YouTubeChannelExtractor:
    def __init__(self, api_key):
        """
        Initialize with your YouTube Data API v3 key
        """
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"

    def get_channel_id_from_username(self, username):
        """
        Get channel ID from username
        Example: 'https://www.youtube.com/user/MarquesBrownlee'
        """
        url = f"{self.base_url}/channels"
        params = {
            'key': self.api_key,
            'forUsername': username,
            'part': 'id,snippet'
        }

        response = requests.get(url, params = params)
        data = response.json()

        if 'items' in data and data['items']:
            return data['items'][0]['id']
        else:
            print(f"Channel not found for username: {username}")
            return None
    
    def get_channel_id_from_url(self, channel_url):
        """
        Extract channel ID from various YouTube url formats
        Examples: 'https://www.youtube.com/channel/UCBJycsmduvYEL83R_U4JriQ',
        'https://www.youtube.com/c/mkbhd', 'https://www.youtube.com/@mkbhd'
        """
        if '/channel/' in channel_url:
            return channel_url.split('/channel/')[-1].split('/')[0]
        elif '/c/' in channel_url:
            channel_name = channel_url.split('/c/')[-1].split('/')[0]
            return self.search_channel_by_name(channel_name)
        elif '/@' in channel_url:
            handle = channel_url.split('/@')[-1].split('/')[0]
            return self.search_channel_by_handle(handle)
        else:
            print("Unsupported URL format")
            return None
        
    def search_channel_by_name(self, channel_name):
        """
        Search for channel by name
        """
        url = f"{self.base_url}/search"
        params = {
            'key': self.api_key,
            'q':channel_name,
            'type': 'channel',
            'part': 'id',
            'maxResults': 1
        }

        response = requests.get(url, params = params)
        data = response.json()

        if 'items' in data and data['items']:
            return data['items'][0]['id']['channelId']
        return None
    
    def search_channel_by_handle(self, handle):
        """
        Search for channel by handle (new format)
        """
        url = f"{self.base_url}/search"
        params = {
            'key': self.api_key,
            'q': handle,
            'type': 'channel',
            'part': 'id',
            'maxResults': 5 # default value
        }

        response = requests.get(url, params = params)
        data = response.json()

        if 'items' in data and data['items']:
            return data['items'][0]['id']['channelId']
        return None
    
    def get_channel_uploads_playlist(self, channel_id):
        """
        Get the uploads playlist (containing all videos) ID for a channel
        """
        url = f"{self.base_url}/channels"
        params = {
            'key': self.api_key,
            'id': channel_id,
            'part': 'contentDetails,snippet,statistics'

        }

        response = requests.get(url, params = params)
        data = response.json()

        if 'items' in data and data['items']:
            channel_info = data['items'][0]
            uploads_playlist = channel_info['contentDetails']['relatedPlaylists']['uploads']
            channel_stats = channel_info.get('statistics', {})
            channel_snippet = channel_info.get('snippet', {})

            return uploads_playlist, channel_stats, channel_snippet
        
        return None, None, None
    
    def get_playlist_videos(self, playlist_id, max_results = 50):
        """
        Get videos from a playlist
        """
        videos = []
        next_page_token = None

        while len(videos) < max_results:
            url = f"{self.base_url}/playlistItems"
            params = {
                'key': self.api_key,
                'playlistId': playlist_id,
                'part': 'snippet,contentDetails',
                'maxResults': min(50, max_results - len(videos))
            }

            if next_page_token:
                params['pageToken'] = next_page_token

            response = requests.get(url, params=params)
            data = response.json()

            if 'items' not in data:
                break

            videos.extend(data['items'])
            next_page_token = data.get('nextPageToken')

            if not next_page_token:
                break

            time.sleep(0.1)

        return videos[:max_results]
    
    def get_video_details(self, video_ids):
        """
        Get detailed information for specific videos
        Batch process for efficiency
        """
        video_details = []

        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            ids_string = ','.join(batch_ids)

            url = f"{self.base_url}/videos"
            params = {
                'key': self.api_key,
                'id': ids_string,
                'part': 'snippet, statistics, contentDetails, status'
            }

            response = requests.get(url, params = params)
            data = response.json()

            if 'items' in data:
                video_details.extend(data['items'])

            # Rate limiting
            time.sleep(0.1)

        return video_details
    
    def extract_channel_videos(self, channel_identifier, max_videos = 100, truncate_description = False, description_limit = 500):
        """
        Main method to extract video information from a channel
        channel_identifier can be: channel_id, username, or channel url
        """
        print(f"Starting extraction for: {channel_identifier}")

        if channel_identifier.startswith('http'):
            channel_id = self.get_channel_id_from_url(channel_identifier)
        elif channel_identifier.startswith('UC') and len(channel_identifier) == 24:
            channel_id = channel_identifier
        else:
            channel_id = self.get_channel_id_from_username(channel_identifier)

        if not channel_id:
            print("Could not find channel ID")
            return None
        
        print(f"Channel ID: {channel_id}")

        # Get uploads playlist
        uploads_playlist, channel_stats, channel_info = self.get_channel_uploads_playlist(channel_id)
        
        print(f"Channel: {channel_info.get('title', 'Unknown')}")
        print(f"Subscribers: {channel_stats.get('subscriberCount', 'Hidden')}")
        print(f"Total Videos: {channel_stats.get('videoCount', 'Unknown')}")

        # Get playlist videos
        print(f"Fetching up to {max_videos} videos...")
        playlist_videos = self.get_playlist_videos(uploads_playlist, max_videos)
        
        video_ids = [item['contentDetails']['videoId'] for item in playlist_videos]

        print(f"Found {len(video_ids)} videos, getting detailed information...")

        video_details = self.get_video_details(video_ids)

        processed_videos = []
        for video in video_details:
            snippet = video.get('snippet', {})
            statistics = video.get('statistics', {})
            content_details = video.get('contentDetails', {})

            processed_video = {
                'video_id': video['id'],
                'title': snippet.get('title', ''),
                'description': snippet.get('description', '')[:500][:description_limit] if truncate_description else snippet.get('description', ''),
                'published_at': snippet.get('publishedAt', ''),
                'duration': content_details.get('duration', ''),
                'view_count': int(statistics.get('viewCount', 0)),
                'like_count': int(statistics.get('likeCount', 0)),
                'comment_count': int(statistics.get('commentCount', 0)),
                'tags': snippet.get('tags', []),
                'category_id': snippet.get('categoryId', ''),
                'thumbnail': snippet.get('thumbnails', {}).get('high', {}).get('url', ''),
                'url': f"https://www.youtube.com/watch?v={video['id']}"
            }

            processed_videos.append(processed_video)

        summary = {
            'channel_id': channel_id,
            'channel_name': channel_info.get('title', 'Unknown'),
            'channel_description': channel_info.get('description', ''),
            'subscriber_count': channel_stats.get('subscriberCount', 'Hidden'),
            'total_videos': channel_stats.get('videoCount', 'Unknown'),
            'total_views': channel_stats.get('viewCount', 'Unknown'),
            'videos_extracted': len(processed_videos),
            'extraction_date': datetime.now().isoformat()
        }

        return {
            'channel_summary': summary,
            'videos': processed_videos
        }
    
    def save_to_files(self, data, base_filename, output_dir = 'Data'):
        """
        Save extracted data to JSON and CSV files
        """
        if not data:
            print("No data to save")
            return
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Save complete data as JSON
        json_filename = os.path.join(output_dir, f"{base_filename}_complete.json")
        with open(json_filename, 'w', encoding = 'utf-8') as f:
            json.dump(data, f, indent = 2, ensure_ascii = False)
        print(f"Complete data saved to: {json_filename}")

        if data['videos']:
            csv_filename = os.path.join(output_dir, f"{base_filename}_videos.csv")
            df = pd.DataFrame(data['videos'])
            df.to_csv(csv_filename, index = False, encoding = 'utf-8')
            print(f"Videos data saved to: {csv_filename}")

        summary_filename = os.path.join(output_dir, f"{base_filename}_summary.txt")
        with open(summary_filename, 'w', encoding='utf-8') as f:
            summary = data['channel_summary']
            f.write(f"Channel: {summary['channel_name']}\n")
            f.write(f"Subscribers: {summary['subscriber_count']}\n")
            f.write(f"Total Videos: {summary['total_videos']}\n")
            f.write(f"Videos Extracted: {summary['videos_extracted']}\n")
            f.write(f"Extraction Date: {summary['extraction_date']}\n")
            f.write(f"Description: {summary['channel_description']}\n")
        print(f"Summary saved to: {summary_filename}")




