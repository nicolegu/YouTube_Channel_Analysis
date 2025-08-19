from Video_Info_Extractor import YouTubeChannelExtractor
from config import YouTube_Data_API_KEY


def main():
    client = YouTubeChannelExtractor(api_key = YouTube_Data_API_KEY)

    channel_to_extract = 'https://www.youtube.com/@jetpens'
    max_videos = 1000
    max_comments_per_video = 100

    try:
        data = client.extract_channel_videos(channel_to_extract, max_videos, max_comments_per_video)

        if data:
            print("\n" + "="*50)
            print("EXTRACTION COMPLETE")
            print("="*50)

            summary = data['channel_summary']
            print(f"Channel: {summary['channel_name']}")
            print(f"Videos extracted: {summary['videos_extracted']}")
            print(f"Total comments: {summary['total_comments']}")

            videos = sorted(data['videos'], key = lambda x: x['view_count'], reverse=True)
            print(f"\nTop 5 videos by views:")
            for i, video in enumerate(videos[:5]):
                print(f"{i+1}. {video['title']:} {video['view_count']} views")

            # Save to files
            channel_name_safe = "".join(char for char in summary['channel_name'] if char.isalnum() or char in (' ', '-', '_')).rstrip()
            base_filename = f"youtube_data_{channel_name_safe}".replace(' ', '_')
            client.save_to_files(data, base_filename)

        else:
            print("Failed to extract data")

    except Exception as e:
        print(f"Error during extraction: {e}")
        print("Make sure your API key is valid and you have quota remaining")

if __name__ == '__main__':
    main()

