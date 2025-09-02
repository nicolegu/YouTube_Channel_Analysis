from Metric_Tracker import YouTubeMetricsTracker
from config import YouTube_Data_API_KEY

def main_track():
    api_key = YouTube_Data_API_KEY
    tracker = YouTubeMetricsTracker(api_key=api_key)
    channel_identifier = 'https://www.youtube.com/@jetpens'

    success = tracker.add_channel_to_tracking(channel_identifier = channel_identifier)
    if success:
        print("Channel added to tracking!")
        channel_info = tracker.get_channel_info(channel_identifier)
        if channel_info:
            channel_id = channel_info['id']
            print(f"Channel ID: {channel_id}")
            print(f"Channel Name: {channel_info['snippet']['title']}")

            thread = tracker.start_automated_collection_background(interval_hours = 12)
            print("Background collection started!")

            file = tracker.export_data(channel_id = channel_id, days = 7)
            if file:
                print(f"Data exported to {file}")
            else:
                print("No data to export yet (channel just added)")
            print("\nAutomated collection is running in background...")
            print("Use command 'export' to export data and command 'quit' to stop collection")

            try:
                while True:
                    user_input = input("Enter command: ").strip().lower()

                    if user_input == 'export':
                        file = tracker.export_data(channel_id = channel_id, days = 7)

                        if file:
                            print(f"Data exported to {file}")
                        else:
                            print("No data available to export")

                    if user_input == 'quit':
                        print("Stopping automated collection...")
                        tracker.stop_automated_collection()
                        break

                    else:
                        print("Available commands: 'export', 'quit'")

            except KeyboardInterrupt:
                print("Stopping automated collection...")
                tracker.stop_automated_collection()

        else:
            print("Failed to get channel information!")
    
    else:
        print("Failed to add channel for tracking!")         

if __name__ == '__main__':
    main_track()