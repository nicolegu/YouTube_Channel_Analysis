import requests
import sys
import signal
import time
from datetime import datetime, timedelta
from Metric_Tracker import YouTubeMetricsTracker
from config import YouTube_Data_API_KEY

def signal_handler(sig, frame):
    print('Collection stopped gracefully')
    sys.exit(0)

def check_internet_connection(timeout=5):
    try:
        response = requests.get('https://www.google.com', timeout = timeout)
        return response.status_code == 200
    except:
        return False
    
def wait_for_internet(max_wait_minutes = 30):
    """
    Wait for internet connection to return
    """
    print("Checking internet connection...")
    wait_time = 0
    while not check_internet_connection():
        if wait_time >= max_wait_minutes * 60:
            print(f"No internet connection after {max_wait_minutes} minutes")
            return False
        print("No internet connection. Waiting 30 seconds...")
        time.sleep(30)
        wait_time += 30

    print("Internet connection restored!")
    return True

def format_time_remaining(end_time):
    """
    Format the remaining time in a readable format

    Args:
        end_time (datetime): the end time for collection

    Returns:
        str: Formatted time remaining
    """
    remaining = end_time - datetime.now()
    if remaining.total_seconds() <= 0:
        return "Collection period ended"
    
    days = remaining.days
    hours, remainder = divmod(remaining.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    if days > 0:
        return f"{days} days, {hours} hours, {minutes} minutes"
    elif hours > 0:
        return f"{hours} hours, {minutes} minutes"
    else:
        return f"{minutes} minutes"

def main(days = 7, hours = 0, minutes = 0):
    signal.signal(signal.SIGINT, signal_handler)

    tracker = YouTubeMetricsTracker(api_key=YouTube_Data_API_KEY)

    # Calculate custom end time
    total_minutes = days * 24 * 60 + hours * 60 + minutes
    end_time = datetime.now() + timedelta(minutes = total_minutes)

    print(f"Custom collection duration: {days} days, {hours} hours, {minutes} minutes")
    print(f"Collection will end at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    channel_identifiers = ['https://www.youtube.com/@jetpens', 'https://www.youtube.com/@Yoseka']
    
    for channel in channel_identifiers:
        if not check_internet_connection():
            print("No internet connection detected!")
            if not wait_for_internet():
                print("Exiting - no internet connection")
                return
            
        success = tracker.add_channel_to_tracking(channel_identifier = channel)
        if success:
            print("Channel added for tracking!")

            channel_info = tracker.get_channel_info(channel_identifier = channel)

            if channel_info:
                channel_id = channel_info['id']
                print(f"Channel ID: {channel_id}")
                print(f"Channel Name: {channel_info['snippet']['title']}") 

    thread = tracker.start_automated_collection_background(metric_interval_hours = 1, run_immediately = True,
                                                           collect_comments = True, comment_interval_hours = 6)
    print("Background collection started!")
    print("Press Ctrl+C to stop gracefully")

    try:
        while datetime.now() < end_time:
            if not check_internet_connection():
                print(f"Internet connection lost at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                if not wait_for_internet():
                    print("Exiting due to prolonged internet outage")
                    break
            time_remaining = format_time_remaining(end_time)
            print(f"Collection running... | Time remaining: {time_remaining}")
            time.sleep(60)

        print(f"Custom duration collection completed!")

    except KeyboardInterrupt:
        print("Collection manually stopped")
    finally:
        tracker.stop_automated_collection()



if __name__ == '__main__':
    main(days = 1, hours = 0, minutes = 0)
