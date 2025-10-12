from data_processor import YouTubeDataProcessor

def main():
     processor = YouTubeDataProcessor()
     processor.run_full_pipeline()

if __name__ == '__main__':
    main()