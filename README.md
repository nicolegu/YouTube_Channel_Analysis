# YouTube Channel Analytics for Stationery Retailers

A data pipeline and analytics dashboard designed to help stationery retailers make informed inventory and brand decisions based on YouTube content performance and audience engagement.

## Project Goal

Small stationery retailers which carry multiple brands face challenges deciding which products to stock, especially when importing from foreign countries. This project analyzes YouTube channel data to reveal:

- Which brands and product categories generate the most customer interest
- Purchase intent signals from audience comments
- Brands competitors cover while you don't
- Content performance trends over time

## Dashboard Features

### Current Version (V1)
- **Daily Engagement Trends**: Compare video performance over time
- **Top Performing Videos**: Identify what content resonates with audience
- **Brand Engagement Analysis**: Track which brands drive highest engagement
- **Product Category Coverage**: Understand content distribution across product types

### Upcoming Version (V2)
- **Comment Sentiment Analysis**: Gauge audience reactions to brands and products
- **Purchase Intent Detection**: Identify comments expressing buying interest
- **Question Analysis**: Surface common customer inquiries
- **Video Description Parsing**: Extract brand mentions from video descriptions

### Components

1. **Data Collection** (`Metric_Tracker.py`)
   - Automate metric collection via YouTube Data API v3
   - Track channel stats, video info, and comments
   - Configurable collection strategies (time-based, recent_count, hybrid)
   - Handle internet connection issues

2. **Data Processing** (`data_processor.py`)
   - Video title cleaning and categorization
   - Brand extraction using keyword matching
   - Product category classification
   - Comment analysis
   - Engagement metric calculations

3. **Database** (`migrations/`)
   - SQLite with version-controlled schema migrations
   - Track raw metrics, comments, processed data, and engagement over time
   - Support historical trend analysis

4. **Dashboards** (`dashboard.py`)
   - Interactive Streamlit web interface
   - Multi-channel comparison
   - Customizable date range and filters

## Tech Stack

- **Python 3.9+**
- **YouTube Data API v3**
- **SQLite** - Local database
- **Streamlit** - Dashboard framework
- **Plotly** - Interactive visualizations
- **Pandas** - Data manipulation
- **NLTK/TextBlob** - Text processing
