import streamlit as st
import pandas as pd
import sqlite3
import json
import plotly.express as px
from datetime import datetime, timedelta

db_path = 'youtube_metrics.db'
conn = sqlite3.connect(db_path)

st.set_page_config(page_title='Stationery Channel Analytics', layout='wide')

st.title('YouTube Channel Analytics Dashboard')

# Color palette
COLORS = {
    'primary': '#2C3E50',
    'accent': '#E74C3C',
    'success': '#27AE60',
    'info': '#3498DB'
}

# Sidebar for channel selection
with st.sidebar:
    st.header('Filters')
    channels = st.multiselect(
        'Select channels:',
        ['Jetpens', 'Yoseka Stationary'],
        default = ['Jetpens']
    )

if not channels:
    st.warning('Please select at least one channel')
    st.stop()

# Convert to SQL format
channel_list = "'" + "','".join(channels) + "'"

# ============ ROW 1: Daily Engagement Trend ============
st.subheader('Daily Engagement Trend')

query = """
    SELECT DATE(vem.timestamp) AS date, AVG(vem.engagement_rate) AS avg_engagement
      FROM video_engagement_metrics vem
      JOIN tracking_config tc
        ON vem.channel_id = tc.channel_id
     WHERE tc.channel_name IN ({channel_list})
     GROUP BY DATE(vem.timestamp)
     ORDER BY DATE(vem.timestamp)
"""
df_trend = pd.read_sql_query(query, conn)

if not df_trend.empty:
    fig = px.line(
        df_trend,
        x = 'date',
        y = 'avg_engagement',
        markers = True,
        color_discrete_sequence = [COLORS['info']],
        labels = {'date': 'Date',
                  'avg_engagement': 'Avg Engagement Rate'}
    )
    fig.update_layout(
        hovermode = 'x unified',
        height = 400,
        showlegend = False
    )
    st.plotly_chart(fig, use_container_width = True)


# Top 10 videos
st.subheader('Top Performing Videos')
top_videos = pd.read_sql_query("""
    SELECT title, engagement_rate, view_count
      FROM processed_videos pv
      JOIN video_engagement_metrics vem
        ON pv.video_id = vem.video_id
     ORDER BY engagement_rate DESC
     LIMIT 10
""", conn)
st.dataframe(top_videos)

# Engagement by brands mentioned (bar chart)
st.subheader('Engagement by Brand')
query = """
    SELECT pv.video_id, pv.brands_mentioned, vem.engagement_rate
      FROM processed_videos pv
      LEFT JOIN video_engagement_metrics vem
        ON pv.video_id = vem.video_id
     WHERE vem.id IN (
         SELECT MAX(id) FROM video_engagement_metrics GROUP BY video_id
     )
"""

df = pd.read_sql_query(query, conn)

# Parse brands
brands_list = []
for idx, row in df.iterrows():
    if row['brands_mentioned']:
        brands = json.loads(row['brands_mentioned'])
        for brand_obj in brands:
            brands_list.append({
                'brand': brand_obj['brand'],
                'category': brand_obj['category'],
                'engagement_rate': row['engagement_rate']
            })

brands_df = pd.DataFrame(brands_list)

# Aggregate by brand
brand_engagement = brands_df.groupby('brand')['engagement_rate'].median().sort_values(ascending=False)

# Bar chart
st.bar_chart(brand_engagement)

# Product category bar chart
st.subheader('Product Pie Chart')
query = """
    SELECT video_id, product_categories
      FROM processed_videos
"""

df = pd.read_sql_query(query, conn)

# Parse products
product_category_list = {}
for idx, row in df.iterrows():
    if row['product_categories']:
        product_categories = json.loads(row['product_categories'])
        for cat in product_categories:
            product_category_list[cat] = product_category_list.get(cat, 0) + 1

st.bar_chart(pd.Series(product_category_list))

