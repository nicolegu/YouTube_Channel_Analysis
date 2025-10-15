import streamlit as st
import pandas as pd
import sqlite3
import json
import plotly.express as px
from datetime import datetime, timedelta

db_path = 'youtube_metrics.db'
conn = sqlite3.connect(db_path)

query = 'SELECT DISTINCT channel_id, channel_name FROM tracking_config WHERE active = 1'
channel_mapping = pd.read_sql_query(query, conn).set_index('channel_name')['channel_id'].to_dict()

available_channels = list(channel_mapping.keys())

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
    selected_channel_names = st.multiselect(
        'Select channels:',
        available_channels
    )


if not selected_channel_names:
    st.warning('Please select at least one channel')
    st.stop()

# Convert to SQL format
selected_channel_ids = [channel_mapping[name] for name in selected_channel_names]
channel_id_list = "'" + "','".join(selected_channel_ids) + "'"

# ============ ROW 1: Daily Engagement Trend ============
st.subheader('Daily Engagement Trend')

query = """
    SELECT DATE(vem.timestamp) AS date, AVG(vem.engagement_rate) AS avg_engagement
      FROM video_engagement_metrics vem
      JOIN processed_videos pv
        ON vem.video_id = pv.video_id
     WHERE pv.channel_id IN ({channel_id_list})
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
    # Modify properties of figure's layout (titles, legends, etc.)
    fig.update_layout(
        hovermode = 'x unified',
        height = 400,
        showlegend = False
    )
    st.plotly_chart(fig, use_container_width = True)
else:
    st.info('No engagement data available for selected channels')

# ============ ROW 2: Top Videos & Engagement by Brand ============
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader('Top Performing Videos')
    query = """
        SELECT title, engagement_rate, view_count
          FROM processed_videos pv
          JOIN video_engagement_metrics vem
            ON pv.video_id = vem.video_id
         WHERE pv.channel_id IN ({channel_id_list})
              AND vem.id IN (
                  SELECT MAX(id) FROM video_engagement_metrics
                   WHERE video_id IN (SELECT video_id FROM processed_videos WHERE channel_id IN ({channel_id_list}))
                   GROUP BY video_id
              )
         ORDER BY vem.engagement_rate DESC
         LIMIT 10
"""
    top_videos = pd.read_sql_query(query, conn)

    if not top_videos.empty:
        top_videos['engagement_rate'] = (top_videos['engagement_rate'] * 100).round(2)
        st.dataframe(
            top_videos.rename(columns={
                'title': 'Title',
                'engagement_rate': 'Engagement %',
                'view_count': 'Views'
            }),
           use_container_width = True,
           height = 400
        )
    else:
        st.info('No video data available')

with col2:
# Engagement by brands mentioned (bar chart)
    st.subheader('Engagement by Brand')
    query = f"""
        SELECT pv.video_id, pv.brands_mentioned, vem.engagement_rate
          FROM processed_videos pv
          LEFT JOIN video_engagement_metrics vem
            ON pv.video_id = vem.video_id
         WHERE pv.channel_id IN ({channel_id_list})
              AND vem.id IN (
                  SELECT MAX(id) FROM video_engagement_metrics
                   WHERE video_id IN (SELECT video_id FROM processed_videos WHERE channel_id IN ({channel_id_list}))
                  GROUP BY video_id
     )
"""

df_brands = pd.read_sql_query(query, conn)

# Parse brands
brands_list = []
for idx, row in df_brands.iterrows():
    if row['brands_mentioned']:
        brands = json.loads(row['brands_mentioned'])
        for brand_obj in brands:
            brands_list.append({
                'brand': brand_obj['brand'],
                'engagement_rate': row['engagement_rate']
            })

if brands_list:
    brands_df = pd.DataFrame(brands_list)
    brand_engagement = brands_df.groupby('brand')['engagement_rate'].median().sort_values(ascending=False).head(10)

    fig = px.bar(
        x = brand_engagement.values,
        y = brand_engagement.index,
        orientation='h',
        color_discrete_sequence=[COLORS['accent']],
        labels = {'x': 'Median Engagement Rate', 'y': 'Brand'}
    )
    fig.update_layout(height = 400, showlegend = False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info('No brand data available')


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

