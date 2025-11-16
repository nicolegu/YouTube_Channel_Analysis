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
    'accent': '#52366F',
    'success': '#035453',
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

query = f"""
    SELECT DATE(vem.timestamp) AS date,
           tc.channel_name,
           AVG(vem.engagement_rate) AS avg_engagement
      FROM video_engagement_metrics vem
      JOIN processed_videos pv
        ON vem.video_id = pv.video_id
      JOIN tracking_config tc
        ON pv.channel_id = tc.channel_id
     WHERE pv.channel_id IN ({channel_id_list})
     GROUP BY DATE(vem.timestamp), tc.channel_name
     ORDER BY DATE(vem.timestamp)
"""
df_trend = pd.read_sql_query(query, conn)

if not df_trend.empty:
    fig = px.line(
        df_trend,
        x = 'date',
        y = 'avg_engagement',
        color = 'channel_name',
        markers = True,
        labels = {'date': 'Date',
                  'avg_engagement': 'Avg Engagement Rate',
                  'channel': 'Channel'}
    )
    # Modify properties of figure's layout (titles, legends, etc.)
    fig.update_layout(
        hovermode = 'x unified',
        height = 400,
        legend = dict(
            orientation = 'h',
            yanchor = 'bottom',
            y = 1.02,
            xanchor = 'right',
            x = 1
        )
    )
    st.plotly_chart(fig, use_container_width = True)
else:
    st.info('No engagement data available for selected channels')

# ============ ROW 2: Top Performing Video Table  ============

st.subheader('Top Performing Videos')

query = f"""
    WITH latest_metrics AS (
      SELECT video_id, MAX(id) AS max_id
        FROM video_metrics
       GROUP BY video_id
    )
    SELECT vm.title, tc.channel_name, vem.engagement_rate, vm.view_count
      FROM video_metrics vm
      JOIN latest_metrics lm
        ON vm.video_id = lm.video_id
          AND vm.id = lm.max_id
      JOIN processed_videos pv
        ON vm.video_id = pv.video_id
      JOIN video_engagement_metrics vem
        ON pv.video_id = vem.video_id
      JOIN tracking_config tc
        ON pv.channel_id = tc.channel_id
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
            'channel_name': 'Channel',
            'engagement_rate': 'Engagement %',
            'view_count': 'Views'
        }),
        use_container_width = True,
        height = 400,
        hide_index=True,
        column_config={
            'Channel': st.column_config.TextColumn(width='small'),
            'Video Title': st.column_config.TextColumn(width='large')
        }
    )

else:
    st.info('No video data available')

# ============ ROW 3: Engagement by Brand Bar Chart ============

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
        try:
            brands = json.loads(row['brands_mentioned'])
            for brand_obj in brands:
                brands_list.append({
                    'brand': brand_obj['brand'],
                    'engagement_rate': row['engagement_rate']
                })
        except json.JSONDecodeError:
            continue

if brands_list:
    brands_df = pd.DataFrame(brands_list)
    brand_engagement = brands_df.groupby('brand')['engagement_rate'].median().sort_values(ascending=False).head(10)
    brand_counts = brands_df.groupby('brand').size()
    brands_with_enough_data = brand_engagement[brand_counts >= 3].index
    brand_engagement_filtered = brand_engagement[brand_engagement.index.isin(brands_with_enough_data)]

    fig = px.bar(
        x = brand_engagement.index,
        y = brand_engagement.values,
        color_discrete_sequence=[COLORS['accent']],
        labels = {'x': 'Median Engagement Rate', 'y': 'Brand'}
    )
    fig.update_layout(height = 400, showlegend = False,
                      xaxis_tickangle = -45,
                      margin = dict(l=0, r=0, t=10, b=80)) # l: left, t: top, b: bottom
    st.plotly_chart(fig, use_container_width=True)
    st.info(f'Showing {len(brand_engagement_filtered)} brands with 3+ mentions')
else:
    st.info('No brand data available')

# ============ ROW 4: Product Categories ============

st.subheader('Product Categories Coverage')
query = f"""
    SELECT video_id, product_categories
      FROM processed_videos
     WHERE channel_id IN ({channel_id_list})
"""

df_products = pd.read_sql_query(query, conn)

# Parse products
product_category_list = {}
for idx, row in df_products.iterrows():
    if row['product_categories']:
        try:
            product_categories = json.loads(row['product_categories'])
            for cat in product_categories:
                product_category_list[cat] = product_category_list.get(cat, 0) + 1
        except json.JSONDecodeError:
            continue

if product_category_list:
    category_series = pd.Series(product_category_list).sort_values(ascending = True)

    # Control how colors are used repetitively explicitly
    # colors_gradient = [COLORS['success'], COLORS['info'], COLORS['accent']] * (len(category_series) // 3 + 1)
    # colors_gradient = colors_gradient[:len(category_series)]

    fig = px.bar(
        y = category_series.index,
        x = category_series.values,
        orientation = 'h',
        color_discrete_sequence = [COLORS['success']],
        labels = {'x': 'Number of Videos', 'y': 'Product Category'}
    )
    fig.update_layout(height = 350, showlegend = False)
    st.plotly_chart(fig, use_container_width = True)

else:
    st.info('No product category data available')


# ============ ROW 5: Overall Sentiment Distribution (Pie Chart) ============

st.subheader('Comment Sentiment Distribution')

query = f"""
    SELECT sentiment, COUNT(*) as count
      FROM comments c
      JOIN processed_videos pv
        ON c.video_id = pv.video_id
     WHERE pv.channel_id IN ({channel_id_list})
          AND sentiment IS NOT NULL
     GROUP BY sentiment
"""

df_sentiment = pd.read_sql_query(query, conn)

fig = px.pie(df_sentiment, values = 'count', names = 'sentiment',
             color = 'sentiment',
             color_discrete_map={'positive': '#0A372F',
                                 'negative': '#F06F0E',
                                 'neutral': '#FFB342'}
            )
st.plotly_chart(fig)

# ============ ROW 6: Purchase Intent (Table) ============

st.subheader('Purchase Intent Signals')
query = f"""
    SELECT DISTINCT c.comment_id, tc.channel_name, vm.title, c.comment_text, c.author_name
      FROM comments c
      JOIN (
          SELECT video_id, title
          FROM video_metrics
          GROUP BY video_id
      ) vm
        ON c.video_id = vm.video_id
      JOIN processed_videos pv
        ON c.video_id = pv.video_id
      JOIN tracking_config tc
        ON pv.channel_id = tc.channel_id
     WHERE pv.channel_id IN ({channel_id_list})
          AND c.purchase_intent = 1
     ORDER BY c.published_at DESC
     LIMIT 20
"""

purchase_comments = pd.read_sql_query(query, conn)

purchase_comments['comment_preview'] = purchase_comments['comment_text'].str[:100] + '...'

st.dataframe(
    purchase_comments[['channel_name', 'title', 'comment_preview', 'author_name']].rename(columns = {
        'channel_name': 'Channel',
        'title': 'Video',
        'comment_preview': 'Comment',
        'author_name': 'Author'
        }
    ),
    use_container_width=True,
    height=400,
    hide_index=True,
    column_config={
        'Channel': st.column_config.TextColumn(width = 'small'),
        'Comment': st.column_config.TextColumn(
            width='large',
            help='Customer comments expressing purchase interest'
            ),
        'Video': st.column_config.TextColumn(width = 'medium'),
        'Author': st.column_config.TextColumn(width = 'small')
        }
)

st.subheader('Full Comments')
for idx, row in purchase_comments.iterrows():
    with st.expander(f"💬{row['author_name']} on {row['title']}..."):
        st.write(row['comment_text'])


# ============ ROW 7: Recent Questions Asked (Table) ============

st.subheader('Recent Customer Questions')
st.caption('Latest questions from viewers - reveals customer interests and potential content ideas')

query = f"""
    SELECT DISTINCT c.comment_id, c.comment_text, vm.title, tc.channel_name, c.published_at
      FROM comments c
      JOIN (
          SELECT video_id, title
          FROM video_metrics
          GROUP BY video_id
      ) vm
        ON c.video_id = vm.video_id
      JOIN processed_videos pv
        ON c.video_id = pv.video_id
      JOIN tracking_config tc
        ON pv.channel_id = tc.channel_id
     WHERE pv.channel_id IN ({channel_id_list})
          AND c.is_question = 1
     ORDER BY c.published_at DESC
     LIMIT 20
"""

questions = pd.read_sql_query(query, conn)

questions['comment_preview'] = questions['comment_text'].str[:100] + '...'

if not questions.empty:
    st.dataframe(
        questions[['comment_preview', 'title', 'channel_name', 'published_at']].rename(columns={
            'comment_preview': 'Question',
            'title': 'Video',
            'channel_name': 'Channel',
            'published_at': 'Date'
        }),
        use_container_width=True,
        height=400,
        hide_index=True,
        column_config={
            'Question': st.column_config.TextColumn(width='large'),
            'Video': st.column_config.TextColumn(width='medium'),
            'Channel': st.column_config.TextColumn(width='small'),
            'Date': st.column_config.DatetimeColumn(width='small')
        }
    )

    st.subheader('Full Questions')
    for idx, row in questions.iterrows():
        with st.expander(f"🤔{row['title']} on {row['published_at']}..."):
            st.write(row['comment_text'])
else:
    st.info('No questions found')


# ============ ROW 8: Brand sentiment from comments ============
st.subheader('Brand Sentiment from Comments')

query = f"""
    SELECT c.brands_mentioned, c.sentiment, c.comment_text
      FROM comments c
      JOIN processed_videos pv
        ON c.video_id = pv.video_id
     WHERE pv.channel_id IN ({channel_id_list})
          AND c.brands_mentioned IS NOT NULL
          AND c.brands_mentioned != '[]'
"""

temp = pd.read_sql_query(query, conn)

# Parse JSON and aggregate
brand_sentiment = []
for idx, row in temp.iterrows():
    brands = json.loads(row['brands_mentioned'])
    for brand_obj in brands:
        brand_sentiment.append({
            'brand': brand_obj['brand'],
            'sentiment': row['sentiment']
        })

brand_sentiment_df = pd.DataFrame(brand_sentiment)

brand_sentiment_pivot = brand_sentiment_df.groupby(['brand', 'sentiment']).size().unstack(fill_value=0)

# Filter brands with at least 5 mentions
brand_totals = brand_sentiment_pivot.sum(axis = 1)
brand_sentiment_pivot = brand_sentiment_pivot[brand_totals >= 3]
brand_sentiment_pivot = brand_sentiment_pivot.sort_values(by = 'positive', ascending = True).tail(10)

fig = px.bar(
    brand_sentiment_pivot,
    x = ['positive', 'neutral', 'negative'],
    y = brand_sentiment_pivot.index,
    orientation = 'h',
    labels = {'value': 'Number of Comments', 'variable': 'Sentiment', 'y': 'Brand'},
    color_discrete_map = {
        'positive': COLORS['success'],
        'neutral': '#95a5a6',
        'negative': COLORS['accent']
    },
    title = 'Brand Sentiment from Comments (5+ mentions)'
)

fig.update_layout(
    height = 500,
    xaxis_title = 'Number of Comments',
    yaxis_title = 'Brand',
    legend_title = 'Sentiment',
    barmode = 'stack'
)

st.plotly_chart(fig, width = 'stretch')       

conn.close()


