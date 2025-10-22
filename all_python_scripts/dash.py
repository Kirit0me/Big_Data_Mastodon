import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import threading
import time
from collections import Counter

st.set_page_config(page_title="Mastodon Dashboard", layout="wide")
st.title("📊 Real-time Mastodon Hashtag Dashboard")

# Global storage for posts
data = []

# -------------------------
# Kafka consumer in background thread
# -------------------------
def consume_kafka():
    consumer = KafkaConsumer(
        "mastodon-processed",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    for message in consumer:
        msg = message.value
        # Ensure expected fields
        if "user" in msg and "content" in msg and "hashtag" in msg and "timestamp" in msg:
            data.append(msg)

threading.Thread(target=consume_kafka, daemon=True).start()

# -------------------------
# Streamlit placeholders
# -------------------------
latest_posts_placeholder = st.empty()
top_hashtags_placeholder = st.empty()

# -------------------------
# Refresh loop
# -------------------------
REFRESH_INTERVAL = 2  # seconds

while True:
    if data:
        df = pd.DataFrame(data)
        # Latest posts table
        latest_posts_placeholder.subheader("📝 Latest posts")
        latest_posts_placeholder.dataframe(df.tail(20), use_container_width=True)

        # Top hashtags
        top_hashtags_placeholder.subheader("🏷️ Top Hashtags")
        hashtags = df['hashtag'].tolist()
        top_counts = Counter(hashtags).most_common(10)
        if top_counts:
            top_df = pd.DataFrame(top_counts, columns=["Hashtag", "Count"])
            top_hashtags_placeholder.table(top_df)
    else:
        latest_posts_placeholder.write("Waiting for posts...")
        top_hashtags_placeholder.write("No hashtags yet...")

    time.sleep(REFRESH_INTERVAL)
