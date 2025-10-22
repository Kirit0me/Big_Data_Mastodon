import json, os, signal, sys, time
from mastodon import Mastodon, StreamListener
from kafka import KafkaProducer

mastodon = Mastodon(
    client_id='qpHVy7dW7A_2lkNsseHiUFetEqHIfEuKIc8e6zirvNM',
    client_secret='Yupj-XTy-Dy99c7RFn8DCPHS_vgHYysRKVUjM1M0Frk',
    access_token='i2rC1YQsJbOpnAL-o-Tzr_k6Bfgj8J3SGlgKudX2iQY',
    api_base_url='https://mastodon.social'
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "mastodon-raw"

class Listener(StreamListener):
    def on_update(self, status):
        # Extract hashtags
        hashtags = [tag["name"] for tag in status.get("tags", [])]

        if not hashtags:
            return  # skip posts with no hashtags

        data = {
            "user": status["account"]["username"],
            "content": status["content"],
            "hashtags": hashtags,
            "timestamp": status["created_at"].isoformat()
        }

        print(f"🚀 Sending: {data}")
        producer.send(TOPIC, data)

# Stream
print("🔴 Listening to Mastodon public timeline (hashtags only)...")
mastodon.stream_public(Listener())
