import json, os, signal, sys, time
from mastodon import Mastodon, StreamListener
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration - with defaults for local development
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MASTODON_API_URL = os.getenv('MASTODON_API_URL', 'https://mastodon.social')
TOPIC = os.getenv('TOPIC_RAW', 'mastodon-raw')

# Mastodon credentials - using your existing ones as defaults
CLIENT_ID = os.getenv('MASTODON_CLIENT_ID', 'qpHVy7dW7A_2lkNsseHiUFetEqHIfEuKIc8e6zirvNM')
CLIENT_SECRET = os.getenv('MASTODON_CLIENT_SECRET', 'Yupj-XTy-Dy99c7RFn8DCPHS_vgHYysRKVUjM1M0Frk')
ACCESS_TOKEN = os.getenv('MASTODON_ACCESS_TOKEN', 'i2rC1YQsJbOpnAL-o-Tzr_k6Bfgj8J3SGlgKudX2iQY')

# Initialize Mastodon client
try:
    mastodon = Mastodon(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        access_token=ACCESS_TOKEN,
        api_base_url=MASTODON_API_URL
    )
    logger.info(f"✓ Connected to Mastodon API: {MASTODON_API_URL}")
except Exception as e:
    logger.error(f"Failed to connect to Mastodon API: {e}")
    sys.exit(1)

# Initialize Kafka producer with retry logic
retry_count = 0
max_retries = 10
producer = None

while retry_count < max_retries and producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP}")
    except Exception as e:
        retry_count += 1
        logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
        time.sleep(5)

if producer is None:
    logger.error("Could not connect to Kafka after maximum retries")
    sys.exit(1)

class Listener(StreamListener):
    def __init__(self):
        self.message_count = 0
        
    def on_update(self, status):
        try:
            # Extract hashtags
            hashtags = [tag["name"] for tag in status.get("tags", [])]

            if not hashtags:
                return  # skip posts with no hashtags

            data = {
                "user_name": status["account"]["username"],  # Using user_name (not user)
                "content": status["content"],
                "hashtags": hashtags,
                "timestamp": status["created_at"].isoformat()
            }

            producer.send(TOPIC, data)
            self.message_count += 1
            
            if self.message_count % 10 == 0:
                logger.info(f"📊 Sent {self.message_count} messages to {TOPIC}")
            else:
                logger.debug(f"🚀 Sent: {data['user_name']} - {len(hashtags)} hashtags")
                
        except Exception as e:
            logger.error(f"Error processing status: {e}")
    
    def on_abort(self, err):
        logger.error(f"Stream aborted: {err}")
    
    def handle_heartbeat(self):
        logger.debug("💓 Heartbeat")

# Graceful shutdown
def signal_handler(sig, frame):
    logger.info("🛑 Shutting down gracefully...")
    if producer:
        producer.flush()
        producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Stream
logger.info(f"🔴 Listening to {MASTODON_API_URL} public timeline (hashtags only)...")
logger.info(f"📤 Publishing to Kafka topic: {TOPIC}")

listener = Listener()
try:
    mastodon.stream_public(listener)
except KeyboardInterrupt:
    logger.info("Interrupted by user")
    signal_handler(signal.SIGINT, None)
except Exception as e:
    logger.error(f"Streaming error: {e}")
    sys.exit(1)