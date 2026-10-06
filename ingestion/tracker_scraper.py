import json
import time
import random
from kafka import KafkaProducer

KAFKA_TOPIC = "torrent-swarm-events"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka Broker successfully.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def simulate_tracker_scrape():
    """Simulates live BitTorrent tracker scrape metrics for active infohashes."""
    sample_infohashes = [
        ("a1b2c3d4e5f6789012300", "movies"),
        ("a1b2c3d4e5f6789012301", "tv"),
        ("a1b2c3d4e5f6789012302", "games"),
        ("a1b2c3d4e5f6789012303", "music")
    ]
    
    item = random.choice(sample_infohashes)
    return {
        "infohash": item[0],
        "category": item[1],
        "seeders": random.randint(50, 5000),
        "leechers": random.randint(10, 1500),
        "tracker_url": "udp://tracker.opentrackr.org:1337/announce",
        "timestamp": int(time.time())
    }

def main():
    producer = create_kafka_producer()
    if not producer:
        return

    print(f"Starting BitTorrent tracker scraper... Streaming to topic '{KAFKA_TOPIC}'")
    
    try:
        events_sent = 0
        while events_sent < 20:  # Produces 20 sample events
            event = simulate_tracker_scrape()
            producer.send(KAFKA_TOPIC, value=event)
            print(f"[Published to Kafka] Infohash: {event['infohash']} | Seeders: {event['seeders']} | Leechers: {event['leechers']}")
            events_sent += 1
            time.sleep(1)
            
        producer.flush()
        print(f"Done! Successfully published {events_sent} live swarm events to Kafka.")
    except KeyboardInterrupt:
        print("Scraper stopped manually.")

if __name__ == "__main__":
    main()