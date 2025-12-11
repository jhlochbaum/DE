"""
Kafka Event Producer
Generates and streams user events to Kafka topics
"""
import json
import time
import random
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


class EventProducer:
    """Produces simulated user events to Kafka"""

    def __init__(self, bootstrap_servers='localhost:29092', topic='user-events'):
        """Initialize Kafka producer"""
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1
        )

        print(f"✓ Connected to Kafka at {bootstrap_servers}")
        print(f"✓ Producing to topic: {self.topic}")

    def generate_user_event(self):
        """Generate a random user event"""
        event_types = ['page_view', 'click', 'purchase', 'signup', 'login', 'logout', 'search']
        users = list(range(101, 111))  # 10 sample users
        pages = ['/home', '/products', '/cart', '/checkout', '/profile', '/search']

        event = {
            'event_id': str(random.randint(10000, 99999)),
            'user_id': random.choice(users),
            'event_type': random.choice(event_types),
            'timestamp': datetime.now().isoformat(),
            'page': random.choice(pages),
            'session_id': f"session_{random.randint(1000, 9999)}",
            'metadata': {
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                'device': random.choice(['desktop', 'mobile', 'tablet']),
                'ip_address': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
            }
        }

        # Add event-specific data
        if event['event_type'] == 'purchase':
            event['purchase_data'] = {
                'product_id': random.randint(1, 10),
                'amount': round(random.uniform(10.0, 1000.0), 2),
                'currency': 'USD'
            }
        elif event['event_type'] == 'search':
            event['search_query'] = random.choice([
                'laptop', 'mouse', 'keyboard', 'monitor', 'headphones'
            ])

        return event

    def send_event(self, event, key=None):
        """Send event to Kafka topic"""
        try:
            # Use user_id as partition key for even distribution
            if key is None:
                key = str(event['user_id'])

            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )

            # Block for 'synchronous' send to handle errors
            record_metadata = future.get(timeout=10)

            return {
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }

        except KafkaError as e:
            print(f"✗ Error sending event: {e}")
            return None

    def produce_continuous(self, duration_seconds=300, rate_per_second=5):
        """Continuously produce events at specified rate"""
        print(f"\n📊 Starting event production...")
        print(f"   Duration: {duration_seconds} seconds")
        print(f"   Rate: {rate_per_second} events/second")
        print("-" * 60)

        start_time = time.time()
        event_count = 0
        error_count = 0

        try:
            while (time.time() - start_time) < duration_seconds:
                # Generate and send events
                for _ in range(rate_per_second):
                    event = self.generate_user_event()
                    result = self.send_event(event)

                    if result:
                        event_count += 1
                        if event_count % 50 == 0:  # Log every 50 events
                            print(f"✓ Sent {event_count} events | "
                                  f"Latest: {event['event_type']} from user {event['user_id']}")
                    else:
                        error_count += 1

                # Wait for next second
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n⚠ Interrupted by user")

        finally:
            # Cleanup
            self.producer.flush()
            self.producer.close()

            elapsed_time = time.time() - start_time
            print("\n" + "=" * 60)
            print("📈 Production Summary:")
            print(f"   Total events sent: {event_count}")
            print(f"   Errors: {error_count}")
            print(f"   Duration: {elapsed_time:.2f} seconds")
            print(f"   Average rate: {event_count / elapsed_time:.2f} events/second")
            print("=" * 60)

    def produce_batch(self, num_events=100):
        """Produce a batch of events"""
        print(f"\n📊 Producing batch of {num_events} events...")

        event_count = 0
        error_count = 0

        try:
            for i in range(num_events):
                event = self.generate_user_event()
                result = self.send_event(event)

                if result:
                    event_count += 1
                    if (i + 1) % 20 == 0:
                        print(f"✓ Progress: {i + 1}/{num_events} events")
                else:
                    error_count += 1

        finally:
            self.producer.flush()
            self.producer.close()

            print("\n" + "=" * 60)
            print("📈 Batch Production Summary:")
            print(f"   Events sent: {event_count}/{num_events}")
            print(f"   Errors: {error_count}")
            print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Kafka Event Producer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:29092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='user-events',
                        help='Kafka topic name')
    parser.add_argument('--mode', type=str, choices=['continuous', 'batch'], default='continuous',
                        help='Production mode')
    parser.add_argument('--duration', type=int, default=300,
                        help='Duration in seconds for continuous mode')
    parser.add_argument('--rate', type=int, default=5,
                        help='Events per second for continuous mode')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of events for batch mode')

    args = parser.parse_args()

    # Create producer
    producer = EventProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )

    # Run in selected mode
    if args.mode == 'continuous':
        producer.produce_continuous(
            duration_seconds=args.duration,
            rate_per_second=args.rate
        )
    else:
        producer.produce_batch(num_events=args.batch_size)


if __name__ == "__main__":
    main()
