"""
Kafka Event Consumer
Consumes and processes user events from Kafka topics
"""
import json
import time
import argparse
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import KafkaError


class EventConsumer:
    """Consumes and processes user events from Kafka"""

    def __init__(self, bootstrap_servers='localhost:29092', topic='user-events', group_id='event-processor'):
        """Initialize Kafka consumer"""
        self.topic = topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )

        print(f"✓ Connected to Kafka at {bootstrap_servers}")
        print(f"✓ Subscribed to topic: {self.topic}")
        print(f"✓ Consumer group: {self.group_id}")

        # Statistics tracking
        self.stats = {
            'total_messages': 0,
            'messages_by_type': defaultdict(int),
            'messages_by_user': defaultdict(int),
            'errors': 0,
            'start_time': time.time()
        }

    def process_event(self, event):
        """Process a single event - implement your business logic here"""
        try:
            event_type = event.get('event_type', 'unknown')
            user_id = event.get('user_id')

            # Update statistics
            self.stats['total_messages'] += 1
            self.stats['messages_by_type'][event_type] += 1
            self.stats['messages_by_user'][user_id] += 1

            # Example processing based on event type
            if event_type == 'purchase':
                self._process_purchase(event)
            elif event_type == 'signup':
                self._process_signup(event)
            elif event_type == 'search':
                self._process_search(event)
            else:
                self._process_generic(event)

            return True

        except Exception as e:
            print(f"✗ Error processing event: {e}")
            self.stats['errors'] += 1
            return False

    def _process_purchase(self, event):
        """Process purchase events"""
        purchase_data = event.get('purchase_data', {})
        amount = purchase_data.get('amount', 0)

        # In production, you might:
        # - Update user lifetime value
        # - Trigger fulfillment workflows
        # - Send to analytics platform
        # - Update inventory systems

        if amount > 500:
            print(f"💰 High-value purchase: User {event['user_id']} spent ${amount}")

    def _process_signup(self, event):
        """Process signup events"""
        # In production, you might:
        # - Send welcome email
        # - Create user profile
        # - Initialize recommendations
        # - Track acquisition metrics

        print(f"👤 New signup: User {event['user_id']}")

    def _process_search(self, event):
        """Process search events"""
        search_query = event.get('search_query')

        # In production, you might:
        # - Update search analytics
        # - Train recommendation models
        # - Optimize search results
        # - Track popular queries

        pass  # Silent processing for search events

    def _process_generic(self, event):
        """Process generic events"""
        # Default processing for other event types
        pass

    def consume_continuous(self, duration_seconds=300):
        """Continuously consume events"""
        print(f"\n📨 Starting event consumption...")
        print(f"   Duration: {duration_seconds} seconds")
        print("-" * 60)

        start_time = time.time()
        last_stats_print = start_time

        try:
            for message in self.consumer:
                # Check if duration exceeded
                current_time = time.time()
                if (current_time - start_time) > duration_seconds:
                    break

                # Process the event
                event = message.value
                self.process_event(event)

                # Print stats every 10 seconds
                if (current_time - last_stats_print) >= 10:
                    self._print_stats()
                    last_stats_print = current_time

        except KeyboardInterrupt:
            print("\n⚠ Interrupted by user")

        finally:
            self._print_final_stats()
            self.consumer.close()

    def consume_batch(self, num_messages=100, timeout_ms=10000):
        """Consume a specific number of messages"""
        print(f"\n📨 Consuming batch of up to {num_messages} messages...")
        print(f"   Timeout: {timeout_ms}ms")
        print("-" * 60)

        messages_consumed = 0
        start_time = time.time()

        try:
            while messages_consumed < num_messages:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=timeout_ms)

                if not message_batch:
                    print("⚠ No more messages available")
                    break

                # Process each message in the batch
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        event = message.value
                        self.process_event(event)
                        messages_consumed += 1

                        if messages_consumed % 20 == 0:
                            print(f"✓ Progress: {messages_consumed}/{num_messages} messages")

                        if messages_consumed >= num_messages:
                            break

        except KeyboardInterrupt:
            print("\n⚠ Interrupted by user")

        finally:
            self._print_final_stats()
            self.consumer.close()

    def _print_stats(self):
        """Print current statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['total_messages'] / elapsed if elapsed > 0 else 0

        print(f"\n📊 Current Stats:")
        print(f"   Total messages: {self.stats['total_messages']}")
        print(f"   Rate: {rate:.2f} messages/second")
        print(f"   Errors: {self.stats['errors']}")

    def _print_final_stats(self):
        """Print final consumption statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['total_messages'] / elapsed if elapsed > 0 else 0

        print("\n" + "=" * 60)
        print("📈 Consumption Summary:")
        print(f"   Total messages: {self.stats['total_messages']}")
        print(f"   Duration: {elapsed:.2f} seconds")
        print(f"   Average rate: {rate:.2f} messages/second")
        print(f"   Errors: {self.stats['errors']}")

        print("\n   Messages by type:")
        for event_type, count in sorted(self.stats['messages_by_type'].items()):
            percentage = (count / self.stats['total_messages'] * 100) if self.stats['total_messages'] > 0 else 0
            print(f"      {event_type:15s}: {count:4d} ({percentage:5.1f}%)")

        print("\n   Top 5 users:")
        top_users = sorted(self.stats['messages_by_user'].items(),
                          key=lambda x: x[1], reverse=True)[:5]
        for user_id, count in top_users:
            print(f"      User {user_id}: {count} events")

        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Kafka Event Consumer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:29092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='user-events',
                        help='Kafka topic name')
    parser.add_argument('--group-id', type=str, default='event-processor',
                        help='Consumer group ID')
    parser.add_argument('--mode', type=str, choices=['continuous', 'batch'], default='continuous',
                        help='Consumption mode')
    parser.add_argument('--duration', type=int, default=300,
                        help='Duration in seconds for continuous mode')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of messages for batch mode')

    args = parser.parse_args()

    # Create consumer
    consumer = EventConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id
    )

    # Run in selected mode
    if args.mode == 'continuous':
        consumer.consume_continuous(duration_seconds=args.duration)
    else:
        consumer.consume_batch(num_messages=args.batch_size)


if __name__ == "__main__":
    main()
