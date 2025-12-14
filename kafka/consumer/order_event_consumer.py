"""
Order Event Consumer
Processes real-time order events from Kafka
Calculates business metrics, tracks inventory, monitors revenue
"""
import json
import time
import argparse
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import KafkaError


class OrderEventConsumer:
    """Consumes and processes order events from Kafka"""

    def __init__(self, bootstrap_servers='localhost:29092', topic='order-events', group_id='order-processor'):
        """Initialize Kafka consumer and metrics tracking"""
        self.topic = topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            max_poll_records=100  # Process in batches for efficiency
        )

        print(f"{'='*60}")
        print("ORDER EVENT CONSUMER INITIALIZED")
        print(f"{'='*60}")
        print(f"  Kafka Brokers: {bootstrap_servers}")
        print(f"  Topic: {self.topic}")
        print(f"  Consumer Group: {self.group_id}")
        print(f"  Auto Commit: Enabled")
        print("-" * 60)

        # Initialize metrics tracking
        self.stats = {
            'total_events': 0,
            'events_by_type': defaultdict(int),
            'orders_by_country': defaultdict(int),
            'revenue_by_country': defaultdict(float),
            'total_revenue': 0,
            'total_orders': 0,
            'total_items_sold': 0,
            'total_units_shipped': 0,
            'cancelled_orders': 0,
            'delivered_orders': 0,
            'high_value_orders': 0,  # Orders > $1000
            'average_order_value': 0,
            'product_demand': defaultdict(int),  # Track popular products
            'top_customers': defaultdict(int),  # Track customer order frequency
            'errors': 0,
            'start_time': time.time()
        }

    def process_event(self, event):
        """
        Process a single order event
        Implements business logic based on event type
        """
        try:
            event_type = event.get('event_type', 'unknown')
            self.stats['total_events'] += 1
            self.stats['events_by_type'][event_type] += 1

            # Route to appropriate handler
            if event_type == 'new_order':
                self._process_new_order(event)
            elif event_type == 'payment_confirmed':
                self._process_payment_confirmed(event)
            elif event_type == 'order_shipped':
                self._process_order_shipped(event)
            elif event_type == 'order_delivered':
                self._process_order_delivered(event)
            elif event_type == 'order_cancelled':
                self._process_order_cancelled(event)
            else:
                self._process_unknown(event)

            return True

        except Exception as e:
            print(f"✗ Error processing event: {e}")
            print(f"  Event: {json.dumps(event, indent=2)}")
            self.stats['errors'] += 1
            return False

    def _process_new_order(self, event):
        """
        Process new order placement
        - Track revenue and order metrics
        - Update product demand
        - Identify high-value customers
        """
        order_id = event.get('order_id')
        order_total = event.get('order_total', 0)
        grand_total = event.get('grand_total', 0)
        customer_id = event.get('customer_id')
        customer_name = event.get('customer_name')
        country = event.get('customer_country', 'Unknown')
        items = event.get('order_items', [])

        # Update revenue metrics
        self.stats['total_orders'] += 1
        self.stats['total_revenue'] += order_total
        self.stats['orders_by_country'][country] += 1
        self.stats['revenue_by_country'][country] += order_total

        # Track item counts
        item_count = event.get('item_count', 0)
        units_ordered = event.get('units_ordered', 0)
        self.stats['total_items_sold'] += item_count
        self.stats['total_units_shipped'] += units_ordered

        # Track high-value orders
        if order_total > 1000:
            self.stats['high_value_orders'] += 1
            print(f"💎 High-Value Order Detected:")
            print(f"   Order ID: #{order_id}")
            print(f"   Customer: {customer_name} ({country})")
            print(f"   Total: ${order_total:,.2f}")
            print(f"   Items: {item_count} ({units_ordered} units)")

        # Track product demand
        for item in items:
            product_id = item.get('product_id')
            product_name = item.get('product_name')
            quantity = item.get('quantity', 0)
            self.stats['product_demand'][f"{product_id}:{product_name}"] += quantity

        # Track customer activity
        self.stats['top_customers'][f"{customer_id}:{customer_name}"] += 1

        # Calculate running average order value
        if self.stats['total_orders'] > 0:
            self.stats['average_order_value'] = self.stats['total_revenue'] / self.stats['total_orders']

    def _process_payment_confirmed(self, event):
        """
        Process payment confirmation
        - In production: Trigger fulfillment workflow
        - Update payment analytics
        - Alert on payment failures
        """
        order_id = event.get('order_id')
        payment_method = event.get('payment_method')
        payment_status = event.get('payment_status')

        # In production, you would:
        # 1. Update order status in database
        # 2. Trigger warehouse fulfillment system
        # 3. Send confirmation email to customer
        # 4. Update payment gateway records
        # 5. Track payment method preferences

        if payment_status != 'confirmed':
            print(f"⚠  Payment issue for order #{order_id}: {payment_status}")

    def _process_order_shipped(self, event):
        """
        Process order shipment
        - Track shipping metrics
        - Update delivery estimates
        - Monitor shipper performance
        """
        order_id = event.get('order_id')
        tracking_number = event.get('tracking_number')
        shipper = event.get('shipper')

        # In production, you would:
        # 1. Send tracking number to customer
        # 2. Update order status in database
        # 3. Calculate expected delivery date
        # 4. Monitor shipper SLAs
        # 5. Update inventory systems

        print(f"📦 Order Shipped: #{order_id} via {shipper} (Tracking: {tracking_number})")

    def _process_order_delivered(self, event):
        """
        Process order delivery confirmation
        - Track fulfillment metrics
        - Calculate delivery times
        - Trigger post-delivery workflows
        """
        order_id = event.get('order_id')
        delivered_date = event.get('delivered_date')

        self.stats['delivered_orders'] += 1

        # In production, you would:
        # 1. Request customer feedback/review
        # 2. Close order in fulfillment system
        # 3. Calculate actual vs. expected delivery time
        # 4. Update shipper performance metrics
        # 5. Trigger customer loyalty programs

        print(f"✓ Order Delivered: #{order_id}")

    def _process_order_cancelled(self, event):
        """
        Process order cancellation
        - Track cancellation reasons
        - Trigger refund workflows
        - Update inventory
        """
        order_id = event.get('order_id')
        reason = event.get('cancellation_reason')
        refund_status = event.get('refund_status')

        self.stats['cancelled_orders'] += 1

        # In production, you would:
        # 1. Process refund if payment was captured
        # 2. Return inventory to available stock
        # 3. Notify customer of cancellation
        # 4. Analyze cancellation patterns
        # 5. Alert teams for specific reasons (e.g., out of stock)

        print(f"❌ Order Cancelled: #{order_id} - Reason: {reason}")

        # Alert on inventory issues
        if reason == 'Out of stock':
            print(f"   ⚠  INVENTORY ALERT: Stock shortage detected")

    def _process_unknown(self, event):
        """Process unknown event types"""
        print(f"⚠  Unknown event type: {event.get('event_type')}")

    def consume_continuous(self, duration_seconds=300):
        """
        Continuously consume and process order events
        Prints metrics periodically and at completion
        """
        print(f"\n{'='*60}")
        print("ORDER EVENT CONSUMPTION STARTED")
        print(f"{'='*60}")
        print(f"  Duration: {duration_seconds} seconds")
        print(f"  Processing events in real-time...")
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

                # Print stats every 30 seconds
                if (current_time - last_stats_print) >= 30:
                    self._print_current_stats()
                    last_stats_print = current_time

        except KeyboardInterrupt:
            print("\n⚠ Interrupted by user")

        finally:
            self._print_final_stats()
            self.consumer.close()

    def _print_current_stats(self):
        """Print current processing statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['total_events'] / elapsed if elapsed > 0 else 0

        print(f"\n{'─'*60}")
        print("📊 Current Metrics:")
        print(f"   Events processed: {self.stats['total_events']}")
        print(f"   Processing rate: {rate:.1f} events/sec")
        print(f"   Total revenue: ${self.stats['total_revenue']:,.2f}")
        print(f"   Total orders: {self.stats['total_orders']}")
        print(f"   Average order: ${self.stats['average_order_value']:,.2f}")
        print(f"   Errors: {self.stats['errors']}")
        print(f"{'─'*60}")

    def _print_final_stats(self):
        """Print comprehensive final statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['total_events'] / elapsed if elapsed > 0 else 0

        print(f"\n{'='*60}")
        print("ORDER EVENT PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"  Duration: {elapsed:.2f} seconds")
        print(f"  Total events processed: {self.stats['total_events']}")
        print(f"  Processing rate: {rate:.2f} events/second")
        print(f"  Errors: {self.stats['errors']}")

        print(f"\n  Event Type Distribution:")
        for event_type, count in sorted(self.stats['events_by_type'].items()):
            pct = (count / self.stats['total_events'] * 100) if self.stats['total_events'] > 0 else 0
            print(f"    {event_type:20s}: {count:4d} ({pct:5.1f}%)")

        print(f"\n  Order Metrics:")
        print(f"    Total orders placed: {self.stats['total_orders']}")
        print(f"    Orders delivered: {self.stats['delivered_orders']}")
        print(f"    Orders cancelled: {self.stats['cancelled_orders']}")
        print(f"    High-value orders (>$1000): {self.stats['high_value_orders']}")

        print(f"\n  Revenue Metrics:")
        print(f"    Total revenue: ${self.stats['total_revenue']:,.2f}")
        print(f"    Average order value: ${self.stats['average_order_value']:,.2f}")
        print(f"    Total items sold: {self.stats['total_items_sold']}")
        print(f"    Total units shipped: {self.stats['total_units_shipped']}")

        print(f"\n  Top 5 Countries by Revenue:")
        top_countries = sorted(self.stats['revenue_by_country'].items(),
                              key=lambda x: x[1], reverse=True)[:5]
        for country, revenue in top_countries:
            orders = self.stats['orders_by_country'][country]
            avg = revenue / orders if orders > 0 else 0
            print(f"    {country:20s}: ${revenue:10,.2f} ({orders:3d} orders, avg: ${avg:,.2f})")

        print(f"\n  Top 5 Products by Demand:")
        top_products = sorted(self.stats['product_demand'].items(),
                             key=lambda x: x[1], reverse=True)[:5]
        for product, units in top_products:
            product_name = product.split(':')[1] if ':' in product else product
            print(f"    {product_name:30s}: {units:4d} units")

        print(f"\n  Top 5 Customers by Order Frequency:")
        top_customers = sorted(self.stats['top_customers'].items(),
                              key=lambda x: x[1], reverse=True)[:5]
        for customer, order_count in top_customers:
            customer_name = customer.split(':')[1] if ':' in customer else customer
            print(f"    {customer_name:30s}: {order_count:3d} orders")

        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='Northwind Order Event Consumer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:29092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='order-events',
                        help='Kafka topic name')
    parser.add_argument('--group-id', type=str, default='order-processor',
                        help='Consumer group ID')
    parser.add_argument('--duration', type=int, default=300,
                        help='Duration in seconds')

    args = parser.parse_args()

    # Create consumer
    consumer = OrderEventConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id
    )

    # Start consuming
    consumer.consume_continuous(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
