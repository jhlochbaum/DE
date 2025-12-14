"""
Order Event Producer
Simulates real-time order events from a Northwind import/export company
Generates realistic order placement, shipping, and fulfillment events
"""
import json
import time
import random
import argparse
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError


class OrderEventProducer:
    """Produces realistic order events using Northwind data"""

    def __init__(self, bootstrap_servers='localhost:29092', topic='order-events'):
        """Initialize Kafka producer and load Northwind reference data"""
        self.topic = topic

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip'  # Compress for efficiency
        )

        print(f"✓ Connected to Kafka at {bootstrap_servers}")
        print(f"✓ Producing to topic: {self.topic}")

        # Load Northwind reference data
        self.products = self._load_products()
        self.customers = self._load_customers()
        self.employees = self._load_employees()
        self.shippers = self._load_shippers()

        print(f"✓ Loaded {len(self.products)} products")
        print(f"✓ Loaded {len(self.customers)} customers")
        print(f"✓ Loaded {len(self.employees)} employees")
        print(f"✓ Loaded {len(self.shippers)} shippers")

        # Track order IDs (start from 10000 for simulation)
        self.order_id_counter = 10000

    def _get_db_connection(self):
        """Get connection to Northwind database"""
        try:
            return psycopg2.connect(
                host='localhost',
                port=5432,
                database='airflow',
                user='airflow',
                password='airflow'
            )
        except Exception as e:
            print(f"⚠ Database connection failed: {e}")
            print("⚠ Using fallback sample data")
            return None

    def _load_products(self):
        """Load available products from Northwind"""
        conn = self._get_db_connection()
        if not conn:
            return self._get_fallback_products()

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT product_id, product_name, unit_price, units_in_stock, category_id
                FROM products
                WHERE NOT discontinued
                ORDER BY product_id
            """)

            products = []
            for row in cursor.fetchall():
                products.append({
                    'product_id': row[0],
                    'product_name': row[1],
                    'unit_price': float(row[2]),
                    'units_in_stock': row[3],
                    'category_id': row[4]
                })

            cursor.close()
            conn.close()
            return products

        except Exception as e:
            print(f"⚠ Error loading products: {e}")
            return self._get_fallback_products()

    def _load_customers(self):
        """Load customers from Northwind"""
        conn = self._get_db_connection()
        if not conn:
            return self._get_fallback_customers()

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT customer_id, company_name, contact_name, country, city
                FROM customers
                ORDER BY customer_id
            """)

            customers = []
            for row in cursor.fetchall():
                customers.append({
                    'customer_id': row[0],
                    'company_name': row[1],
                    'contact_name': row[2],
                    'country': row[3],
                    'city': row[4]
                })

            cursor.close()
            conn.close()
            return customers

        except Exception as e:
            print(f"⚠ Error loading customers: {e}")
            return self._get_fallback_customers()

    def _load_employees(self):
        """Load employees from Northwind"""
        conn = self._get_db_connection()
        if not conn:
            return [{'employee_id': 1, 'name': 'Sales Rep'}]

        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT employee_id, first_name || ' ' || last_name AS name
                FROM employees
            """)

            employees = [{'employee_id': row[0], 'name': row[1]} for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return employees

        except Exception as e:
            print(f"⚠ Error loading employees: {e}")
            return [{'employee_id': 1, 'name': 'Sales Rep'}]

    def _load_shippers(self):
        """Load shipping companies from Northwind"""
        conn = self._get_db_connection()
        if not conn:
            return [{'shipper_id': 1, 'company_name': 'Express Shipping'}]

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT shipper_id, company_name FROM shippers")
            shippers = [{'shipper_id': row[0], 'company_name': row[1]} for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return shippers

        except Exception as e:
            print(f"⚠ Error loading shippers: {e}")
            return [{'shipper_id': 1, 'company_name': 'Express Shipping'}]

    def _get_fallback_products(self):
        """Fallback products if database unavailable"""
        return [
            {'product_id': 1, 'product_name': 'Chai', 'unit_price': 18.00, 'units_in_stock': 39, 'category_id': 1},
            {'product_id': 2, 'product_name': 'Chang', 'unit_price': 19.00, 'units_in_stock': 17, 'category_id': 1},
            {'product_id': 11, 'product_name': 'Queso Cabrales', 'unit_price': 21.00, 'units_in_stock': 22, 'category_id': 4},
        ]

    def _get_fallback_customers(self):
        """Fallback customers if database unavailable"""
        return [
            {'customer_id': 'ALFKI', 'company_name': 'Alfreds Futterkiste', 'contact_name': 'Maria Anders', 'country': 'Germany', 'city': 'Berlin'},
            {'customer_id': 'BONAP', 'company_name': 'Bon app', 'contact_name': 'Laurence Lebihan', 'country': 'France', 'city': 'Marseille'},
        ]

    def generate_new_order_event(self):
        """Generate a new order placement event"""
        self.order_id_counter += 1
        order_id = self.order_id_counter

        # Select random customer and employee
        customer = random.choice(self.customers)
        employee = random.choice(self.employees)
        shipper = random.choice(self.shippers)

        # Generate order line items (1-5 products per order)
        num_items = random.randint(1, 5)
        order_items = []
        order_total = 0

        for _ in range(num_items):
            product = random.choice(self.products)
            quantity = random.randint(1, 20)
            discount = random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2])  # Most orders have no discount

            item_total = product['unit_price'] * quantity * (1 - discount)
            order_total += item_total

            order_items.append({
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'unit_price': product['unit_price'],
                'quantity': quantity,
                'discount': discount,
                'line_total': round(item_total, 2)
            })

        # Calculate freight (shipping cost)
        freight = round(random.uniform(5.0, 50.0), 2)

        event = {
            'event_id': f"evt_{order_id}_{int(time.time())}",
            'event_type': 'new_order',
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'customer_name': customer['company_name'],
            'customer_country': customer['country'],
            'customer_city': customer['city'],
            'employee_id': employee['employee_id'],
            'employee_name': employee['name'],
            'shipper_id': shipper['shipper_id'],
            'shipper_name': shipper['company_name'],
            'order_date': datetime.now().isoformat(),
            'required_date': (datetime.now() + timedelta(days=random.randint(7, 30))).isoformat(),
            'freight': freight,
            'order_items': order_items,
            'order_total': round(order_total, 2),
            'grand_total': round(order_total + freight, 2),
            'item_count': num_items,
            'units_ordered': sum(item['quantity'] for item in order_items)
        }

        return event

    def generate_payment_confirmed_event(self, order_id):
        """Generate payment confirmation event"""
        payment_methods = ['Credit Card', 'Bank Transfer', 'PayPal', 'Invoice']

        event = {
            'event_id': f"evt_payment_{order_id}_{int(time.time())}",
            'event_type': 'payment_confirmed',
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'payment_method': random.choice(payment_methods),
            'payment_status': 'confirmed',
            'confirmation_code': f"PAY{random.randint(100000, 999999)}"
        }

        return event

    def generate_order_shipped_event(self, order_id):
        """Generate order shipped event"""
        event = {
            'event_id': f"evt_ship_{order_id}_{int(time.time())}",
            'event_type': 'order_shipped',
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'shipped_date': datetime.now().isoformat(),
            'tracking_number': f"TRK{random.randint(1000000000, 9999999999)}",
            'shipper': random.choice(self.shippers)['company_name']
        }

        return event

    def generate_order_delivered_event(self, order_id):
        """Generate order delivered event"""
        event = {
            'event_id': f"evt_deliv_{order_id}_{int(time.time())}",
            'event_type': 'order_delivered',
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'delivered_date': datetime.now().isoformat(),
            'delivery_status': 'delivered',
            'signature_required': random.choice([True, False])
        }

        return event

    def generate_order_cancelled_event(self, order_id):
        """Generate order cancellation event"""
        cancellation_reasons = [
            'Customer request',
            'Out of stock',
            'Payment failed',
            'Shipping address invalid',
            'Duplicate order'
        ]

        event = {
            'event_id': f"evt_cancel_{order_id}_{int(time.time())}",
            'event_type': 'order_cancelled',
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'cancellation_reason': random.choice(cancellation_reasons),
            'refund_status': 'pending'
        }

        return event

    def send_event(self, event, key=None):
        """Send event to Kafka topic"""
        try:
            # Use order_id as partition key for ordering guarantees
            if key is None:
                key = str(event.get('order_id', 'unknown'))

            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )

            # Wait for send to complete
            record_metadata = future.get(timeout=10)

            return {
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }

        except KafkaError as e:
            print(f"✗ Error sending event: {e}")
            return None

    def produce_continuous(self, duration_seconds=300, rate_per_second=3):
        """
        Continuously produce order events at specified rate
        Simulates realistic order lifecycle with multiple event types
        """
        print(f"\n{'='*60}")
        print("ORDER EVENT PRODUCTION STARTED")
        print(f"{'='*60}")
        print(f"  Duration: {duration_seconds} seconds")
        print(f"  Rate: {rate_per_second} events/second")
        print(f"  Generating new orders with lifecycle events")
        print("-" * 60)

        start_time = time.time()
        event_count = 0
        error_count = 0
        event_type_counts = {}

        # Track recent orders for lifecycle events
        recent_orders = []

        try:
            while (time.time() - start_time) < duration_seconds:
                events_this_second = []

                for _ in range(rate_per_second):
                    # Determine event type (weighted probabilities)
                    event_type_choice = random.random()

                    if event_type_choice < 0.50:  # 50% new orders
                        event = self.generate_new_order_event()
                        recent_orders.append(event['order_id'])
                        # Keep only last 50 orders for lifecycle events
                        if len(recent_orders) > 50:
                            recent_orders.pop(0)

                    elif event_type_choice < 0.70 and recent_orders:  # 20% payment confirmations
                        order_id = random.choice(recent_orders)
                        event = self.generate_payment_confirmed_event(order_id)

                    elif event_type_choice < 0.85 and recent_orders:  # 15% shipments
                        order_id = random.choice(recent_orders)
                        event = self.generate_order_shipped_event(order_id)

                    elif event_type_choice < 0.95 and recent_orders:  # 10% deliveries
                        order_id = random.choice(recent_orders)
                        event = self.generate_order_delivered_event(order_id)

                    else:  # 5% cancellations
                        if recent_orders:
                            order_id = random.choice(recent_orders)
                            event = self.generate_order_cancelled_event(order_id)
                        else:
                            continue

                    events_this_second.append(event)

                # Send all events for this second
                for event in events_this_second:
                    result = self.send_event(event)

                    if result:
                        event_count += 1
                        event_type = event['event_type']
                        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1

                        # Log interesting events
                        if event['event_type'] == 'new_order':
                            if event.get('order_total', 0) > 1000:
                                print(f"💰 High-value order: #{event['order_id']} - "
                                      f"${event['order_total']:,.2f} from {event['customer_name']}")
                        elif event['event_type'] == 'order_cancelled':
                            print(f"⚠  Order cancelled: #{event['order_id']} - "
                                  f"Reason: {event['cancellation_reason']}")

                        # Periodic progress
                        if event_count % 100 == 0:
                            elapsed = time.time() - start_time
                            rate = event_count / elapsed if elapsed > 0 else 0
                            print(f"✓ Sent {event_count} events | Rate: {rate:.1f}/sec")
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
            print("ORDER EVENT PRODUCTION SUMMARY")
            print("=" * 60)
            print(f"  Total events sent: {event_count}")
            print(f"  Errors: {error_count}")
            print(f"  Duration: {elapsed_time:.2f} seconds")
            print(f"  Average rate: {event_count / elapsed_time:.2f} events/second")
            print("\n  Events by type:")
            for event_type, count in sorted(event_type_counts.items()):
                pct = (count / event_count * 100) if event_count > 0 else 0
                print(f"    {event_type:20s}: {count:4d} ({pct:5.1f}%)")
            print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Northwind Order Event Producer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:29092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='order-events',
                        help='Kafka topic name')
    parser.add_argument('--duration', type=int, default=300,
                        help='Duration in seconds')
    parser.add_argument('--rate', type=int, default=3,
                        help='Events per second')

    args = parser.parse_args()

    # Create producer
    producer = OrderEventProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )

    # Start producing
    producer.produce_continuous(
        duration_seconds=args.duration,
        rate_per_second=args.rate
    )


if __name__ == "__main__":
    main()
