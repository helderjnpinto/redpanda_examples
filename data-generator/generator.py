#!/usr/bin/env python3
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pika
from faker import Faker

# Initialize Faker
fake = Faker()

# RabbitMQ connection parameters
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'password')

# Event types
EVENT_TYPES = [
    'product_created',
    'product_updated',
    'order_created',
    'order_updated',
    'subscription_created',
    'subscription_updated'
]

# Product types
PRODUCT_TYPES = [
    'Clothing',
    'Shoes',
    'Accessories',
    'Electronics',
    'Home',
    'Beauty',
    'Food',
    'Books'
]

# Order statuses
ORDER_STATUSES = [
    'pending',
    'processing',
    'completed',
    'cancelled',
    'refunded'
]

# Subscription statuses
SUBSCRIPTION_STATUSES = [
    'active',
    'paused',
    'cancelled',
    'expired'
]

# Sample shop IDs for multi-tenancy
SHOP_IDS = [
    'shop_1001',
    'shop_1002',
    'shop_1003',
    'shop_1004',
    'shop_1005'
]

# Generate a random date within the last 30 days
def random_date(days=30):
    return datetime.now() - timedelta(days=random.randint(0, days))

# Format date as ISO string
def format_date(date):
    return date.isoformat()

# Generate a random price
def random_price(min_price=5.0, max_price=1000.0):
    return round(random.uniform(min_price, max_price), 2)

# Generate a product variant
def generate_variant(product_id):
    price = random_price()
    return {
        'id': str(uuid.uuid4()),
        'product_id': product_id,
        'title': fake.word().capitalize(),
        'price': {
            'amount': str(price),
            'currency': 'USD',
            'number': lambda: price  # This is a hack for Bloblang to call
        },
        'sku': fake.bothify(text='???-######'),
        'inventory_quantity': random.randint(0, 100),
        'weight': random.uniform(0.1, 10.0),
        'weight_unit': 'kg'
    }

# Generate a product
def generate_product():
    product_id = str(uuid.uuid4())
    created_at = random_date(60)
    updated_at = created_at + timedelta(days=random.randint(0, 30))
    
    variants_count = random.randint(1, 5)
    variants = [generate_variant(product_id) for _ in range(variants_count)]
    
    return {
        'type': 'product_created' if random.random() < 0.3 else 'product_updated',
        'id': product_id,
        'shop_id': random.choice(SHOP_IDS),
        'title': fake.catch_phrase(),
        'vendor': fake.company(),
        'product_type': random.choice(PRODUCT_TYPES),
        'created_at': format_date(created_at),
        'updated_at': format_date(updated_at),
        'variants': variants,
        'options': [
            {
                'name': 'Size',
                'values': ['S', 'M', 'L', 'XL']
            },
            {
                'name': 'Color',
                'values': [fake.color_name() for _ in range(3)]
            }
        ],
        'tags': [fake.word() for _ in range(random.randint(0, 5))],
        'status': 'active' if random.random() < 0.8 else 'draft',
        'images': [
            {
                'id': str(uuid.uuid4()),
                'src': fake.image_url()
            }
        ]
    }

# Generate a line item for an order
def generate_line_item(product_id=None):
    if not product_id:
        product_id = str(uuid.uuid4())
        
    price = random_price(10.0, 200.0)
    quantity = random.randint(1, 5)
    
    return {
        'id': str(uuid.uuid4()),
        'product_id': product_id,
        'variant_id': str(uuid.uuid4()),
        'title': fake.catch_phrase(),
        'quantity': quantity,
        'price': {
            'amount': str(price),
            'currency': 'USD',
            'number': lambda: price  # This is a hack for Bloblang to call
        },
        'total_price': {
            'amount': str(price * quantity),
            'currency': 'USD',
            'number': lambda: price * quantity  # This is a hack for Bloblang to call
        }
    }

# Generate an order
def generate_order():
    order_id = str(uuid.uuid4())
    created_at = random_date(30)
    updated_at = created_at + timedelta(hours=random.randint(0, 72))
    
    line_items_count = random.randint(1, 10)
    line_items = [generate_line_item() for _ in range(line_items_count)]
    
    subtotal = sum(float(item['total_price']['amount']) for item in line_items)
    tax_rate = 0.1  # 10% tax
    tax = round(subtotal * tax_rate, 2)
    discount = round(subtotal * random.uniform(0, 0.2), 2) if random.random() < 0.3 else 0
    total = subtotal + tax - discount
    
    return {
        'type': 'order_created' if random.random() < 0.3 else 'order_updated',
        'id': order_id,
        'shop_id': random.choice(SHOP_IDS),
        'order_number': str(random.randint(1000, 9999)),
        'customer': {
            'id': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email()
        },
        'email': fake.email(),
        'created_at': format_date(created_at),
        'updated_at': format_date(updated_at),
        'line_items': line_items,
        'currency': 'USD',
        'subtotal_price': {
            'amount': str(subtotal),
            'currency': 'USD',
            'number': lambda: subtotal  # This is a hack for Bloblang to call
        },
        'total_tax': {
            'amount': str(tax),
            'currency': 'USD',
            'number': lambda: tax  # This is a hack for Bloblang to call
        },
        'total_discounts': {
            'amount': str(discount),
            'currency': 'USD',
            'number': lambda: discount  # This is a hack for Bloblang to call
        },
        'total_price': {
            'amount': str(total),
            'currency': 'USD',
            'number': lambda: total  # This is a hack for Bloblang to call
        },
        'financial_status': random.choice(['pending', 'paid', 'refunded', 'partially_refunded']),
        'fulfillment_status': random.choice(['fulfilled', 'partial', 'unfulfilled']),
        'shipping_lines': [
            {
                'title': 'Standard Shipping',
                'price': {
                    'amount': '10.00',
                    'currency': 'USD'
                }
            }
        ],
        'tax_lines': [
            {
                'title': 'VAT',
                'rate': 0.1,
                'price': {
                    'amount': str(tax),
                    'currency': 'USD'
                }
            }
        ],
        'discount_codes': [
            {
                'code': fake.bothify(text='SALE##'),
                'amount': {
                    'amount': str(discount),
                    'currency': 'USD'
                }
            }
        ] if discount > 0 else []
    }

# Generate a subscription
def generate_subscription():
    subscription_id = str(uuid.uuid4())
    product_id = str(uuid.uuid4())
    variant_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    
    created_at = random_date(90)
    updated_at = created_at + timedelta(days=random.randint(0, 30))
    next_billing_date = datetime.now() + timedelta(days=random.randint(1, 30))
    
    price = random_price(10.0, 100.0)
    quantity = random.randint(1, 3)
    
    intervals = ['day', 'week', 'month', 'year']
    interval = random.choice(intervals)
    interval_count = random.randint(1, 12) if interval == 'month' else random.randint(1, 4)
    
    return {
        'type': 'subscription_created' if random.random() < 0.3 else 'subscription_updated',
        'id': subscription_id,
        'shop_id': random.choice(SHOP_IDS),
        'customer_id': customer_id,
        'status': random.choice(SUBSCRIPTION_STATUSES),
        'created_at': format_date(created_at),
        'updated_at': format_date(updated_at),
        'next_billing_date': format_date(next_billing_date),
        'billing_interval': {
            'interval': interval,
            'interval_count': interval_count
        },
        'product_id': product_id,
        'variant_id': variant_id,
        'quantity': quantity,
        'price': {
            'amount': str(price),
            'currency': 'USD',
            'number': lambda: price  # This is a hack for Bloblang to call
        },
        'customer': {
            'id': customer_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email()
        }
    }

# Generate a random event
def generate_event():
    event_type = random.choice(EVENT_TYPES)
    
    if event_type in ['product_created', 'product_updated']:
        return generate_product()
    elif event_type in ['order_created', 'order_updated']:
        return generate_order()
    elif event_type in ['subscription_created', 'subscription_updated']:
        return generate_subscription()

# Custom JSON encoder to handle callable properties
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if callable(obj):
            return obj()
        return super().default(obj)

# Connect to RabbitMQ
def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        connection_attempts=10,
        retry_delay=5
    )
    
    print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}...")
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declare exchange and queue
    channel.exchange_declare(exchange='shopify', exchange_type='topic', durable=True)
    channel.queue_declare(queue='shopify_events', durable=True)
    channel.queue_bind(exchange='shopify', queue='shopify_events', routing_key='#')
    
    return connection, channel

# Main function
def main():
    connection, channel = connect_to_rabbitmq()
    
    try:
        print("Starting to generate Shopify events...")
        while True:
            event = generate_event()
            event_type = event['type']
            shop_id = event['shop_id']
            
            # Serialize event to JSON
            event_json = json.dumps(event, cls=CustomEncoder)
            
            # Publish to RabbitMQ
            channel.basic_publish(
                exchange='shopify',
                routing_key=f"{shop_id}.{event_type}",
                body=event_json,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
            
            print(f"Published {event_type} event for shop {shop_id}")
            
            # Sleep for a random interval
            sleep_time = random.uniform(0.5, 3.0)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Stopping event generator...")
    finally:
        connection.close()

if __name__ == "__main__":
    # Wait for RabbitMQ to be ready
    time.sleep(10)
    main() 