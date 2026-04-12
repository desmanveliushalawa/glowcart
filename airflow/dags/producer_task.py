import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker('id_ID')

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = [
    {"id": "P001", "name": "Sepatu Lari", "price": 450000},
    {"id": "P002", "name": "Kaos Polos", "price": 85000},
    {"id": "P003", "name": "Celana Jeans", "price": 320000},
    {"id": "P004", "name": "Jaket Hoodie", "price": 275000},
    {"id": "P005", "name": "Topi Baseball", "price": 95000},
]

def generate_order():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "total_price": product["price"] * quantity,
        "status": random.choice(["pending", "confirmed", "shipped"]),
        "ordered_at": datetime.now().isoformat(),
    }

def generate_user_event():
    product = random.choice(PRODUCTS)
    return {
        "event_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "event_type": random.choice(["view", "click", "add_to_cart", "remove_from_cart"]),
        "product_id": product["id"],
        "product_name": product["name"],
        "session_id": fake.uuid4(),
        "timestamp": datetime.now().isoformat(),
    }

def generate_payment():
    return {
        "payment_id": fake.uuid4(),
        "order_id": fake.uuid4(),
        "amount": random.randint(1, 5) * random.choice([p["price"] for p in PRODUCTS]),
        "method": random.choice(["transfer", "gopay", "ovo", "dana", "cod"]),
        "status": random.choice(["success", "failed", "pending"]),
        "paid_at": datetime.now().isoformat(),
    }

print("Mulai generate data...")
for i in range(30):
    producer.send('orders', value=generate_order())
    producer.send('user-events', value=generate_user_event())
    producer.send('payments', value=generate_payment())
    time.sleep(2)

producer.flush()
print("Selesai generate 30 batch data.")