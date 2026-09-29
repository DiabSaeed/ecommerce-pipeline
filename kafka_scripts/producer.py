import json
from datetime import datetime
import uuid
import random
import time
from confluent_kafka import Producer

ORDER_TOPIC = "orders"
ORDER_ITEMS_TOPIC = "order_items"

config = {
    "bootstrap.servers":"kafka:9092",
    "acks":"all"
}

producer = Producer(config)

def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery Error as {err}")
    else:
        print(
            f"Delivered to "
            f"{msg.topic()} "
            f"[partition={msg.partition()}] "
            f"offset={msg.offset()}"
        )
try:
    while True:
        order_id = str(uuid.uuid4())
        order_dict = {
            'order_id': order_id,
            'customer_id': f"CUST_{random.randint(1, 1000):04d}",
            'order_date': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choice(['Pending', 'Processing', 'Shipped', 'Delivered'])
        }
        producer.produce(
            ORDER_TOPIC, 
            value=json.dumps(order_dict).encode('utf-8'), 
            callback=delivery_report
        )
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            order_item_id = str(uuid.uuid4())   
            quantity = random.randint(1, 4)
            unit_price = round(random.uniform(10.0, 500.0), 2)
            item_dict = {
                'order_item_id': order_item_id,
                'order_id': order_id,
                'product_id': f"PROD_{random.randint(1, 200):03d}",
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': round(quantity * unit_price, 2)
            }
            producer.produce(
                ORDER_ITEMS_TOPIC, 
                value=json.dumps(item_dict).encode('utf-8'), 
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(random.uniform(1.0, 3.0))
except KeyboardInterrupt:
    print("Generation stops")
finally:
    producer.flush()
    print("Generation end")