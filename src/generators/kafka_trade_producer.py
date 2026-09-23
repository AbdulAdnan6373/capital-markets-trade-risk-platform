import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


producer = Producer({"bootstrap.servers": "localhost:9092"})
topic_name = "trade-events"


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(f"Sent trade to partition {message.partition()}")


try:
    while True:
        trade = {
            "trade_id": f"TRD{uuid.uuid4().hex[:10].upper()}",
            "account_id": f"ACC{random.randint(1, 1000):07d}",
            "security_id": f"SEC{random.randint(1, 500):07d}",
            "trade_type": random.choice(["BUY", "SELL"]),
            "quantity": random.randint(1, 1000),
            "trade_price": round(random.uniform(10, 500), 2),
            "trade_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_system": "KAFKA_SIMULATOR",
        }

        producer.produce(
            topic=topic_name,
            key=trade["account_id"],
            value=json.dumps(trade),
            callback=delivery_report,
        )

        producer.poll(0)
        print(trade)
        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.flush()