import json
import uuid
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer


TOPIC_NAME = "trade-events"
BUCKET_NAME = "cm-trade-risk-lake-adnan-6373"
BATCH_SIZE = 10

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "trade-risk-s3-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

s3 = boto3.Session(
    profile_name="trade-risk",
    region_name="us-east-1",
).client("s3")

consumer.subscribe([TOPIC_NAME])
trade_batch = []


def valid_trade(trade):
    required_columns = {
        "trade_id",
        "account_id",
        "security_id",
        "trade_type",
        "quantity",
        "trade_price",
        "trade_timestamp",
    }

    return (
        required_columns.issubset(trade)
        and trade["trade_type"] in ["BUY", "SELL"]
        and trade["quantity"] > 0
        and trade["trade_price"] > 0
    )


def upload_to_s3(records):
    current_time = datetime.now(timezone.utc)
    ingestion_date = current_time.strftime("%Y-%m-%d")
    file_time = current_time.strftime("%Y%m%dT%H%M%S%f")

    s3_key = (
        f"streaming/trades/ingestion_date={ingestion_date}/"
        f"trades_{file_time}_{uuid.uuid4().hex[:8]}.json"
    )

    file_content = "\n".join(json.dumps(record) for record in records)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=file_content.encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Uploaded {len(records)} trades to s3://{BUCKET_NAME}/{s3_key}")


try:
    print("Waiting for trade events...")

    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Kafka error: {message.error()}")
            continue

        trade = json.loads(message.value().decode("utf-8"))
        trade["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

        if valid_trade(trade):
            trade_batch.append(trade)
            print(f"Valid trade received: {trade['trade_id']}")
        else:
            print(f"Invalid trade rejected: {trade}")

        if len(trade_batch) >= BATCH_SIZE:
            upload_to_s3(trade_batch)
            consumer.commit(asynchronous=False)
            trade_batch.clear()

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    if trade_batch:
        upload_to_s3(trade_batch)
        consumer.commit(asynchronous=False)

    consumer.close()