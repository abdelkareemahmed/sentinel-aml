import json
import time
import pandas as pd
from confluent_kafka import Producer

# إعدادات الاتصال بـ Redpanda
config = {"bootstrap.servers": "localhost:19092"}
producer = Producer(config)

TOPIC = "transactions"


def delivery_report(err, msg):
    """تتنادى تلقائياً بعد كل محاولة إرسال."""
    if err is not None:
        print(f"❌ Failed to deliver message: {err}")


def main():
    # نقرأ أول 1000 صف بس في التجربة الأولى
    df = pd.read_csv("data/HI-Small_Trans.csv", nrows=1000)
    print(f"Loaded {len(df)} transactions")

    for index, row in df.iterrows():
        transaction = {
            "timestamp": str(row["Timestamp"]),
            "from_bank": int(row["From Bank"]),
            "from_account": str(row["Account"]),
            "to_bank": int(row["To Bank"]),
            "to_account": str(row["Account.1"]),
            "amount_received": float(row["Amount Received"]),
            "receiving_currency": str(row["Receiving Currency"]),
            "amount_paid": float(row["Amount Paid"]),
            "payment_currency": str(row["Payment Currency"]),
            "payment_format": str(row["Payment Format"]),
            "is_laundering": int(row["Is Laundering"]),
        }

        producer.produce(
            topic=TOPIC,
            key=str(row["Account"]),
            value=json.dumps(transaction),
            callback=delivery_report,
        )

        producer.poll(0)

        if index % 100 == 0:
            print(f"Sent {index} transactions...")

        time.sleep(0.05)

    producer.flush()
    print(f"✅ Done. Sent {len(df)} transactions to topic '{TOPIC}'")


if __name__ == "__main__":
    main()