import json
from confluent_kafka import Consumer, KafkaError

config = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "sentinel-test-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)
TOPIC = "transactions"


def main():
    consumer.subscribe([TOPIC])
    print(f"Listening to topic '{TOPIC}'... (press Ctrl+C to stop)")

    count = 0
    laundering_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"❌ Error: {msg.error()}")
                continue

            transaction = json.loads(msg.value().decode("utf-8"))
            count += 1

            if transaction["is_laundering"] == 1:
                laundering_count += 1
                print(f"🚨 SUSPICIOUS | account: {transaction['from_account']} "
                      f"| amount: {transaction['amount_paid']} "
                      f"{transaction['payment_currency']}")

            if count % 100 == 0:
                print(f"Processed {count} transactions "
                      f"({laundering_count} suspicious)")

    except KeyboardInterrupt:
        print(f"\nStopping... Total processed: {count} "
              f"| Suspicious: {laundering_count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()