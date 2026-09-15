import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from deltalake import write_deltalake

config = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "sentinel-delta-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)
TOPIC = "transactions"
DELTA_PATH = "data/delta/transactions"
BATCH_SIZE = 50  # اكتب كل 50 رسالة مرة واحدة


def write_batch(buffer):
    df = pd.DataFrame(buffer)
    write_deltalake(DELTA_PATH, df, mode="append")


def main():
    consumer.subscribe([TOPIC])
    print(f"Listening to '{TOPIC}'... writing to Delta Lake at '{DELTA_PATH}'\n")

    buffer = []
    total_written = 0

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
            buffer.append(transaction)

            if len(buffer) >= BATCH_SIZE:
                write_batch(buffer)
                total_written += len(buffer)
                print(f"💾 Wrote batch of {len(buffer)} rows (total: {total_written})")
                buffer = []

    except KeyboardInterrupt:
        if buffer:
            write_batch(buffer)
            total_written += len(buffer)
            print(f"💾 Wrote final partial batch of {len(buffer)} rows")
        print(f"\nStopping... Total written: {total_written}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()