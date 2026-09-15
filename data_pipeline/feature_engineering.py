import json
import time
from collections import defaultdict, deque
from confluent_kafka import Consumer, KafkaError

config = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "sentinel-feature-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)
TOPIC = "transactions"

WINDOW_SECONDS = 60      # النافذة الزمنية: آخر 60 ثانية
VELOCITY_THRESHOLD = 5   # لو حساب بعت 5 معاملات أو أكتر في النافذة دي، يبقى مشبوه

# لكل حساب، هنخزن قايمة بأوقات آخر معاملاته
account_history = defaultdict(deque)


def compute_velocity(account_id, current_time):
    history = account_history[account_id]
    history.append(current_time)

    # اشيل أي وقت أقدم من النافذة الزمنية
    while history and history[0] < current_time - WINDOW_SECONDS:
        history.popleft()

    return len(history)


def main():
    consumer.subscribe([TOPIC])
    print(f"Listening to topic '{TOPIC}'... computing velocity feature\n")

    count = 0
    high_velocity_count = 0

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
            account_id = transaction["from_account"]
            now = time.time()

            velocity = compute_velocity(account_id, now)
            count += 1

            if velocity >= VELOCITY_THRESHOLD:
                high_velocity_count += 1
                print(f"⚠️ HIGH VELOCITY | account: {account_id} "
                      f"| {velocity} transactions in last {WINDOW_SECONDS}s "
                      f"| labeled_laundering: {transaction['is_laundering']}")

            if count % 200 == 0:
                print(f"Processed {count} transactions "
                      f"({high_velocity_count} flagged so far)")

    except KeyboardInterrupt:
        print(f"\nStopping... Total: {count} | Flagged: {high_velocity_count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()