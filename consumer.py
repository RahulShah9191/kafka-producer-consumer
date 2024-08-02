import time

from kafka3 import KafkaConsumer

# Configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'test-topic1'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # or 'latest'
    group_id='test_topic'
)


try:
    while True:
        for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')} from partition: {message.partition}, offset: {message.offset}")
            consumer.commit()
            time.sleep(1)

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
