import time

from kafka3 import KafkaProducer

bootstrap_servers = 'localhost:9092'
topic_name = 'test-topic1'

# Create a Kafka consumer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
)

i = 0

try:
    while True:
        print(f"Sending message: {i}")
        producer.send(topic_name, value=f"Message {i}".encode('utf-8'))
        i += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping producer...")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()

