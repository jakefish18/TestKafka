import time

from kafka import KafkaProducer

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8') 
)

# Send message to topic
while True:
    producer.send('likes', value='Hello Kafka!')
    producer.flush()  # Wait for all messages to be sent
    print("✅ Message sent")

    time.sleep(3)