from kafka import KafkaConsumer

# Initialize consumer
consumer = KafkaConsumer(
    'likes',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Read from beginning if no offset is committed
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda v: v.decode('utf-8')  # Decode bytes to string
)

print("📥 Listening for messages...")

# Consume messages
for message in consumer:
    print(f"✅ Received: {message.value}")
