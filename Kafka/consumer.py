from kafka import KafkaConsumer

consumer = KafkaConsumer('first-topic', bootstrap_servers = ['localhost:9092'])

for msgs in consumer:
    print(msgs)