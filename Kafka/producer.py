from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

producer.send('first-topic', b'hello first topic')
producer.flush()