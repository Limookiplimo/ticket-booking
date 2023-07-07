import json
from kafka import KafkaConsumer, KafkaProducer

TICKET_KAFKA_TOPIC = 'ticket_details'
CONFIRMED_TICKET_KAFKA_TOPIC = 'ticket_confirmed'

consumer = KafkaConsumer(
    TICKET_KAFKA_TOPIC,
    bootstrap_servers='localhost:9092'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

# Read ticket details from app_backend and loop through each to process them
print('Transaction is listening')
while True:
    for message in consumer:
        print('Transaction in progress')
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        # After processing, write back to kafka the confirmed ticket details
        passenger_id = consumed_message['passenger_id']
        fare = consumed_message['fare']

        data={
            'customer_id':passenger_id,
            'customer_email':f'{passenger_id}@mail.com',
            'total_cost':fare
        }

        print('Transaction done successfully')
        producer.send(
            CONFIRMED_TICKET_KAFKA_TOPIC,
            json.dumps(data).encode('utf-8')
        )
