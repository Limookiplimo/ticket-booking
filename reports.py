import json
from kafka import KafkaConsumer

CONFIRMED_TICKET_KAFKA_TOPIC = 'ticket_confirmed'

consumer = KafkaConsumer(
    CONFIRMED_TICKET_KAFKA_TOPIC,
    bootstrap_servers='localhost:9092'
)

order_count = 0
total_revenue = 0

print('Reports is listening')
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message['total_cost'])

        order_count += 1
        total_revenue += total_cost

        print(f'Total tickets generated: {order_count}')
        print(f'Total revenue generated: {total_revenue}')
