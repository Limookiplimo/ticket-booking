import json
from kafka import KafkaConsumer

CONFIRMED_TICKET_KAFKA_TOPIC = 'ticket_confirmed'


consumer = KafkaConsumer(
    CONFIRMED_TICKET_KAFKA_TOPIC,
    bootstrap_servers='localhost:9092'
)


mails_sent = set()
print('Listening for notifications')
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message['customer_email']
        print(f'Sending ticket notification to {customer_email}')
        mails_sent.add(customer_email)
        print(f'Total mails sent: {len(mails_sent)} unique mails')
