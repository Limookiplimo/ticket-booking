import time
import json
from kafka import KafkaProducer, producer

TICKET_KAFKA_TOPIC = 'ticket_details'
TICKET_LIMIT = 72

producer = KafkaProducer(bootstrap_servers='localhost:9092')
print('Begin ticket processing')

for ticket in range(1, TICKET_LIMIT+1):
    data = {
        'ticket_id': ticket,
        'passenger_id': f"limoo_{ticket}",
        'fare': 1000,
        'fro': 'nrb',
        'to': 'eld'
    }
    producer.send(
        TICKET_KAFKA_TOPIC,
        json.dumps(data).encode('utf-8')
    )
    print(f'Ticket {ticket}')
    time.sleep(1)
