from confluent_kafka import Producer
import csv
import json
import sys
import time

def receipt(err, msg):
    if err is not None:
        print('Error: {}', format(err))
    else:
        message = 'Produces message on topic {}:{}'.format(msg.topic(), msg.value().decode('utf-8'))
        print(message) 

if __name__ == '__main__':
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    topic = 'taxi-porto'
    print('Kafka Producer has been initiated')

    reader = csv.reader(open('train.csv'))
    next(reader)
    first_line = True

    while True:
        try:
            line = next(reader, None)
            json_line = json.dumps(line)

            producer.produce(topic, key = 'porto', value = json_line, callback = receipt)
            producer.flush()
            time.sleep(2)
        except TypeError:
            sys.exit()

