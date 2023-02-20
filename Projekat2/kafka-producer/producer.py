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
    producer = Producer({'bootstrap.servers': 'localhost:29092'})
    topic = 'taxiporto'
    print('Kafka Producer has been initiated')

    with open('porto.csv') as csvFile:  
        data = csv.DictReader(csvFile)
        for row in data:
            row['trip_id'] = int(row['trip_id'])
            row['origin_call'] = int(row['origin_call']) if row['origin_call'] != "" else -1
            row['origin_stand'] = int(row['origin_stand']) if row['origin_stand'] != "" else -1
            row['taxi_id'] = int(row['taxi_id'])
            row['start_time'] = int(row['start_time'])
            row['end_time'] = float(row['end_time'])
            row['trip_duration'] = float(row['trip_duration'])
            row['start_lon'] = float(row['start_lon']) if row['start_lon'] != "" else -8.652186
            row['start_lat'] = float(row['start_lat']) if row['start_lat'] != "" else 41.15178
            row['end_lon'] = float(row['end_lon']) if row['end_lon'] != "" else -8.619606
            row['end_lat'] = float(row['end_lat']) if row['end_lat'] != "" else 41.149827

            producer.produce(topic, key = 'porto', value = json.dumps(row), callback = receipt)
            producer.flush()
            
            time.sleep(0.1)

    print('Kafka message producer done!')

