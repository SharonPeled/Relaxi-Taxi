from kafka import KafkaConsumer
from json import loads
import pandas as pd
from datetime import datetime


if __name__ == '__main__':
    consumer = KafkaConsumer(
        'Relaxi_Taxi',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: x.decode('utf-8'))

    for msg in consumer:
        print(msg)
        print()