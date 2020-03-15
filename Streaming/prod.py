from kafka import KafkaProducer
from json import dumps
import pandas as pd
FILE_TO_STREAM = "busFile"

if __name__ == '__main__':
    streamer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))
    with open(FILE_TO_STREAM, "r") as file:
        for ind, line in enumerate(file):
            streamer.send('ping', value=line)