from app import app
from flask import Flask, abort, request 
from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

@app.route('/index', methods=['POST'])
def index():

    data = json.dumps(request.json)
    print(data, type(data))
    producer.send('yash', data.encode('utf-8'))
    return "Data Sent"
