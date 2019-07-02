#!/usr/bin/env python
# coding: utf-8

# In[9]:


from flask import Flask
from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv
import json

app = Flask(__name__)

from app import routes


# In[5]:


@app.route("/hello")
def hello():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    csvfile = open('files/abalone.data', 'r')
    fieldnames = ("Sex", "Length", "Diameter", "Height", "Whole_weight", "Shucked", "weight", "Viscera weight", "Shell weight", "Rings")
    reader = csv.DictReader( csvfile, fieldnames)
    out = json.dumps( [ row for row in reader ] )
    
    
    return producer.send('twitter', out.encode('utf-8'))


# In[ ]:




