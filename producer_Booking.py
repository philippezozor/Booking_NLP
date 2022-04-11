

import py
from kafka import KafkaProducer
import json
import time
import csv
import pandas as pd

# Chargement des données
df = pd.read_csv ('streaming_booking_prepared.csv',sep=';')
df = df.sample(frac=1)

# Preparation du message kafka
p = KafkaProducer(bootstrap_servers=['Localhost:9092']) # api_version=(2,6,0)

for x in df.to_dict(orient="records"):
	p.send("review", json.dumps(x).encode('utf-8'))
	print(json.dumps(x).encode('utf-8'))
	#p.flush()
	time.sleep(5)


    