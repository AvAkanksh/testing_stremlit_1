from kafka import KafkaProducer
import json
import time
import random
import pandas as pd
from datetime import datetime

df = pd.read_csv('weather.csv')
columns_to_change = ['temperature_celsius', 'precip_mm', 'wind_kph', 'humidity']
# mean = df[columns_to_change].mean()
std = df[columns_to_change].std()

random_row = df.sample(n=1)

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# Generate and send weather data
def generate_weather_data(only_India=False):
    if(only_India):
        global df
        df = df.query("location_name == 'New Delhi'")
    count = 60
    while (count>0):
        random_row = df.sample(n=1)        
        data = random_row.to_dict(orient='records')[0]
        for column in columns_to_change:
            data[column] = random.gauss(data[column], std[column])
        data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        producer.send('global_weather', data)        
        # print(f"Sent data: {data}")
        time.sleep(1)  # Send data at 2-second intervals
        count-=1

if __name__ == "__main__":
    only_India = True
    generate_weather_data(only_India)
