# Module Imports
import mariadb
import sys
import random
import sys
import Adafruit_DHT
from datetime import datetime, timezone
import uuid
import json 
import time

with open('config.json', 'r') as f:
    config = json.load(f)


# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=config.get("username"),
        password=config.get("password"),
        host=config.get("host"),
        port=3306,
        database=config.get("database")

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# setup dht_22 sensor - a temp and humidity sensor
DHT_SENSOR = Adafruit_DHT.DHT22
DHT_PIN = 4 

# Get Cursor
cur = conn.cursor()

cnt = 0
while True:
    timestamp = datetime.now(timezone.utc)

    if timestamp.minute in [0, 30]:

        humidity, temperature = Adafruit_DHT.read_retry(DHT_SENSOR, DHT_PIN)
        timestamp = timestamp.strftime("%Y/%m/%d %H:%M:%S")


        if humidity is not None and temperature is not None:
            
            print(temperature, humidity)
            
            print('Temp={0:0.1f}*  Humidity={1:0.1f}%'.format(temperature, humidity))
            
            #insert temp
            try:
                cur.execute("INSERT INTO Readings (ReadingId, SensorId, Value, Timestamp) VALUES (?, ?, ?, ?)", (str(uuid.uuid4()),"6de58c3e-ad9d-49b8-bd71-cbd0f6023b6f",str(temperature),str(timestamp)))
                conn.commit()
            except mariadb.Error as e:
                print(f"Error: {e}")


            #insert humidity
            try:
                cur.execute("INSERT INTO Readings (ReadingId, SensorId, Value, Timestamp) VALUES (?, ?, ?, ?)", (str(uuid.uuid4()),"7597211a-7c3a-11ee-9aa9-f9b59efa093d",str(humidity),str(timestamp)))
                conn.commit()
            except mariadb.Error as e:
                print(f"Error: {e}") 
    

        else:
            print('Failed to get reading. Try again!')

    time.sleep(60)