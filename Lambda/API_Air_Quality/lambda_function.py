import json
import os
import psycopg2
import pandas as pd
import requests
import numpy as np
import random
import boto3
from datetime import datetime

# --- Environment variables for secure DB credentials ---
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
S3_BUCKET = os.environ['S3_BUCKET']
S3_FOLDER = os.environ.get('S3_FOLDER', 'air_quality-backup')

# --- AirVisual API configuration ---
API_KEY = "7369b25c-46cc-428a-ab35-cc2805b3bdab"
CITY = "Tokyo"
STATE = "Tokyo"
COUNTRY = "Japan"

TABLE_NAME = "air_quality"

def lambda_handler(event, context):
    try:
        # --- 1️⃣ Connect to PostgreSQL ---
        print("🔌 Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=ENDPOINT,
            dbname=DB_NAME,
            user=USERNAME,
            password=PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        print("❌ Database connection failed:", e)
        return {"status": "error", "message": str(e)}

    try:
        # --- 2️⃣ Create table if it doesn't exist ---
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            city TEXT,
            state TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT,
            pollution_ts TIMESTAMP,
            pollution_aqius INT,
            pollution_mainus TEXT,
            pollution_aqicn INT,
            pollution_maincn TEXT,
            weather_ts TIMESTAMP,
            weather_ic TEXT,
            weather_hu INT,
            weather_pr INT,
            weather_tp INT,
            weather_wd INT,
            weather_ws FLOAT,
            weather_heatIndex INT
        );
        """
        cur.execute(create_sql)
        print(f"✅ Table '{TABLE_NAME}' is ready.")
    except Exception as e:
        print("❌ Table creation failed:", e)
        cur.close()
        conn.close()
        return {"status": "error", "message": str(e)}

    try:
        # --- 3️⃣ Fetch data from AirVisual API ---
        url = f"http://api.airvisual.com/v2/city?city={CITY}&state={STATE}&country={COUNTRY}&key={API_KEY}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}")
        json_data = response.json()
        data = json_data["data"]

        # --- Save raw json to S3 ---
        s3 = boto3.client('s3')
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_FOLDER}/tokyo_air_quality_{timestamp}.json"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(json_data), ContentType='application/json')
        print(f"✅ Raw JSON uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
   
        # --- 4️⃣ Flatten nested JSON ---
        coords = data["location"]["coordinates"]
        pollution = data["current"]["pollution"]
        weather = data["current"]["weather"]

        record = {
            "city": data["city"],
            "state": data["state"],
            "country": data["country"],
            "latitude": coords[1],      # latitude
            "longitude": coords[0],     # longitude
            # convert timestamps to datetime
            "pollution_ts": datetime.fromisoformat(pollution["ts"].replace("Z", "+00:00")),
            "aqius": pollution["aqius"],
            "mainus": pollution["mainus"],
            "aqicn": pollution["aqicn"],
            "maincn": pollution["maincn"],
            "weather_ts": datetime.fromisoformat(weather["ts"].replace("Z", "+00:00")),
            "ic": weather["ic"],
            "hu": weather["hu"],
            "pr": weather["pr"],
            "tp": weather["tp"],
            "wd": weather["wd"],
            "ws": weather["ws"],
            "heatIndex": weather["heatIndex"]
        }

        print("📌 Flattened record:", record)

        # --- 5️⃣ Insert into DB ---
        insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            city, state, country, latitude, longitude,
            pollution_ts, pollution_aqius, pollution_mainus, pollution_aqicn, pollution_maincn,
            weather_ts, weather_ic, weather_hu, weather_pr, weather_tp, weather_wd, weather_ws, weather_heatIndex
        ) VALUES (
            %(city)s, %(state)s, %(country)s, %(latitude)s, %(longitude)s,
            %(pollution_ts)s, %(aqius)s, %(mainus)s, %(aqicn)s, %(maincn)s,
            %(weather_ts)s, %(ic)s, %(hu)s, %(pr)s, %(tp)s, %(wd)s, %(ws)s, %(heatIndex)s
        );
        """
        cur.execute(insert_sql, record)
        print("✅ Insert successful")

    except Exception as e:
        print("❌ API fetch or insert failed:", e)
        cur.close()
        conn.close()
        return {"status": "error", "message": str(e)}

    # --- 6️⃣ Close DB connection ---
    cur.close()
    conn.close()

    return {"status": "success", "message": "Data inserted successfully"}
