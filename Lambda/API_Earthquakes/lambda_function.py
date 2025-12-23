import json
import os
import psycopg2
import pandas as pd
import requests
import numpy as np
import random
from datetime import datetime
import xmltodict
import boto3
from io import StringIO

# --- Environment variables ---
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
S3_BUCKET = os.environ['S3_BUCKET']
S3_FOLDER = os.environ.get('S3_FOLDER', 'earthquake-backup')

TABLE_NAME = "earthquakes"

# Tokyo bounding box
LAT_MIN = 33
LAT_MAX = 37
LON_MIN = 137
LON_MAX = 141

# USGS XML feed (Beispiel: letzte 30 Tage)
USGS_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query?"
    "format=quakeml"
    "&starttime=2025-10-26"
    "&endtime=2025-12-31"
    f"&minlatitude={LAT_MIN}"
    f"&maxlatitude={LAT_MAX}"
    f"&minlongitude={LON_MIN}"
    f"&maxlongitude={LON_MAX}"
)

def lambda_handler(event, context):
    # --- 1️⃣ Connect to PostgreSQL ---
    try:
        conn = psycopg2.connect(
            host=ENDPOINT,
            dbname= DB_NAME,
            user=USERNAME,
            password=PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        print("❌ Database connection failed:", e)
        return {"status": "error", "message": str(e)}

    # --- 2️⃣ Create table if not exists ---
    try:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            event_id TEXT,
            title TEXT,
            magnitude FLOAT,
            place TEXT,
            time TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            depth FLOAT
        );
        """
        cur.execute(create_sql)
        print(f"✅ Table '{TABLE_NAME}' ready.")
    except Exception as e:
        print("❌ Table creation failed:", e)
        cur.close()
        conn.close()
        return {"status": "error", "message": str(e)}

    # --- 3️⃣ Fetch data from USGS ---
    try:
        response = requests.get(USGS_URL)
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}")

        s3 = boto3.client('s3')
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_FOLDER}/tokyo_earthquakes_{timestamp}.xml"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
        print(f"✅ Raw XML uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

        data = xmltodict.parse(response.content)
        events = data['q:quakeml']['eventParameters']['event']

        for event in events:
            try:
                lat = float(event['origin']['latitude']['value'])
                lon = float(event['origin']['longitude']['value'])
            except:
                continue  # skip if missing coordinates

            # Filter: Tokyo bounding box
            if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
                continue

            # Extract data
            magnitude = float(event['magnitude']['mag']['value'])
            place = event.get('description', {}).get('text', '')
            time_str = event['origin']['time']['value']
            event_id = event['@publicID']
            title = f"M{magnitude} - {place}"
            depth = float(event['origin']['depth']['value'])

            record = {
                "event_id": event_id,
                "title": title,
                "magnitude": magnitude,
                "place": place,
                "time": datetime.fromisoformat(time_str.replace("Z", "+00:00")),
                "latitude": lat,
                "longitude": lon,
                "depth": depth
            }

            # --- 4️⃣ Insert into DB ---
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                event_id, title, magnitude, place, time, latitude, longitude, depth
            ) VALUES (
                %(event_id)s, %(title)s, %(magnitude)s, %(place)s, %(time)s, %(latitude)s, %(longitude)s, %(depth)s
            );
            """
            cur.execute(insert_sql, record)

        print("✅ Tokyo earthquakes inserted.")

    except Exception as e:
        print("❌ API fetch or insert failed:", e)
        cur.close()
        conn.close()
        return {"status": "error", "message": str(e)}

    # --- 5️⃣ Close DB connection ---
    cur.close()
    conn.close()

    return {"status": "success", "message": "Earthquake data inserted successfully"}
