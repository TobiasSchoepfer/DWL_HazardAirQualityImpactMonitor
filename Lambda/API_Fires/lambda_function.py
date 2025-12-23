import os
import psycopg2
import pandas as pd
import requests
from io import StringIO
import boto3
from datetime import datetime

# --- Environment variables for secure DB credentials ---
ENDPOINT = os.environ["ENDPOINT"]
DB_NAME = os.environ["DB_NAME"]
USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]
S3_BUCKET = os.environ['S3_BUCKET']
S3_FOLDER = os.environ.get('S3_FOLDER', 'fire-backup')

# --- NASA FIRE API configuration ---
API_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/608361bbfd7b20f2c124edd64caee8c6/VIIRS_NOAA20_NRT/139.5,35.4,140.0,36.0/5"
TABLE_NAME = "fires"

def lambda_handler(event, context):
    try:
        print("🔌 Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=ENDPOINT,
            dbname=DB_NAME,
            user=USERNAME,
            password=PASSWORD,
        )
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        print("❌ Database connection failed:", e)
        return {"status": "error", "message": str(e)}

    try:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            bright_ti4 FLOAT,
            scan FLOAT,
            track FLOAT,
            acq_date DATE,
            acq_time INT,
            satellite TEXT,
            instrument TEXT,
            confidence TEXT,
            version TEXT,
            bright_ti5 FLOAT,
            frp FLOAT,
            daynight TEXT
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
        response = requests.get(API_URL)
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}")

        s3 = boto3.client('s3')
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_FOLDER}/tokyo_fires_{timestamp}.csv"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
        print(f"✅ Raw CSV uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            latitude, longitude, bright_ti4, scan, track, acq_date, acq_time,
            satellite, instrument, confidence, version, bright_ti5, frp, daynight
        ) VALUES (
            %(latitude)s, %(longitude)s, %(bright_ti4)s, %(scan)s, %(track)s,
            %(acq_date)s, %(acq_time)s, %(satellite)s, %(instrument)s, %(confidence)s,
            %(version)s, %(bright_ti5)s, %(frp)s, %(daynight)s
        );
        """

        for _, row in df.iterrows():
            record = {
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "bright_ti4": row.get("bright_ti4"),
                "scan": row.get("scan"),
                "track": row.get("track"),
                "acq_date": row.get("acq_date"),
                "acq_time": row.get("acq_time"),
                "satellite": row.get("satellite"),
                "instrument": row.get("instrument"),
                "confidence": row.get("confidence"),
                "version": row.get("version"),
                "bright_ti5": row.get("bright_ti5"),
                "frp": row.get("frp"),
                "daynight": row.get("daynight"),
            }
            cur.execute(insert_sql, record)

        print("✅ Insert successful")
    except Exception as e:
        print("❌ API fetch or insert failed:", e)
        cur.close()
        conn.close()
        return {"status": "error", "message": str(e)}

    cur.close()
    conn.close()
    return {"status": "success", "message": "Data inserted successfully"}