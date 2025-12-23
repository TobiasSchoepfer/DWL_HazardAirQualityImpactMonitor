import os
import math
import random
from datetime import timedelta
import pandas as pd
import psycopg2

random.seed(42)

# ---------- small plain-Python linear reg trained by SGD ----------
class SimpleLinearTimeModel:
    """
    Linear model: y = w0 + w1*x_aqi + w2*x_fire
    Trained using mean squared error and SGD (plain Python).
    """
    def __init__(self, lr=0.001):
        # weights: [w0 (bias), w1 (aqi_lag), w2 (fire)]
        self.w = [0.0, 0.5, 0.5]
        self.lr = lr

    def predict_one(self, aqi_t, fire_t):
        return self.w[0] + self.w[1] * aqi_t + self.w[2] * fire_t

    def predict_batch(self, aqi_list, fire_list):
        return [self.predict_one(a, f) for a, f in zip(aqi_list, fire_list)]

    def sgd_train(self, x_aqi, x_fire, y_next, epochs=2000):
        # x_aqi, x_fire, y_next are lists of floats, same length n
        n = len(y_next)
        if n == 0:
            return
        for ep in range(epochs):
            # small random minibatch (size 1) to keep it simple and stable
            i = random.randrange(n)
            a = x_aqi[i]
            f = x_fire[i]
            y = y_next[i]
            pred = self.predict_one(a, f)
            err = pred - y  # prediction - target
            # gradients of MSE (for a single sample)
            grad_w0 = 2.0 * err
            grad_w1 = 2.0 * err * a
            grad_w2 = 2.0 * err * f
            # update
            self.w[0] -= self.lr * grad_w0
            self.w[1] -= self.lr * grad_w1
            self.w[2] -= self.lr * grad_w2

# ---------- Lambda handler ----------
def lambda_handler(event, context):
    # === 1. Connect to DB ===
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )
    cursor = conn.cursor()

    # === 2. Query fire data (last 90 days) ===
    fire_query = """
        SELECT acq_date, bright_ti4
        FROM fires
        WHERE acq_date >= NOW() - INTERVAL '90 days'
        ORDER BY acq_date;
    """
    fire_df = pd.read_sql(fire_query, conn)

    # === 3. Query AQI data (last 90 days) ===
    aqi_query = """
        SELECT pollution_ts AS aqi_date, pollution_aqius AS aqi
        FROM air_quality
        WHERE pollution_ts >= NOW() - INTERVAL '90 days'
        ORDER BY pollution_ts;
    """
    aqi_df = pd.read_sql(aqi_query, conn)

    # === 4. Build daily merged series (date, aqi, fire_intensity) ===
    # Normalize column names and types
    if not fire_df.empty:
        fire_df['date'] = pd.to_datetime(fire_df['acq_date']).dt.date
        fire_daily = fire_df.groupby('date')['bright_ti4'].mean().reset_index()
        last_fire_date = pd.to_datetime(fire_daily['date'].max())
        fire_daily.rename(columns={'bright_ti4': 'fire_intensity'}, inplace=True)
    else:
        fire_daily = pd.DataFrame(columns=['date', 'fire_intensity'])

    if not aqi_df.empty:
        aqi_df['date'] = pd.to_datetime(aqi_df['aqi_date']).dt.date
        aqi_daily = aqi_df.groupby('date')['aqi'].mean().reset_index()
    else:
        aqi_daily = pd.DataFrame(columns=['date', 'aqi'])

    # merge by date (left join on aqi_daily so we keep days with aqis)
    df = pd.merge(aqi_daily, fire_daily, on='date', how='left')
    df['fire_intensity'] = df['fire_intensity'].fillna(0.0)

    # ensure sorted by date and reset index
    df.sort_values('date', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # need at least 5 days to train something meaningful
    if len(df) < 5:
        # fallback: use last fire intensity * simple weight (safe fallback)
        last_fire = float(fire_daily['fire_intensity'].iloc[-1]) if not fire_daily.empty else 0.0
        fire_weight = 5.0
        forecast_dates = []
        last_date = pd.to_datetime(fire_daily['date'].iloc[-1]) if not fire_daily.empty else pd.Timestamp.now()
        for i in range(1, 4):
            forecast_dates.append((last_date + timedelta(days=i)).date())
        forecast_aqi = [float(last_fire * fire_weight)] * 3

        # create table (if missing) and insert time-series rows (see below creation)
        _create_prediction_table(cursor, conn)
        fire_event_time = pd.to_datetime(fire_df['acq_date'].max()) if not fire_df.empty else pd.Timestamp.now()
        _insert_timeseries(cursor, conn, forecast_dates, fire_event_time, forecast_aqi)
        cursor.close()
        conn.close()
        return {"statusCode": 200, "message": "Fallback forecast stored (insufficient history)."}

    # Prepare training data:
    # We create samples where features are (AQI_t, Fire_t) and target is AQI_{t+1}
    aqi_vals = df['aqi'].tolist()
    fire_vals = df['fire_intensity'].tolist()
    x_aqi = []
    x_fire = []
    y_next = []
    for i in range(len(df) - 1):
        x_aqi.append(float(aqi_vals[i]))
        x_fire.append(float(fire_vals[i]))
        y_next.append(float(aqi_vals[i + 1]))

    # normalize features a bit to help SGD (simple scaling by mean)
    # compute small scaling factors to keep values near ~1
    mean_aqi = max(1.0, sum(x_aqi) / len(x_aqi))
    mean_fire = max(1.0, sum(x_fire) / len(x_fire))
    x_aqi_scaled = [a / mean_aqi for a in x_aqi]
    x_fire_scaled = [f / mean_fire for f in x_fire]
    y_scaled = [y / mean_aqi for y in y_next]  # scale outputs same as aqi feature

    # train simple linear model on scaled data, then unscale weights later
    model = SimpleLinearTimeModel(lr=0.001)
    model.sgd_train(x_aqi_scaled, x_fire_scaled, y_scaled, epochs=4000)

    # Extract weights and unscale them back to original units:
    # recall: pred_scaled = w0 + w1 * (aqi/mean_aqi) + w2 * (fire/mean_fire)
    # so in original units: pred = mean_aqi * w0 + w1 * aqi + (mean_aqi/mean_fire) * w2 * fire
    w0_s, w1_s, w2_s = model.w
    w0 = mean_aqi * w0_s
    w1 = w1_s
    w2 = (mean_aqi / mean_fire) * w2_s

    # prepare iterative forecast: start from last observed day
    df_before_fire = df[df['date'] <= last_fire_date.date()]
    last_row = df_before_fire.iloc[-1]

    last_aqi = float(last_row['aqi'])
    last_fire_intensity = float(last_row['fire_intensity'])
    last_date = last_fire_date
    # assume future fire intensity stays equal to last observed (can be changed)
    future_fire = last_fire_intensity

    forecast_days = 3
    forecast_dates = []
    forecast_aqi = []
    current_aqi = last_aqi


    for i in range(1, forecast_days + 1):
        pred = w0 + w1 * current_aqi + w2 * future_fire
        # optional: ensure AQI >= 0
        pred = max(0.0, float(pred))
        forecast_dates.append((last_date + timedelta(days=i)).date())
        forecast_aqi.append(float(pred))
        # for next iteration, current_aqi becomes predicted value
        current_aqi = pred

    # === create table if needed and insert the 3-day time series ===
    _create_prediction_table(cursor, conn)
    fire_event_time = pd.to_datetime(fire_df['acq_date'].max()) if not fire_df.empty else pd.Timestamp.now()
    _insert_timeseries(cursor, conn, forecast_dates, fire_event_time, forecast_aqi)

    cursor.close()
    conn.close()

    return {
        "statusCode": 200,
        "message": f"3-day AQI time series forecast stored. Dates: {forecast_dates}"
    }

# ---------- helper functions ----------
def _create_prediction_table(cursor, conn):
    # Drop old table and create the new one with time-series rows (uncomment drop if you want destructive reset)
    # cursor.execute("DROP TABLE IF EXISTS air_quality_predictions;")
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS dv.air_quality_predictions (
            id SERIAL PRIMARY KEY,
            prediction_date DATE,
            fire_event_timestamp TIMESTAMP,
            predicted_aqi FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """
    cursor.execute(create_table_sql)
    conn.commit()

def _insert_timeseries(cursor, conn, dates, fire_event_time, aqi_values):
    cursor.execute("SELECT prediction_date FROM dv.air_quality_predictions;")
    existing_dates = set(row[0] for row in cursor.fetchall())

    insert_sql = """
        INSERT INTO dv.air_quality_predictions
        (prediction_date, fire_event_timestamp, predicted_aqi)
        VALUES (%s, %s, %s);
    """
    for d, a in zip(dates, aqi_values):
        if d not in existing_dates:
            cursor.execute(insert_sql, (d, fire_event_time, float(a)))
            existing_dates.add(d)
    conn.commit()
