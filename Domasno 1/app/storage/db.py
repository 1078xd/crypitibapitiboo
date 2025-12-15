import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def save_ohlcv(symbol, records):
    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO ohlcv (date, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    batch = [
        (r["date"], symbol, r["open"], r["high"], r["low"], r["close"], r["volume"])
        for r in records
    ]

    try:
        cur.executemany(query, batch)
        conn.commit()
        print(f"Inserted {len(batch)} rows for {symbol}")

    except Exception as e:
        conn.rollback()
        print("DB error:", e)

    finally:
        cur.close()
        conn.close()
