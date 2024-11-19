import json
import yfinance as yf
from nats.aio.client import Client as NATS
import asyncio
import psycopg2

def fetch_live_data(symbols):
    """
    Fetch live data from Yahoo Finance for the given stock symbols.
    """
    data = {}
    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        info = ticker.history(period="1d", interval="1d").tail(1)  # Fetch 1 day's data
        if not info.empty:
            data[symbol] = {
                "price": float(info["Close"].iloc[-1]),  # Closing price
                "timestamp": info.index[-1].isoformat()  # Date of the data
            }
    return data

async def publish_daily_data():
    """
    Connect to the public NATS server, publish stock data once, and exit.
    """
    nc = NATS()
    await nc.connect("nats://demo.nats.io:4222")  # Public NATS server

    symbols = ["AAPL", "GOOGL", "MSFT"]  # Replace with your desired stock symbols
    data = fetch_live_data(symbols)
    for symbol, details in data.items():
        message = json.dumps({
            "symbol": symbol,
            "price": details["price"],
            "timestamp": details["timestamp"]
        })
        await nc.publish("stocks.live", message.encode())
        print(f"Published: {message}")

    await nc.close()

async def update_postgres():
    """
    Insert stock data into PostgreSQL cloud database.
    """
    # Connect to cloud-hosted PostgreSQL database
    conn = psycopg2.connect(
        dbname="datab",
        user="shree",
        password="desai123",
        host="localhost",  # Replace with your PostgreSQL cloud host
        port="5432"
    )
    cursor = conn.cursor()

    symbols = ["AAPL", "GOOGL", "MSFT"]
    data = fetch_live_data(symbols)
    for symbol, details in data.items():
        cursor.execute(
            """
            INSERT INTO stocks.stock_data_new (date, close, ticker)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker, date) DO NOTHING
            """,
            (details["timestamp"][:10], details["price"], symbol)
        )
        conn.commit()
        print(f"Inserted: {symbol} - {details['price']}")

    conn.close()

# Run both publishing to NATS and updating PostgreSQL
async def main():
    await publish_daily_data()
    await update_postgres()

asyncio.run(main())
