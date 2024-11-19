import asyncio
import json
import yfinance as yf
from nats.aio.client import Client as NATS

def fetch_live_data(symbols):
    """
    Fetch live data from Yahoo Finance for the given stock symbols.
    """
    data = {}
    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        info = ticker.history(period="1d", interval="1m").tail(1)
        if not info.empty:
            data[symbol] = {
                "price": float(info["Close"].iloc[-1]),  # Convert np.float64 to Python float
                "timestamp": info.index[-1].isoformat()
            }
    return data

async def publish_data():
    """
    Connect to the NATS server and continuously publish stock data.
    """
    nc = NATS()
    await nc.connect("nats://localhost:4222")  # Connect to the Dockerized NATS

    symbols = ["AAPL", "GOOGL", "MSFT"]  # Replace with desired stock symbols
    while True:
        # Fetch data from Yahoo Finance
        data = fetch_live_data(symbols)
        for symbol, details in data.items():
            # Serialize data to JSON string
            message = json.dumps({
                "symbol": symbol,
                "price": details["price"],
                "timestamp": details["timestamp"]
            })
            await nc.publish("stocks.live", message.encode())
            print(f"Published: {message}")

        # Wait for 1 minute before the next fetch
        await asyncio.sleep(60)

asyncio.run(publish_data())
