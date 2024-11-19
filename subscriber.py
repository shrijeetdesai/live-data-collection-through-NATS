import asyncio
import json
import psycopg2
from nats.aio.client import Client as NATS

async def run():
    """
    Connect to NATS, subscribe to the stock updates, and store them in PostgreSQL.
    """
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="datab",             # Your database name
        user="shree",       # Replace with your PostgreSQL username
        password="desai123",   # Replace with your PostgreSQL password
        host="localhost",           # Assuming PostgreSQL is running locally
        port="5432"
    )
    cursor = conn.cursor()

    # Connect to NATS
    nc = NATS()
    await nc.connect("nats://localhost:4222")  # Connect to the Dockerized NATS

    async def message_handler(msg):
        """
        Handle incoming NATS messages and insert them into PostgreSQL.
        """
        try:
            # Parse the incoming message as JSON
            data = json.loads(msg.data.decode())
            print(f"Received: {data}")

            # Map incoming data to your table columns
            ticker = data['symbol']
            close = data['price']
            timestamp = data['timestamp']

            # Insert data into PostgreSQL (stocks schema, stock_data_new table)
            cursor.execute(
                """
                INSERT INTO stocks.stock_data_new (date, close, ticker)
                VALUES (%s, %s, %s)
                ON CONFLICT (ticker, date) DO NOTHING
                """,
                (timestamp[:10], close, ticker)  # Extract date from timestamp
            )
            conn.commit()
        except json.JSONDecodeError as e:
            print(f"Failed to decode message: {msg.data.decode()} - Error: {e}")
        except Exception as e:
            print(f"Database error: {e}")
            conn.rollback()  # Rollback to clear the aborted transaction

    # Subscribe to the NATS subject
    await nc.subscribe("stocks.live", cb=message_handler)

    # Keep the connection alive
    while True:
        await asyncio.sleep(1)

asyncio.run(run())
