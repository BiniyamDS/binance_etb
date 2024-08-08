import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine
import os
# from flask import Flask

from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()


# Database URL
DATABASE_URL = os.getenv('DB_URL')
print(DATABASE_URL)

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def fetch_binance_p2p_data(asset='USDT', trade_type='BUY', fiat='USD', page=1):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

    payload = {
        "asset": asset,
        "fiat": fiat,
        "page": page,
        "rows": 10,  # Number of rows per page
        "tradeType": trade_type  # 'BUY' or 'SELL'
    }

    headers = {
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def store_p2p_data_to_db(buy_data, sell_data):
    records = []

    # Process buy data
    if buy_data and buy_data.get('data'):
        for ad in buy_data['data']:
            details = ad['adv']
            advertiser = ad['advertiser']
            record = {
                'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Price': details['price'],
                'Available Quantity': details['surplusAmount'],
                'Fiat': details['fiatUnit'],
                'User': advertiser['nickName'],
                'Order Count': advertiser['monthOrderCount'],
                'Completion Rate (%)': advertiser['monthFinishRate'],
                'Trade Type': 'BUY'
            }
            records.append(record)

    # Process sell data
    if sell_data and sell_data.get('data'):
        for ad in sell_data['data']:
            details = ad['adv']
            advertiser = ad['advertiser']
            record = {
                'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Price': details['price'],
                'Available Quantity': details['surplusAmount'],
                'Fiat': details['fiatUnit'],
                'User': advertiser['nickName'],
                'Order Count': advertiser['monthOrderCount'],
                'Completion Rate (%)': advertiser['monthFinishRate'],
                'Trade Type': 'SELL'
            }
            records.append(record)

    # Convert to DataFrame
    if records:
        df = pd.DataFrame(records)
        # Store DataFrame in PostgreSQL
        df.to_sql('binance_p2p_data', engine, if_exists='append', index=False)

# app = Flask(__name__)

# @app.route('/')
# def index():
#     return "The service is running and connected to the database."

if __name__ == "__main__":
    asset = 'USDT'
    fiat = 'ETB'

    print('Initiating request...')
    # Fetch buy and sell data
    buy_data = fetch_binance_p2p_data(asset=asset, trade_type='BUY', fiat=fiat)
    sell_data = fetch_binance_p2p_data(asset=asset, trade_type='SELL', fiat=fiat)

    # Store the data in the database
    store_p2p_data_to_db(buy_data, sell_data)
    
    print(f"Data stored at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Sleep for a specified interval (e.g., 10 minutes)
    # time.sleep(3600)  # 3600 seconds = 1 hour
    # app.run(host='0.0.0.0', port=5000)