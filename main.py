import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Database URL
DATABASE_URL = os.getenv('DB_URL')
print(f'The database URL is: {DATABASE_URL}')

# Create SQLAlchemy engine
try:
    engine = create_engine(DATABASE_URL)
    print('Database engine created successfully.')
except Exception as e:
    print(f'Error creating engine: {e}')
    exit(1)

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

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
        print(f"Response Status Code: {response.status_code}")
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Something went wrong: {err}")
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
        try:
            df.to_sql('binance_p2p_data', engine, if_exists='append', index=False)
            print('Data stored successfully in the database.')
        except Exception as e:
            print(f'Error adding to DB: {e}')

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
    # Sleep for a specified interval (e.g., 1 hour)