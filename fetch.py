import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()

# Function to create table if it doesn't exist
def create_table():
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS student.am_capstone_cryptocurrency_data (
        id SERIAL PRIMARY KEY,
        coin_id VARCHAR(255),
        symbol VARCHAR(255),
        name VARCHAR(255),
        current_price NUMERIC,
        market_cap BIGINT,
        total_volume BIGINT,
        last_updated TIMESTAMP,
        UNIQUE (coin_id, last_updated)
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()

# Function to insert data into the table
def insert_data(data):
    insert_query = '''
    INSERT INTO student.am_capstone_cryptocurrency_data (coin_id, symbol, name, current_price, market_cap, total_volume, last_updated)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (coin_id, last_updated) DO NOTHING;
    '''
    for record in data:
        cursor.execute(insert_query, (
            record['id'],
            record['symbol'],
            record['name'],
            record['current_price'],
            record['market_cap'],
            record['total_volume'],
            record['last_updated']
        ))
    conn.commit()

# Function to fetch cryptocurrency data
def fetch_crypto_data():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 100,
        'page': 1,
        'sparkline': 'false'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Create the table
create_table()

# Fetch and store cryptocurrency data
crypto_data = fetch_crypto_data()
if crypto_data:
    for item in crypto_data:
        item['last_updated'] = datetime.strptime(item['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ')
    insert_data(crypto_data)

# Close the database connection
conn.close()
