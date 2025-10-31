#!/usr/bin/env python3
"""load.py

Reads data/transformed.parquet and inserts into PostgreSQL table `sales`.
Connection parameters are read from environment variables:
- POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
"""
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine
import sys

ROOT = Path(__file__).resolve().parents[1]
TRANSFORMED = ROOT / 'data' / 'transformed.parquet'

def get_db_url():
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    db = os.getenv('POSTGRES_DB', 'postgres')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'

def run(input_path: Path = TRANSFORMED):
    print(f'Reading {input_path}')
    df = pd.read_parquet(input_path)
    if df.empty:
        print('No data to load.')
        return

    engine = create_engine(get_db_url())
    with engine.begin() as conn:
        # to_sql will create the table if it doesn't exist
        df.to_sql('sales', con=conn, if_exists='append', index=False)
    print(f'Inserted {len(df)} rows into sales table')

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print('Error in load:', e)
        sys.exit(1)
