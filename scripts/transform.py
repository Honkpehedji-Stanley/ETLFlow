#!/usr/bin/env python3
"""transform.py

Reads data/extracted.parquet, performs cleaning/renaming/enrichment and writes data/transformed.parquet
"""
from pathlib import Path
import pandas as pd
import sys

ROOT = Path(__file__).resolve().parents[1]
EXTRACTED = ROOT / 'data' / 'extracted.parquet'
TRANSFORMED = ROOT / 'data' / 'transformed.parquet'

def run(input_path: Path = EXTRACTED, output_path: Path = TRANSFORMED):
    print(f'Reading {input_path}')
    df = pd.read_parquet(input_path)

    # Basic cleaning
    # Rename columns to snake_case if needed
    df = df.rename(columns=lambda c: c.strip().lower())

    # Drop rows without order_id
    if 'order_id' in df.columns:
        df = df[df['order_id'].notna()]

    # Ensure numeric types
    for col in ('quantity', 'price'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate derived column
    if 'quantity' in df.columns and 'price' in df.columns:
        df['total_price'] = df['quantity'].fillna(0) * df['price'].fillna(0.0)

    # Parse dates
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Filter out zero or negative totals
    if 'total_price' in df.columns:
        df = df[df['total_price'] > 0]

    df.to_parquet(output_path, index=False)
    print(f'Wrote {output_path} ({len(df)} rows)')

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print('Error in transform:', e)
        sys.exit(1)
