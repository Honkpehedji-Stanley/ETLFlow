#!/usr/bin/env python3
"""extract.py

Reads data/input_data.csv and writes data/extracted.parquet
"""
from pathlib import Path
import pandas as pd
import sys

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / 'data' / 'input_data.csv'
EXTRACTED = ROOT / 'data' / 'extracted.parquet'

def run(input_path: Path = INPUT, output_path: Path = EXTRACTED):
    print(f'Reading {input_path}')
    df = pd.read_csv(input_path)
    df.to_parquet(output_path, index=False)
    print(f'Wrote {output_path} ({len(df)} rows)')

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print('Error in extract:', e)
        sys.exit(1)
