import os
import argparse
from datetime import datetime

from fetch_incremental import fetch_incremental
from delta_write import write_delta
from metadata import update_metadata


def run_once():
    df = fetch_incremental()
    if df.empty:
        print("No new data to process.")
        return
    
    write_delta(df)
    max_ts = df['time_period_start'].max()
    update_metadata(max_ts)
    print(f"Appended {len(df)} rows, updated metadata to {max_ts}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-once",
        action="store_true",
    )
    args = parser.parse_args()

    if args.run_once:
        run_once()
    else:
        run_once()