from fetch_raw import fetch_raw
from fetch_incremental import build_incremental
from delta_write import write_raw, write_clean
from metadata import update_metadata
from spark_session import get_spark


def run_once():
    spark = get_spark("Run Pipeline")
    raw_df = fetch_raw(spark)
    write_raw(raw_df)

    clean_df = build_incremental(raw_df)

    if clean_df.rdd.isEmpty():
        print("No new data to process.")
        return
    
    write_clean(clean_df)

    max_time = clean_df.agg({"event_time": "max"}).collect()[0][0]
    update_metadata(max_time)

    print(f"Pipeline run complete. Updated up to {max_time}.")

if __name__ == "__main__":
    run_once()