from pyspark.sql import DataFrame

RAW_DATA_PATH = "s3a://delta/raw/bitcoin_prices"
CLEAN_DATA_PATH = "s3a://delta/clean/bitcoin_prices"


def _is_empty(df: DataFrame) -> bool:
    return df is None or df.rdd.isEmpty()

def write_raw(df: DataFrame) -> None:
    if _is_empty(df):
        print("No raw data to write.")  
        return

    (df.write.format("delta").mode("append").save(RAW_DATA_PATH))
    print(f"Raw layer: data written to {RAW_DATA_PATH}.")

def write_clean(df: DataFrame) -> None:
    if _is_empty(df):
        print("No clean data to write.")
        return

    (df.write.format("delta").mode("append").save(CLEAN_DATA_PATH))

    print(f"Clean layer: data written to {CLEAN_DATA_PATH}.")