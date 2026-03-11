import os
from datetime import datetime as dt

import polars as pl
from loguru import logger


def load_json_files(date):
    filelist = os.listdir("./bronze")
    filenames = [
        f"./bronze/{file}" for file in filelist if file.endswith(f"{date}.json")
    ]
    df = pl.DataFrame()
    for file in filenames:
        file_df = pl.read_json(file)
        df = pl.concat([df, file_df])
    return df


def run_processing():
    fetching_date = dt.now().strftime("%Y-%m-%d")
    df = load_json_files(fetching_date)
    logger.info(f"Rows loaded : {df.shape[0]} row")
    df = (
        df.with_columns(
            pl.col("title").str.strip_chars().str.replace_all(r"\s+", " "),
            pl.col("abstract").str.strip_chars().str.replace_all(r"\s+", " "),
            pl.col("published").str.slice(0, 10),
            pl.col("abstract").str.split(" ").list.len().alias("abstract_word_count"),
            pl.col("title").str.split(" ").list.len().alias("title_word_count"),
        )
        .with_columns(
            pl.col("published").str.slice(0, 4).cast(pl.Int32).alias("publish_year"),
            pl.col("published").str.slice(5, 2).cast(pl.Int32).alias("publish_month"),
        )
        .unique(subset=["id"])
    )
    logger.info(f"Rows after processing and deduplication : {df.shape[0]} row")
    os.makedirs("silver", exist_ok=True)
    df.write_parquet(f"./silver/arxiv_{fetching_date}.parquet")
    logger.info(
        f"Silver file has been written to ./silver/arxiv_{fetching_date}.parquet"
    )


if __name__ == "__main__":
    run_processing()
