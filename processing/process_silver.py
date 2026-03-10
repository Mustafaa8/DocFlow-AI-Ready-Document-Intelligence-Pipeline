import polars as pl
from datetime import datetime as dt
import os
from loguru import logger

def load_json_files(date):
    filelist = os.listdir("./bronze")
    filenames = [f"./bronze/{file}" for file in filelist if file.endswith(f"{date}.json")]
    df = pl.DataFrame()
    for file in filenames:
        file_df = pl.read_json(file)
        df = pl.concat([df,file_df])
    logger.info(df.shape)
    return df

def run_processing():
    pass

if __name__ == "__main__":
    fetching_date = dt.now().strftime("%Y-%m-%d")
    load_json_files(fetching_date)
    #run_processing()