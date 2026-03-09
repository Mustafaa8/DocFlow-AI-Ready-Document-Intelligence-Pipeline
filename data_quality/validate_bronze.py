import json
import os
from datetime import datetime as dt

import great_expectations as gx
import polars as pl

FETCHING_DATE = dt.now().strftime("%Y-%m-%d")

filelist = []
for file in os.listdir():
    if file.endswith(f"{FETCHING_DATE}.json"):
        filelist.append(file)

context = gx.get_context()
suite = context.suites.add(gx.ExpectationSuite("arxiv_bronze_suite"))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column="title"))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column="title"))


data_source = context.data_sources.add_pandas("arxiv_source")
data_assest = data_source.add_dataframe_asset("arxiv_assest")
batch_def = data_assest.add_batch_definition_whole_dataframe("batch")


def file_checking(filename):
    with open(f"./bronez/{filename}", "r") as f:
        data = json.load(f)
    df = pl.DataFrame(data).to_pandas()
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
