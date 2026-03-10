import json
import os
from datetime import datetime as dt

import great_expectations as gx
import polars as pl
from loguru import logger


def file_checking(filename, batch_def, suite):
    with open(f"./bronze/{filename}", "r") as f:
        data = json.load(f)
    df = pl.DataFrame(data).to_pandas()
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    results = batch.validate(suite)
    if results.success:
        logger.info(f"PASSED: {filename}")
    else:
        logger.error(f"FAILED: {filename}")


def run_validation():
    FETCHING_DATE = dt.now().strftime("%Y-%m-%d")
    filelist = []
    
    for file in os.listdir("./bronze"):
        if file.endswith(f"{FETCHING_DATE}.json"):
            filelist.append(file)
    if not filelist:
        logger.warning(f"no bronze files found for {FETCHING_DATE}")
        return

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("arxiv_source")
    data_assest = data_source.add_dataframe_asset("arxiv_assest")
    batch_def = data_assest.add_batch_definition_whole_dataframe("batch")
    
    suite = context.suites.add(gx.ExpectationSuite("arxiv_bronze_suite"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="pdf_url"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="title"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="abstract"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="published"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="ingestion_ts"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="category"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="source"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="title"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="abstract")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(
            column="category", value_set=["cs.AI", "cs.LG"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="source", value_set=["arxiv"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="pdf_url", regex=r"https://arxiv\.org/pdf/.*"
        )
    )
    
    for file in filelist:
        file_checking(file, batch_def, suite)

if __name__ == "__main__":
    run_validation()