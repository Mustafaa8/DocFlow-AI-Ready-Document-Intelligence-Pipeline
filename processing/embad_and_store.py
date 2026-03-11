from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from fastembed import TextEmbedding
from datetime import datetime as dt 
import os
import polars as pl
from loguru import logger

def load_silver_data():
    fetching_date = dt.now().strftime("%Y-%m-%d")
    filename = None
    for file in os.listdir("./silver"):
        if file.endswith(f"{fetching_date}.parquet"):
            filename = file
    if filename:
        df = pl.read_parquet(f"./silver/{filename}")
        return df
    else:
        logger.error(f"No Silver Files has been found for date {fetching_date}")
        raise 
    
def text_preparation():
    df = load_silver_data()
    text = [f"{row["title"]}. {row["abstract"]}" for row in df.to_dicts()]
    return text
    
def run_embedding():
    data_text = text_preparation()
    model = TextEmbedding("BAAI/bge-small-en-v1.5")
    embeddings = list(model.embed(data_text))
    client = QdrantClient(host="localhost",ports="6333")
    client.rec
    

    
if __name__ == "__main__":
    run_embedding