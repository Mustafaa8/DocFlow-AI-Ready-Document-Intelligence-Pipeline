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
        raise FileNotFoundError(f"No silver files found for {fetching_date}")
    
def text_preparation():
    df = load_silver_data()
    text = [f"{row['title']}. {row['abstract']}" for row in df.to_dicts()]
    return df.to_dicts(),text
    
def run_embedding():
    df_dict,data_text = text_preparation()
    model = TextEmbedding("BAAI/bge-small-en-v1.5")
    embeddings = list(model.embed(data_text))
    client = QdrantClient(host="localhost",port=6333)
    client.recreate_collection("arxiv_papers",vectors_config=VectorParams(size=384, distance=Distance.COSINE))
    
if __name__ == "__main__":
    run_embedding()