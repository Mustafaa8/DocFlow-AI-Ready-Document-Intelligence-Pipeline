import os
from datetime import datetime as dt

import polars as pl
from fastembed import TextEmbedding
from loguru import logger
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams


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
    return df.to_dicts(), text


def run_embedding():
    papers, data_text = text_preparation()
    logger.info(f"Loaded {len(papers)} papers")
    model = TextEmbedding("BAAI/bge-small-en-v1.5")

    logger.info("Model loaded — starting embeddings")
    client = QdrantClient(host="qdrant", port=6333, prefer_grpc=False)
    if not client.collection_exists("arxiv_papers"):
        client.create_collection(
            "arxiv_papers",
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
    logger.info("Collection created")
    batch_size = 100
    for i in range(0, len(papers), batch_size):
        batch_papers = papers[i : i + batch_size]
        batch_texts = data_text[i : i + batch_size]
        batch_embeddings = list(model.embed(batch_texts))
        batch_points = []
        for paper, embedding in zip(batch_papers, batch_embeddings):
            batch_points.append(
                PointStruct(
                    id=abs(hash(paper["id"])) % (10**9),
                    vector=embedding.tolist(),
                    payload={
                        "title": paper["title"],
                        "abstract": paper["abstract"],
                        "published": paper["published"],
                        "ingestion_ts": paper["ingestion_ts"],
                        "category": paper["category"],
                        "source": paper["source"],
                    },
                )
            )
        client.upsert(collection_name="arxiv_papers", points=batch_points)
        logger.info(
            f"Upserted batch {i // batch_size + 1}/{(len(papers) + batch_size - 1) // batch_size}"
        )
    logger.info(f"Done — {len(papers)} points upserted into Qdrant")


if __name__ == "__main__":
    run_embedding()
