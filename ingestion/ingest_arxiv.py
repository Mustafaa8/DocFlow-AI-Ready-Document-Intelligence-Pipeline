import json
import os
import time
import xml.etree.ElementTree as et
from datetime import datetime as dt

import requests
from loguru import logger

BASE_URL = "http://export.arxiv.org/api/query"
ARXIV_NS = "{http://www.w3.org/2005/Atom}"


def getting_pdf_link(entry):
    links = entry.findall(f"{ARXIV_NS}link")
    pdf_url = None
    for link in links:
        if link.attrib.get("type") == "application/pdf":
            pdf_url = link.attrib.get("href")
            break
    return pdf_url


def fetch_arxiv_papars(category: str, start: int, max_results: int):
    params = {
        "search_query": f"cat:{category}",
        "start": start,
        "max_results": max_results,
    }
    for i in range(3):
        response = requests.get(f"{BASE_URL}", params=params)
        if response.status_code == 429:
            wait = 30 * i
            logger.warning(f"Rate limiting. waiting {wait} seconds till next request")
            time.sleep(wait)
            continue
        response.raise_for_status()
        break

    root = et.fromstring(response.text)
    papars_for_category = []
    for entry in root.findall(f"{ARXIV_NS}entry"):
        papars_for_category.append(
            {
                "pdf_url": getting_pdf_link(entry),
                "id": entry.find(f"{ARXIV_NS}id").text.strip(),
                "title": entry.find(f"{ARXIV_NS}title").text.strip(),
                "abstract": entry.find(f"{ARXIV_NS}summary").text.strip(),
                "published": entry.find(f"{ARXIV_NS}published").text.strip(),
                "ingestion_ts": dt.now().isoformat(),
                "category": category,
                "source": "arxiv",
            }
        )
    return papars_for_category


def run_ingestion():
    os.makedirs("./bronze", exist_ok=True)
    categories = ["cs.AI", "cs.LG"]

    for category in categories:
        logger.info(f"Starting fetching for category: {category}")
        all_papars = []
        date = dt.now().strftime("%Y-%m-%d")
        filename = f"./bronze/arxiv_{category}_{date}.json"
        for page in range(3):
            logger.info(f"Fetched page {page + 1} for {category}")
            start = page * 100
            results = fetch_arxiv_papars(category, start, 100)
            all_papars.extend(results)
            time.sleep(6)
        with open(filename, "w") as f:
            json.dump(all_papars, f, indent=2)
        logger.info(f"Saved {len(all_papars)} into {filename}")
    logger.info("Finished Fetching Process")


if __name__ == "__main__":
    run_ingestion()
