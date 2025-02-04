import json
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
BATCH_SIZE = 10
INDEX_NAME = "movies"
INPUT_FILE = "embeddings.jsonl"

es = Elasticsearch(
    cloud_id=ELASTIC_CLOUD_ID,
    api_key=ELASTIC_API_KEY,
)

print(es.info())

def load_data_in_batches(file_path, batch_size):
    batch = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            batch.append({
                "_index": INDEX_NAME,
                "_source": doc
            })
            
            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch

# Bulk insert into Elasticsearch in batches
for batch in load_data_in_batches(INPUT_FILE, BATCH_SIZE):
    try:
        helpers.bulk(es, batch)
        print(f"Inserted {len(batch)} documents into '{INDEX_NAME}' index.")
    except Exception as e:
        print(e)
        print(f"Error inserting documents: {e}")