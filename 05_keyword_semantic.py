import json
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()
import cohere

ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
BATCH_SIZE = 10
INDEX_NAME = "movies"
INPUT_FILE = "embeddings.jsonl"

co = cohere.ClientV2()
es = Elasticsearch(
    cloud_id=ELASTIC_CLOUD_ID,
    api_key=ELASTIC_API_KEY,
)

question = "Movies that talk about machines enslaving humanity"
question_embedding = co.embed(texts=[question], model="embed-english-v3.0", input_type="search_query", embedding_types=["float"])
vector = question_embedding.embeddings.float_[0]

query = {
     "knn": {
        "field": "plot_embedding",
        "k": 10,
        "num_candidates": 100,
        "query_vector":vector,
        }

}

# Execute the query
response = es.search(index=INDEX_NAME, body=query)

# Print results
print("Search Results:")
for hit in response["hits"]["hits"]:
    print(f"ID: {hit['_id']}, Title: {hit['_source']['title']}, Score: {hit['_score']}")
