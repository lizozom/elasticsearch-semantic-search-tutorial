import json
import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

DATASET_NAME="jrobischon/wikipedia-movie-plots"
FILE_NAME="./wiki_movie_plots_deduped.csv"
OUTPUT_FILE = "./movies.jsonl"

if not os.path.exists(FILE_NAME):
    api.dataset_download_files(DATASET_NAME, path=".", unzip=True)
else: 
    print(f"Dataset {DATASET_NAME} already downloaded")

if not os.path.exists(OUTPUT_FILE):
    print(f"Converting {FILE_NAME} to {FILE_NAME}...")
    df = pd.read_csv(FILE_NAME)
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as jsonl_file:
        for record in df.to_dict(orient="records"):
            lowercased_record = {key.lower(): value for key, value in record.items()}  # Convert keys to lowercase
            jsonl_file.write(json.dumps(lowercased_record) + "\n")

    print(f"Conversion complete! JSONL file saved as {OUTPUT_FILE}")
else:
    print(f"File {OUTPUT_FILE} already exists")

print("Done!")