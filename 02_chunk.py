import json

INPUT_FILE = "movies.jsonl"
OUTPUT_FILE = "chunks.jsonl"

CHUNK_SIZE = 100  # Number of words per chunk
OVERLAP = 20      # Number of words to overlap between chunks

def chunk_text(text, chunk_size, overlap):
    """Splits text into fixed-length chunks with overlapping sections."""
    words = text.split()
    chunks = []
    
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i + chunk_size])
        if chunk:
            chunks.append(chunk)
    
    return chunks

with open(INPUT_FILE, "r", encoding="utf-8") as infile, open(OUTPUT_FILE, "w", encoding="utf-8") as outfile:
    for line in infile:
        movie = json.loads(line)
        plot = movie.get("plot", "").strip()
        
        if plot:
            plot_chunks = chunk_text(plot, CHUNK_SIZE, OVERLAP)
            
            for i, chunk in enumerate(plot_chunks):
                chunk_entry = {
                    **movie,
                    "plot": chunk,
                    "trail": f"chunk_{i}"
                }
                outfile.write(json.dumps(chunk_entry) + "\n")

print(f"Chunking complete. Output saved to {OUTPUT_FILE}")
