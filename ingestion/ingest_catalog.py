import os
import pandas as pd

def generate_sample_rarbg_data(file_path):
    """Generates baseline RARBG torrent catalog dataset if source dump is missing."""
    print("Generating baseline RARBG torrent catalog dataset...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    sample_data = {
        "infohash": [f"a1b2c3d4e5f67890123{i:02d}" for i in range(100)],
        "title": [
            "Inception 2010 1080p BluRay x264",
            "The Witcher S01E01 720p WEBRip",
            "Cyberpunk 2077 v2.0-CODEX",
            "Taylor Swift Midnights 320kbps MP3"
        ] * 25,
        "category": ["movies", "tv", "games", "music"] * 25,
        "size_bytes": [2147483648, 1073741824, 53687091200, 104857600] * 25,
        "dt": ["2023-01-15", "2023-02-10", "2023-03-01", "2023-04-05"] * 25
    }
    df = pd.DataFrame(sample_data)
    df.to_csv(file_path, index=False)
    print(f"Sample catalog CSV generated at: {file_path}")

def main():
    raw_csv_path = "ingestion/rarbg_dump.csv"
    processed_dir = "warehouse"
    os.makedirs(processed_dir, exist_ok=True)

    if not os.path.exists(raw_csv_path):
        generate_sample_rarbg_data(raw_csv_path)

    print("Processing RARBG torrent catalog using Pandas...")
    df = pd.read_csv(raw_csv_path)

    # Cast schema & Add metadata
    df["size_bytes"] = df["size_bytes"].astype("int64")
    df["ingested_at"] = pd.Timestamp.now()

    output_csv_path = os.path.join(processed_dir, "rarbg_catalog.csv")
    print(f"Saving catalog to {output_csv_path}...")
    df.to_csv(output_csv_path, index=False)

    print("\n--- Catalog Ingestion Completed Successfully! ---")

if __name__ == "__main__":
    main()