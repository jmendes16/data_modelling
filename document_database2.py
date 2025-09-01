import pandas as pd
from pymongo import MongoClient, operations
import uuid
import os
from dotenv import load_dotenv
from datetime import datetime

# --- Configuration ---
CSV_FILE_PATH = "music_data.csv"

def extract(filepath: str) -> pd.DataFrame:
    """
    Extracts data from a CSV file, handling specific null values.
    """
    print(f"EXTRACT: Reading data from {filepath}...")
    try:
        # keep_default_na=False ensures 'None' is treated as a string,
        # while na_values=['none'] handles 'none' as null.
        df = pd.read_csv(
            filepath,
            na_values=['none'],
            keep_default_na=False,
            dtype={'album_id': str}
        )
        # Convert date strings to datetime objects for BSON compatibility
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        return df
    except FileNotFoundError:
        print(f"ERROR: The file '{filepath}' was not found.")
        raise

def transform_to_track_documents(df: pd.DataFrame) -> list[dict]:
    """
    Transforms the DataFrame into a list of track-centric documents.
    """
    print("TRANSFORM: Restructuring data into track documents...")

    # Handle the 'Singles' album logic as before
    artists_with_singles = df[df['is_single'] == True]['artist_id'].unique()
    artist_singles_album_map = {artist_id: str(uuid.uuid4()) for artist_id in artists_with_singles}
    
    def assign_album_id(row):
        if row['is_single']:
            return artist_singles_album_map.get(row['artist_id'])
        return row['album_id']
    
    df['album_id'] = df.apply(assign_album_id, axis=1)

    documents = []
    for _, row in df.iterrows():
        track_doc = {
            "_id": row['track_id'],
            "track_title": row['track_title'],
            "duration_seconds": row['duration_seconds'],
            "is_explicit": row['is_explicit'],
            "genre": row['genre'],
            "popularity_rating": row['popularity_rating'],
            "total_streams": row['total_streams'],
            "artist": {
                "artist_id": row['artist_id'],
                "artist_name": row['artist_name']
            },
            "album": {
                "album_id": row['album_id'],
                "album_name": "Singles" if row['is_single'] else row['album_name'],
                "release_date": None if row['is_single'] else (row['release_date'] if pd.notna(row['release_date']) else None)
            }
        }
        documents.append(track_doc)

    print(f"TRANSFORM: Created {len(documents)} track documents.")
    return documents

def load(documents: list[dict], mongo_uri: str, db_name: str, collection_name: str):
    """
    Loads the track documents into a MongoDB collection.
    """
    print(f"LOAD: Connecting to MongoDB...")
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        print(f"ERROR: Could not connect to MongoDB. Check your MONGO_URI.")
        raise e
    
    if not documents:
        print("LOAD: No documents to insert.")
        return

    # Using BulkWrite for performance is much better than inserting one-by-one
    # ReplaceOne with upsert=True means it will update an artist doc if it exists,
    # or insert it if it's new. This makes the script safely re-runnable.
    requests = [operations.ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in documents]
    
    try:
        print(f"LOAD: Inserting {len(requests)} documents into '{db_name}.{collection_name}'...")
        result = collection.bulk_write(requests)
        print(f"LOAD: Bulk write complete. Matched: {result.matched_count}, Upserted: {result.upserted_count}")
    except Exception as e:
        print(f"ERROR: An error occurred during bulk insert.")
        raise e
    finally:
        client.close()

def main():
    """Main function to orchestrate the ETL process for MongoDB."""
    load_dotenv()
    
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME")
    collection_name = os.getenv("MONGO_TRACK_COLLECTION")

    if not all([mongo_uri, db_name, collection_name]):
        print("ERROR: MongoDB environment variables not set. Check .env file.")
        return

    try:
        raw_data = extract(CSV_FILE_PATH)
        transformed_docs = transform_to_track_documents(raw_data)
        load(transformed_docs, mongo_uri, db_name, collection_name)

        print("\n Track-centric MongoDB collection created successfully!")

    except Exception as e:
        print(f"An unexpected error occurred during the ETL process: {e}")

if __name__ == "__main__":
    main()