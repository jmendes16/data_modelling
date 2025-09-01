import pandas as pd
from pymongo import MongoClient, operations
import uuid
import os
from dotenv import load_dotenv
import json
from datetime import datetime

# --- Configuration ---
CSV_FILE_PATH = "music_data.csv"

# --- ETL Functions ---

def extract(filepath: str) -> pd.DataFrame:
    """
    Extracts data from a CSV file into a pandas DataFrame
    """
    print(f"EXTRACT: Reading data from {filepath}...")
    try:
        # keep_default_na=False ensures 'None' is treated as a string,
        # while na_values=['none'] handles 'none' as null.
        df = pd.read_csv(
            filepath,
            na_values=['none'],
            keep_default_na=False,
            dtype={'album_id': str} # Ensure IDs are read as strings
        )
        # Convert date strings to datetime objects for BSON compatibility
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        return df
    except FileNotFoundError:
        print(f"ERROR: The file '{filepath}' was not found.")
        raise

def transform(df: pd.DataFrame) -> list[dict]:
    """
    Transforms the raw DataFrame into a list of structured artist documents
    ready for MongoDB insertion.
    """
    print("TRANSFORM: Restructuring data into artist documents...")

    # Handle the 'Singles' album logic
    artists_with_singles = df[df['is_single'] == True]['artist_id'].unique()
    artist_singles_album_map = {artist_id: str(uuid.uuid4()) for artist_id in artists_with_singles}

    def assign_album_id(row):
        if row['is_single']:
            return artist_singles_album_map.get(row['artist_id'])
        return row['album_id']

    df['album_id'] = df.apply(assign_album_id, axis=1)

    # Structure the data by grouping
    artist_documents = []
    # Group by artist first
    for artist_id, artist_group in df.groupby('artist_id'):
        artist_doc = {
            "_id": artist_id, # Use the existing artist_id as MongoDB's primary key
            "artist_name": artist_group['artist_name'].iloc[0],
            "albums": []
        }

        # Now group that artist's tracks by album
        for album_id, album_group in artist_group.groupby('album_id'):
            # Create a 'Singles' album if necessary, otherwise use album data
            if album_id in artist_singles_album_map.values():
                album_data = {
                    "album_id": album_id,
                    "album_name": "Singles",
                    "release_date": None,
                }
            else:
                first_row = album_group.iloc[0]
                album_data = {
                    "album_id": album_id,
                    "album_name": first_row['album_name'],
                    "release_date": first_row['release_date'] if pd.notna(first_row['release_date']) else None,
                }
            
            # Use .to_dict('records') for efficient conversion to a list of dicts
            tracks_data = album_group[[
                'track_id', 'track_title', 'duration_seconds', 'is_explicit',
                'genre', 'popularity_rating', 'total_streams'
            ]].to_dict('records')

            album_data['tracks'] = tracks_data
            artist_doc['albums'].append(album_data)
        
        artist_documents.append(artist_doc)

    print(f"TRANSFORM: Created {len(artist_documents)} artist documents.")
    return artist_documents

def load(documents: list[dict], mongo_uri: str, db_name: str, collection_name: str):
    """
    Loads the transformed documents into a MongoDB collection.
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
        print("LOAD: No documents to insert. Skipping.")
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
    collection_name = os.getenv("MONGO_COLLECTION_NAME")

    if not all([mongo_uri, db_name, collection_name]):
        print("ERROR: MongoDB environment variables not set. Please check your .env file.")
        return

    try:
        # Run the ETL pipeline
        raw_data = extract(CSV_FILE_PATH)
        transformed_docs = transform(raw_data)
        load(transformed_docs, mongo_uri, db_name, collection_name)
        
        print("\n MongoDB ETL process completed successfully!")
        
    except Exception as e:
        print(f"An unexpected error occurred during the ETL process: {e}")

if __name__ == "__main__":
    main()