import pandas as pd
import psycopg
import uuid
import io
import os
from dotenv import load_dotenv

# --- Configuration ---
CSV_FILE_PATH = "music_data.csv"

def create_tables(conn):
    """Creates the normalized tables in the database if they don't exist."""
    
    sql_commands = """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id UUID PRIMARY KEY,
            artist_name VARCHAR(255) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS albums (
            album_id UUID PRIMARY KEY,
            album_name VARCHAR(255) NOT NULL,
            release_date DATE,
            artist_id UUID REFERENCES artists(artist_id)
        );
        CREATE TABLE IF NOT EXISTS tracks (
            track_id UUID PRIMARY KEY,
            track_title VARCHAR(255) NOT NULL,
            duration_seconds INTEGER,
            is_explicit BOOLEAN,
            genre VARCHAR(100),
            popularity_rating REAL,
            total_streams BIGINT,
            album_id UUID REFERENCES albums(album_id),
            artist_id UUID REFERENCES artists(artist_id) -- could be omitted
        );
    """
    with conn.cursor() as cur:
        print("Creating tables if they don't exist...")
        cur.execute(sql_commands)
        conn.commit()
    print("Tables created successfully.")

# --- ETL Functions ---

def extract(filepath: str) -> pd.DataFrame:
    """
    Extracts data from a CSV file into a pandas DataFrame.
    
    Args:
        filepath: The path to the CSV file.
    
    Returns:
        A pandas DataFrame containing the raw data.
    """
    print(f"EXTRACT: Reading data from {filepath}...")
    try:
        return pd.read_csv(
            filepath,
            na_values=['none'],
            keep_default_na=False # due to an error caused by some albums titled 'None'
            )
    except FileNotFoundError:
        print(f"ERROR: The file '{filepath}' was not found.")
        raise

def transform(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Transforms the raw data by normalizing singles albums and separating
    the data into distinct DataFrames for each database table.
    
    Args:
        df: The raw pandas DataFrame.
        
    Returns:
        A dictionary of transformed DataFrames, keyed by table name.
    """
    print("TRANSFORM: Normalizing singles and structuring data...")
    
    # Normalise the "Singles Album" for each artist
    artists_with_singles = df[df['is_single'] == True]['artist_id'].unique()
    artist_singles_album_map = {artist_id: uuid.uuid4() for artist_id in artists_with_singles}
  
    df['album_id'] = df.apply(
        lambda row: artist_singles_album_map.get(row['artist_id'], row['album_id']) if row['is_single'] else row['album_id'],
        axis=1
    )

    # Prepare data for normalized tables
    artists_df = df[['artist_id', 'artist_name']].drop_duplicates().set_index('artist_id')

    singles_albums_data = [
        {
            'album_id': album_id, 
            'album_name': 'Singles', 
            'release_date': None,
            'artist_id': artist_id
        } for artist_id, album_id in artist_singles_album_map.items()
    ]
    singles_albums_df = pd.DataFrame(singles_albums_data)
    albums_df = df[df['is_single'] == False][['album_id', 'album_name', 'release_date', 'artist_id']]
    all_albums_df = pd.concat([albums_df, singles_albums_df]).drop_duplicates(subset=['album_id']).set_index('album_id')
    
    tracks_df = df[[
        'track_id', 'track_title', 'duration_seconds', 'is_explicit', 'genre',
        'popularity_rating', 'total_streams', 'album_id', 'artist_id'
    ]].set_index('track_id')
    
    print("TRANSFORM: Data transformation complete.")
    return {
        "artists": artists_df,
        "albums": all_albums_df,
        "tracks": tracks_df,
    }

def load(dataframes: dict[str, pd.DataFrame], conn):
    """
    Loads the transformed DataFrames into their respective PostgreSQL tables
    using a high-speed COPY operation.
    
    Args:
        dataframes: A dictionary of DataFrames keyed by table name.
        conn: An active psycopg database connection.
    """
    print("LOAD: Starting data insertion into PostgreSQL...")
    
    # Define the correct insertion order to respect foreign key constraints
    insertion_order = ["artists", "albums", "tracks"]

    with conn.cursor() as cur:
        for table_name in insertion_order:
            df = dataframes[table_name]
            if df.empty:
                print(f"LOAD: No data to insert for '{table_name}'. Skipping.")
                continue

            print(f"LOAD: Bulk inserting {len(df)} records into '{table_name}'...")
            buffer = io.StringIO()
            df.to_csv(buffer, header=False)
            buffer.seek(0)
            
            with cur.copy(f"COPY {table_name} FROM STDIN WITH CSV") as copy:
                copy.write(buffer.read())
                
    conn.commit()
    print("LOAD: Data loading complete.")

def main():
    """Main function to orchestrate the ETL process."""
    load_dotenv() # Loads variables from .env file into environment
    
    try:
        conn_string = "dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}".format_map(os.environ)
    except KeyError as e:
        print(f"ERROR: Environment variable {e} not set. Please check your .env file.")
        return

    try:
        with psycopg.connect(conn_string) as conn:
            print("Successfully connected to the database.")
            create_tables(conn)
            
            # Run the ETL pipeline
            raw_data = extract(CSV_FILE_PATH)
            transformed_data = transform(raw_data)
            load(transformed_data, conn)
            
            print("\nETL process completed successfully!")
            
    except psycopg.OperationalError as e:
        print(f"Could not connect to the database. Please check your connection settings in the .env file.")
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()