from faker import Faker
import random
import csv
from typing import Any, Generator

def _calculate_popularity(total_streams: int) -> float:
    """Calculates a popularity rating based on stream count with some randomness."""
    rating = round((total_streams / 10_000_000) + random.uniform(-1, 5), 2)
    if rating > 10.0:
        return 10.0
    if rating < 1.0:
        return round(random.uniform(1.0, 3.0), 2)
    return rating

def _generate_artists(num_artists: int, fake: Faker) -> list[dict[str, Any]]:
    """Generates a list of unique artists."""
    return [{
        'artist_id': fake.uuid4(),
        'artist_name': fake.name(),
    } for _ in range(num_artists)]

def _generate_albums(artists: list[dict[str, Any]], fake: Faker) -> list[dict[str, Any]]:
    """Generates a pool of albums for the given artists."""
    albums = []
    for artist in artists:
        for _ in range(random.randint(2, 10)):
            albums.append({
                'album_id': fake.uuid4(),
                'album_name': fake.sentence(nb_words=3).replace('.', ''),
                'artist_id': artist['artist_id'],
                'artist_name': artist['artist_name'],
                'release_date': fake.date_between(start_date='-10y', end_date='today'),
                'num_tracks': random.randint(4, 20)
            })
    return albums

def generate_music_data(num_records: int, num_artists: int) -> Generator[dict[str, Any], None, None]:
    """
    Generates dictionaries containing realistic-looking music data.

    Args:
        num_records (int): The number of music records to generate.
        num_artists (int): The number of unique artists to generate.

    Yields:
        A dictionary representing a track.
    """
    fake = Faker()
    records_yielded = 0

    # A list of common music genres
    genres = [
        "Pop", "Rock", "Hip Hop", "R&B", "Electronic", "Country", "Jazz",
        "Classical", "Indie", "Folk", "Reggae", "Metal"
    ]

    # Generate a pool of unique artists
    artists = _generate_artists(num_artists, fake)

    # Generate albums for each artist, with a random number of tracks
    albums = _generate_albums(artists, fake)

    # Generate individual tracks and assign them to albums
    for album in albums:
        for _ in range(album['num_tracks']):
            if records_yielded >= num_records:
                return
            
            total_streams = random.randint(50_000, 1_000_000_000)
            yield {
                'track_id': fake.uuid4(),
                'track_title': fake.sentence(nb_words=4).replace('.', ''),
                'artist_id': album['artist_id'],
                'artist_name': album['artist_name'],
                'album_id': album['album_id'],
                'album_name': album['album_name'],
                'is_single': False, # Albums are not singles
                'genre': random.choice(genres),
                'release_date': album['release_date'].strftime('%Y-%m-%d'),
                'duration_seconds': random.randint(120, 600),
                'popularity_rating': _calculate_popularity(total_streams),
                'total_streams': total_streams,
                'is_explicit': fake.boolean(chance_of_getting_true=20)
            }
            records_yielded += 1



    # Generate singles to fill in gap to target number of tracks.
    while records_yielded < num_records:
        artist = random.choice(artists)
        total_streams = random.randint(50_000, 1_000_000_000)
           
        yield {
            'track_id': fake.uuid4(),
            'track_title': fake.sentence(nb_words=4).replace('.', ''),
            'artist_id': artist['artist_id'],
            'artist_name': artist['artist_name'],
            'album_id': 'none',
            'album_name': 'Single',
            'is_single': True,
            'genre': random.choice(genres),
            'release_date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            'duration_seconds': random.randint(120, 300),
            'popularity_rating': _calculate_popularity(total_streams),
            'total_streams': total_streams,
            'is_explicit': fake.boolean(chance_of_getting_true=20)
        }
        records_yielded += 1


if __name__ == '__main__':
    records = generate_music_data(100, 1)

    # Define the output file and fields
    csv_file = 'music_data.csv'

    # Write the data to a CSV file
    try:
        first_record = next(records)
    except StopIteration:
        print("No records were generated.")
    else:
        fieldnames = first_record.keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header
            writer.writeheader()
            writer.writerow(first_record)

            # Write all the rows at once
            writer.writerows(records)

    print(f"Successfully generated and saved records to {csv_file}")