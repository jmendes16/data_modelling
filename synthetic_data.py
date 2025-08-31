from faker import Faker
import random
import csv

def generate_music_data(num_records)->list:
    """
    Generates a list of dictionaries containing realistic-looking music data.

    Args:
        num_records (int): The number of music records to generate.

    Returns:
        list: A list of dictionaries, where each dictionary represents a track.
    """
    fake = Faker()
    music_data = []

    # A list of common music genres
    genres = [
        "Pop", "Rock", "Hip Hop", "R&B", "Electronic", "Country", "Jazz",
        "Classical", "Indie", "Folk", "Reggae", "Metal"
    ]

    for i in range(num_records):
        # Determine if the track is a single
        is_single = random.choice([True, False, False, False]) # More likely to be part of an album

        # Generate a random release date in the last 10 years
        release_date = fake.date_between(start_date='-10y', end_date='today')

        # Generate total streams and a popularity rating
        # The rating is loosely tied to streams but has some randomness
        total_streams = random.randint(50000, 1000000000)
        popularity_rating = round((total_streams / 10000000) + random.uniform(-1, 5), 2)
        if popularity_rating > 10.0:
            popularity_rating = 10.0
        elif popularity_rating < 1.0:
            popularity_rating = random.uniform(1.0, 3.0)

        # Generate artist and album names. An artist might have a distinct style.
        artist = fake.name()
        if is_single:
            album_name = 'Single'
        else:
            album_name = fake.sentence(nb_words=3).replace('.', '')

        # Generate the track data dictionary
        track_data = {
            'track_id': fake.uuid4(),
            'track_title': fake.sentence(nb_words=4).replace('.', ''),
            'artist': artist,
            'album': album_name,
            'is_single': is_single,
            'genre': random.choice(genres),
            'release_date': release_date.strftime('%Y-%m-%d'),
            'duration_seconds': random.randint(120, 600),
            'popularity_rating': popularity_rating,
            'total_streams': total_streams,
            'is_explicit': fake.boolean(chance_of_getting_true=20)
        }
        music_data.append(track_data)

    return music_data


if __name__ == '__main__':
    records = generate_music_data(1000000)

    # 2. Define the output file and fields
    csv_file = 'music_data.csv'
    fieldnames = list(records[0].keys())

    # 3. Write the data to a CSV file
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write all the rows at once
        writer.writerows(records)

    print(f"Successfully generated and saved {len(records)} records to {csv_file}")