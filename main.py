import sqlite3
import pandas as pd
import time
import argparse
import os

# ArgumentParser section
parser = argparse.ArgumentParser()
parser.add_argument('database_directory',
                    metavar='database_directory',
                    type=str,
                    help='Database directory')

parser.add_argument('unique_tracks_directory',
                    metavar='unique_tracks_directory',
                    type=str,
                    help='Unique tracks directory')

parser.add_argument('triplets_sample_directory',
                    metavar='triplets_sample_directory',
                    type=str,
                    help='Triplets sample directory')

parser.add_argument('limit_of_plays',
                    metavar='limit_of_plays',
                    type=int,
                    help='Limit of plays in triplets_sample.txt')

args = parser.parse_args()

database_directory = args.database_directory
unique_tracks_directory = args.unique_tracks_directory
triplets_sample_directory = args.triplets_sample_directory

# Create database
conn = sqlite3.connect(database_directory)
cursor = conn.cursor()

# Create table (if not exist) with tracks from unique_tracks file
cursor.execute('''CREATE TABLE IF NOT EXISTS tracks( 
                    execution_id TEXT, 
                    song_id TEXT, 
                    artist TEXT, 
                    title TEXT)''')

# Create table (if not exist) with number of plays from triplets_sample_20p file
cursor.execute('''CREATE TABLE IF NOT EXISTS triplets(
                    user_id TEXT, 
                    song_id TEXT, 
                    listened_at INTEGER,
                    FOREIGN KEY (song_id) REFERENCES tracks(song_id))''')


def process_data(limit):
    '''
        Section below: loop through unique_tracks file to replace <SEP> (character of separator is > 1)
        First step: loop through list file and save all file lines in 'data' list
        Second step: loop through the list and replace <SEP> string with special character
        Third step: loop through new created auxiliary file and write lines from 'data' list
    '''
    data = []
    with open(unique_tracks_directory, 'r', encoding='ISO-8859-1') as file:
        data = file.readlines()

    for index, item in enumerate(data):
        data[index] = item.replace('<SEP>', '¤')

    with open('unique_tracks_new.txt', 'w', encoding='ISO-8859-1') as txt_file:
        txt_file.writelines(data)

    '''
        Section below: loop through triplets_sample file to replace <SEP> (character of separator is > 1)
        First step: loop through list file and save all file lines in 'data' list
        Second step: loop through the list and replace <SEP> string with special character
        Third step: loop through new created auxiliary file and write lines from 'data' list
    '''
    data = []
    with open(triplets_sample_directory, 'r', encoding='ISO-8859-1') as file:
        for i, line in enumerate(file):
            if i >= limit and limit != 0:
                break
            data.append(line)

    for index, item in enumerate(data):
        data[index] = item.replace('<SEP>', '¤')

    with open('triplets_sample_new.txt', 'w', encoding='ISO-8859-1') as txt_file:
        txt_file.writelines(data)

    # Create pandas DataFrame from text file
    unique_tracks_ = pd.read_csv('unique_tracks_new.txt', sep='¤', header=None,
                                 names=['execution_id', 'song_id', 'artist', 'title'],
                                 encoding='ISO-8859-1', engine='python'
                                 )

    # Create pandas DataFrame from text file
    triplets_sample_ = pd.read_csv('triplets_sample_20p_new.txt', sep='¤', header=None,
                                   names=['user_id', 'song_id', 'listened_at'],
                                   encoding='ISO-8859-1', engine='python'
                                   )

    table_name_tracks = 'tracks'
    table_name_triplets = 'triplets'

    # Put data into the database
    unique_tracks_.to_sql(name=table_name_tracks, con=conn, if_exists='replace', index=False)
    triplets_sample_.to_sql(name=table_name_triplets, con=conn, if_exists='replace', index=False)

    # Commit the current transaction in database
    conn.commit()

    # Create SQL query
    query_artist = cursor.execute('''SELECT artist, COUNT(triplets.song_id) AS listens FROM tracks
                                    JOIN triplets ON tracks.song_id = triplets.song_id
                                    GROUP BY artist
                                    ORDER BY listens DESC
                                    LIMIT 1''')

    result_artist = query_artist.fetchall()
    print(f'Artist with most plays:\n{result_artist}')

    # Create SQL query
    query_songs = cursor.execute('''SELECT title, COUNT(triplets.song_id) AS listens FROM tracks
                                        JOIN triplets ON tracks.song_id = triplets.song_id
                                        GROUP BY title
                                        ORDER BY listens DESC
                                        LIMIT 5''')

    result_songs = query_songs.fetchall()
    print('5 most popular songs:')
    for row in result_songs:
        print(row)

    # Close the connection
    conn.close()

    # Remove auxiliary files
    os.remove('unique_tracks_new.txt')
    os.remove('triplets_sample_new.txt')


def main():
    limit_of_plays = args.limit_of_plays
    start_time = time.time()
    process_data(limit_of_plays)
    end_time = time.time()
    processing_time = end_time - start_time
    print(f'Data processing time: {processing_time}')


if __name__ == '__main__':
    main()
