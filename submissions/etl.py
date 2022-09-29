"""
IMPORTANT!
Make sure you have updates your computer enviromentals
to contains your client id, secret and redirect URI
for Spotify set up.

Builds ETL pipeline for Spotify SQLite database.
Pulls data from Spotify's API and saves relavent
data in dataframes. Data is then tested and
transformed in preperation fo the database.
Connects to the Spotify database and
populates the artist, album, track, and
track_features relations with data contained
in the cleaned dataframes.
Views are created in spotify.db.
"""

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from helper_functions import load
from helper_functions import sql_queries
from helper_functions import transformation
from helper_functions import db_views
from classes.data_pull import DataPull
from classes.db_connection import DatabaseGen
from tests import null_duplicate



def main():
    """
    Builds ETL pipeline for Spotify SQLite database.
    Pulls data from Spotify's API and saves relavent
    data in dataframes. Data is then tested and
    transformed in preperation fo the database.
    Connects to the Spotify database and
    populates the artist, album, track, and
    track_features relations with data contained
    in the cleaned dataframes.
    Views are created in spotify.db.
    """
    # SET UP
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)
    database_path = r"db_files/spotify.db"

    search_artists = ["Wild Child", "Tessa Violet", "Halsey", "OK Go",
                      "Regina Spektor", "Modest Mouse", "The Killers",
                      "the GazettE", "Dolly Parton", "Ed Sheeran",
                      "The Vaccines", "Bloc Party", "Simple Plan",
                      "Creedence Clearwater Revival", "Miike Snow",
                      "Adele", "Grace VanderWaal", "Fall Out Boy",
                      "blink-182", "Taylor Swift"]

    # EXTRACTION AND TRANSFORMATION
    # Creates data pull object
    spotify_data_pull = DataPull(sp)
    print("Artist Dataframe was populated.")

    # Pull artist data and populates artist dataframe
    spotify_data_pull.artist_data_pull(search_artists)

    # Pull album data and populates album dataframe
    spotify_data_pull.pull_artist_albums()
    print("Album Dataframe was populated.")

    # Removes duplicates and populates cleaned album dataframe
    spotify_data_pull.remove_album_dups()
    print("Cleaned Album Dataframe was populated.")

    # Pull track data and populates track dataframe
    spotify_data_pull.get_tracks()
    print("Track Dataframe was populated.")

    # Pull track feature data and populates track feature dataframe
    spotify_data_pull.get_track_info()
    print("Track Feature Dataframe was populated.")

    # Dataframes
    # Returning the final dataframes to etl.py
    df_artist = spotify_data_pull.return_artist()
    df_album_nodup = spotify_data_pull.return_album()
    df_track = spotify_data_pull.return_track()
    df_track_feature_nodup = spotify_data_pull.return_track_feature()

    # list of dataframe and db table name pairs
    df_list = [(df_artist, 'artist'), (df_album_nodup, 'album'),
               (df_track, 'track'),
               (df_track_feature_nodup, 'track_feature')]

    # Data Type Transformation
    transformation.data_type(df_artist)

    # NULL AND DUPLICATE TEST
    print("cheaking for NULL values:")
    null_duplicate.null_count(df_list)

    print("cheaking for duplicate values:")
    null_duplicate.duplicate_count(df_list)

    # CREATING DATABASE
    # creating database generator object
    db_con = DatabaseGen(database_path)

    # Creating connection to the database
    con = db_con.create_connection()

    # Dropping tables if they exist
    db_con.drop_tables(con, sql_queries.drop_table_queries)

    # Creating all four tables
    db_con.create_tables(con, sql_queries.create_table_queries)

    # Committing updates
    db_con.commit(con)

    # LOADING DATA TO DATABASE

    # loading dataframes into database
    load.load_tables(con, df_list)
    print("The data has been loaded into the database.")

    # CREATING VIEWS IN DATABASE
    # Creates 5 different view in the database
    db_views.create_views(con, sql_queries.create_view_queries)
    print("Views have been created in the database.")

    # Committing updates
    db_con.commit(con)
    print("ELT is complete!")


if __name__ == "__main__":
    main()
