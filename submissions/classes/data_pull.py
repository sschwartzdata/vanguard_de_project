"""
This class

Methods:
- pull_artist_albums()
- pull_artist_albums()
- remove_album_dups()
- get_tracks()
- get_track_info()
- return_artist()
- return_album()
- return_track()
- return_track_feature()
"""
# importing packages
import pandas as pd


class DataPull:
    # df_artist column names
    _artist_table_columns = ['artist_id', 'artist_name', 'external_url',
                             'genre', 'image_url', 'followers',
                             'popularity', 'type', 'artist_uri']

    # df_album column names
    _album_table_columns = ['album_id', 'album_name', 'external_url',
                            'image_url', 'release_date', 'total_tracks',
                            'type', 'album_uri', 'artist_id']

    # df_track column names
    _track_table_columns = ['track_id', 'song_name', 'external_url',
                            'duration_ms', 'explicit', 'disc_number',
                            'type', 'song_uri', 'album_id']

    # df_track_feature column names
    _track_feature_table_columns = ['track_id', 'danceability', 'energy',
                                    'instrumentalness', 'liveness', 'loudness',
                                    'speechiness', 'tempo', 'type', 'valence',
                                    'song_uri']

    def __init__(self, sp):
        # The connection to Sptify
        self.sp = sp

        # Creating the empty dataframes
        self.df_artist = pd.DataFrame(columns=self._artist_table_columns)
        self.df_album = pd.DataFrame(columns=self._album_table_columns)
        self.df_album_nodup = pd.DataFrame(columns=self._album_table_columns)
        self.df_track = pd.DataFrame(columns=self._track_table_columns)
        self.df_track_feature = pd.DataFrame(
            columns=self._track_feature_table_columns)

        # Creating empty list to be filled with ids
        self.artist_ids = []
        self.album_ids = []
        self.track_ids = []

    def artist_data_pull(self, artist_list):
        """
        Trys to pull data for each artist in the input list and updates
        ad_artist with the required data. If an exception arrises, a
        statement will be printed and the methon will continue without
        stopping.
        Parameters:
            artist_list (list): a list of artists to pull data about
        """
        for artist in artist_list:
            try:

                # Requesting results for each artist in list
                result = self.sp.search(q=artist, type="artist")

                # Selecting all data within 'items' in the pulled data
                item = result['artists']['items']

                # Creating a list of required data to enter into artist_df
                new_row = [
                    item[0]['id'],  # artist id
                    item[0]['name'],  # artist name
                    item[0]['external_urls']['spotify'],  # external url
                    item[0]['genres'][0],  # selecting the first genres
                    item[0]['images'][0]['url'],  # selection the first image
                    item[0]['followers']['total'],  # followers
                    item[0]['popularity'],  # populatrity
                    item[0]['type'],  # type
                    item[0]['uri']]  # artist uri

                # Inserting pulled data into last row of artist_df
                self.df_artist.loc[len(self.df_artist.index)] = new_row
            except Exception as e:
                print(e)

        self.artist_ids = self.artist_ids \
            + self.df_artist["artist_id"].to_list()

    # Get Spotify catalog information about an artist’s albums
    def pull_artist_albums(self):
        """
        Pulls data for all of the required album data for each artist
        and updates ad_album with the required data.
        """
        print("number of artists : " + str(len(self.artist_ids)))
        for artist_id in self.artist_ids:
            # Requesting album results for each artist_id in list
            results = self.sp.artist_albums(artist_id, album_type='album',
                                            country='US')

            # Selecting all data within 'items' in the pulled data
            items = results['items']

            # Pulling all of the albums from the artist
            for i in range(len(items)):
                new_row = [
                    items[i]['id'],  # alumb id
                    items[i]['name'],  # album name
                    items[i]['external_urls']['spotify'],  # external url
                    items[i]['images'][0]['url'],  # first image result
                    items[i]['release_date'],  # release date
                    items[i]['total_tracks'],  # number of tracks
                    items[i]['type'],  # album type
                    items[i]['uri'],  # album uri
                    artist_id]  # artist id

                # Inserting pulled data into last row of album_df
                self.df_album.loc[len(self.df_album.index)] = new_row

    # Removing redundant albums
    def remove_album_dups(self):
        """
        Removes all albums with duplicated names and those that
        contain keywords suggesting that they are a remix or similar.
        """
        # removing albums with same names
        self.df_album_nodup = self.df_album.drop_duplicates(subset=['album_name'])

        # removing redundant albums based on key words
        to_remove = ['Remix', 'remix', 'Version', 'version', 'Delux',
                     'Edition', 'Live', 'Mix', 'Tour', 'Remaster', 'Anthology']
        for words in to_remove:
            self.df_album_nodup = self.df_album_nodup[self.df_album_nodup["album_name"].str.contains(words) == False]

            removed_rows = len(self.df_album.index) - len(self.df_album_nodup.index)

        self.album_ids = self.album_ids + self.df_album_nodup["album_id"].to_list()
        print(f"A total of {removed_rows} duplicate albums have been removed.")

    # Get Spotify catalog information on tracks
    def get_tracks(self):
        """
        Pulls data for all of the required track data for each
        non-duplicated album and updates ad_track with the required data.
        """
        for album_id in self.album_ids:
            results = self.sp.album_tracks(album_id)
            # Selecting all data within 'items' in the pulled data
            item = results['items']

            for i in range(len(item)):
                new_row = [
                    item[i]['uri'],  # song uri
                    item[i]['name'],  # song name
                    item[i]['external_urls']['spotify'],
                    item[i]['duration_ms'],  # duration in ms
                    item[i]['explicit'],  # is explicit
                    item[i]['disc_number'],  # disc number
                    item[i]['type'],  # track type
                    item[i]['uri'],  # track uri
                    album_id]  # album id

                # Inserting pulled data into last row of track_df
                self.df_track.loc[len(self.df_track.index)] = new_row
        self.track_ids = self.track_ids  + self.df_track["track_id"].tolist()

    # Get Spotify catalog information on song features
    def get_track_info(self):
        """
        Pulls data for all of the required track feature data for each
        track and updates df_track_feature with the required data.
        """
        for track_id in self.track_ids:

            result = self.sp.audio_features(track_id)[0]

            try:
                new_row = [
                    track_id,  # track's id
                    result['danceability'],  # danceability
                    result['energy'],  # enegy level
                    result['instrumentalness'],  # instramental level
                    result['liveness'],  # liveness level
                    result['loudness'],  # loudness level
                    result['speechiness'],  # speah level
                    result['tempo'],  # track's tempo
                    result['type'],  # track type
                    result['valence'],  # valence
                    result['uri']]  # tracks uri

            except Exception as e:
                print("There was an issue with a track.")
                print(e)
            self.df_track_feature.loc[len(self.df_track_feature.index)] = new_row

    def return_artist(self):
        """
        Returns df.artist dataframe
        """
        return self.df_artist

    def return_album(self):
        """
        Returns df.album_nodup dataframe
        (The dataframe without duplicate albums)
        """
        return self.df_album_nodup

    def return_track(self):
        """
        Returns df.track dataframe
        """
        return self.df_track

    def return_track_feature(self):
        """
        Returns df.track_feature dataframe
        """
        return self.df_track_feature
