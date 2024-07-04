import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list = []

    for row in data['items']:
        album_element = {}
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
    
        album_element['album_id']=album_id
        album_element['album_name']=album_name
        album_element['album_release_date']=album_release_date
        album_element['album_total_tracks']=album_total_tracks
        album_element['album_url']=album_url
    
        album_list.append(album_element)
        
    return album_list

def artist(data):
    artist_list = []
    for item in data['items']:
        for row in item['track']['album']['artists']:
            artist_element = {'artist_id':row['id'], 'artist_name':row['name'], 'external_url':row['href']}
            artist_list.append(artist_element)
    return artist_list

def songs(data):
    songs_list = []

    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        
        album_id = row['track']['album']['id']
    
        for artist in row['track']['album']['artists']:
            song_element = {'song_id':song_id, 'song_name':song_name, 'song_duration':song_duration, 'song_url':song_url, 'popularity':song_popularity,
                        'song_added':song_added, 'album_id':album_id, 'artist_id':artist['id']}
    
            songs_list.append(song_element)
    
    return songs_list



def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-bucket-vamshee'
    Key = 'raw_data/to_processed/'
    
    spotify_data = []
    spotify_keys = []
    
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        if file['Key'].split('.')[-1]=='json':
            file_key = file['Key']
            
            response = s3.get_object(Bucket=Bucket, Key = file_key)
            
            content = response['Body']
            
            data = json.loads(content.read())
            spotify_data.append(data)
            spotify_keys.append(file_key)
            
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        songs_list = songs(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        songs_df = pd.DataFrame.from_dict(songs_list)
        
        
        album_df.drop_duplicates(inplace=True)
        artist_df.drop_duplicates(inplace=True)
        songs_df.drop_duplicates(inplace=True)
        
        
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        songs_df['song_added'] = pd.to_datetime(songs_df['song_added']) 
        
        songs_key = 'transformed_data/songs_data/'+str(datetime.now())+'.csv'
        songs_buffer = StringIO()
        songs_df.to_csv(songs_buffer, index=False)
        songs_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body = songs_content)
        
        
        album_key = 'transformed_data/album_data/'+str(datetime.now())+'.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body = album_content)
        
        artist_key = 'transformed_data/artist_data/'+str(datetime.now())+'.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body = artist_content)
            
            
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket':Bucket,
            'Key':key
        }
        
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/'+ key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
            
                
