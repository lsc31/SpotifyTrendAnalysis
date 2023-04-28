import json
from dotenv import dotenv_values
from pymongo import MongoClient
from spotify_client import get_data

config = dotenv_values(".env")
print(f"Read the mongo connection details from .env file")

mongodb_client = MongoClient(config["ATLAS_URI"])
print("Mongo client created")

def get_database():
    return mongodb_client[config["DB_NAME"]]


def get_collection_name(dbname):
    return dbname[config["CHART_COLLECTION"]]


def store_data(collection_name, spotifyData):
    count =0
    print(len(spotifyData['items']))
    for i in range(0, len(spotifyData['items'])):
            count+=1
            data={
                'added_at':spotifyData['items'][i]["added_at"],
                'added_by':spotifyData['items'][i]["added_by"],
                'is_local':spotifyData['items'][i]["is_local"],
                'primary_color':spotifyData['items'][i]["primary_color"],
                'track': {
                    'album':spotifyData['items'][i]["track"]["album"],
                    'album':spotifyData['items'][i]["track"]["album"],
                    'artists':spotifyData['items'][i]["track"]["artists"],
                    'available_markets':spotifyData['items'][i]["track"]["available_markets"],
                    'disc_number':spotifyData['items'][i]["track"]['disc_number'],
                    'duration_ms':spotifyData['items'][i]["track"]['duration_ms'],
                    'episode':spotifyData['items'][i]["track"]['episode'],
                    'explicit':spotifyData['items'][i]["track"]['explicit'],
                    'external_ids':spotifyData['items'][i]["track"]['external_ids'],
                    'external_urls':spotifyData['items'][i]["track"]['external_urls'],
                    'href':spotifyData['items'][i]["track"]['href'],
                    'id':spotifyData['items'][i]["track"]['id'],
                    'is_local':spotifyData['items'][i]["track"]['is_local'],
                    'name':spotifyData['items'][i]["track"]['name'],
                    'popularity':spotifyData['items'][i]["track"]['popularity'],
                    'preview_url':spotifyData['items'][i]["track"]['preview_url'],
                    'track':spotifyData['items'][i]["track"]['track'],
                    'track_number':spotifyData['items'][i]["track"]['track_number'],
                    'type':spotifyData['items'][i]["track"]['type'],
                    'uri':spotifyData['items'][i]["track"]['uri'],   
                }
            }
            document = collection_name.insert_one(data)
    print(count)

    
if __name__ == '__main__':
    data = get_data()
    tracks = data['tracks']
    dbname = get_database()
    collection_name = get_collection_name(dbname)
    store_data(collection_name, tracks)
    print(" [x] Done")