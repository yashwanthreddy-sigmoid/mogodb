from pymongo import MongoClient
try:
    client = MongoClient('localhost', 27017)
    print("Connected")
except:
    print("Print could not connect to MongoDB")

import json
from bson import ObjectId
db = client['sample_mflix']
mycol=db["movies"]


collection_comments = db['comments']


collection_movies = db['movies']


collection_theaters = db['theaters']

collection_users = db['users']

