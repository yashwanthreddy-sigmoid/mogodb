from loaddata import *

def insertComments(items):
    collection_comments.insert_one(items)

def insertMovies(items):
    collection_movies.insert_one(items)

def insertTheaters(items):
    collection_theaters.insert_one(items)

def insertUsers(items):
    collection_users.insert_one(items)