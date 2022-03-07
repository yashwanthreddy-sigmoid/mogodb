from loaddata import *
from insertmethods import *
from pprint import pprint

pprint(collection_movies.find_one())

#1. Top n movies with highest imdb rating

def highest_imdb(value):
    output = collection_movies.aggregate([{"$project":{"_id":0, "title":1, "imdb.rating":1}},
                                          {"$sort":{"imdb.rating":-1}},
                                          {"$limit":value}])

    for x in output:
        print(x)

#highest_imdb(5)

#2. Top n movies with the highest IMDB rating in a given year
def highest_imdb_in_a_year(value,year1):
    output = collection_movies.aggregate([

        {"$match":{"year":{"$eq":year1}}},
        {"$project":{"_id":1,"title":1,"year":1,"imdb.rating":1}},
        {"$sort":{"imdb.rating":-1}},
        {"$limit":value}
    ])

    for x in output:
        print(x)

#highest_imdb_in_a_year(5,2000)

#3. Top n movies with highest IMDB rating with number of votes > 1000
def highest_rated_movies_having_votes_gt_thousand (n):

   movie = collection_movies.aggregate(
       [{"$addFields": {"vote": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$imdb.votes"}}}},
       {"$match": {"$expr": {"$gt": [{"$toInt": "$vote"}, 1000]}}},
       {"$sort": {"imdb.rating": -1}},
       {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "vote": 1}},
       {"$limit": n}])

   for x in movie:
        print(x)

#highest_rated_movies_having_votes_gt_thousand(5)

#4. Top n movies with title matching a given pattern sorted by highest tomatoes ratings
def movies_with_matching_title(n, string_match):
   pipeline = [{"$addFields": {"tomatoes_Rating": "$tomatoes.viewer.rating", "result": {"$cond": {"if":
               {"$regexMatch": {"input": "$title","regex": string_match} },"then": "yes","else": "no"}}}},
                {"$project": {"_id": 0, "title": 1, "tomatoes_Rating": 1, "result": 1}},
                {"$match": {"result": {"$eq": "yes"}}},
               {"$sort": {"tomatoes_Rating": -1}},
                {"$limit": n}]
   movie = list(collection_movies.aggregate(pipeline))

   for x in movie:
       print(x)

#movies_with_matching_title(5, "scape")

#5. Find top N Directors who created the maximum number of movies
def top_n_director_who_create_max_Movies(n):
   movie = collection_movies.aggregate([{"$unwind": "$directors"},
                           {"$group": {"_id": {"dir_name":"$directors"}, "Movie_count": {"$sum": 1}}},
                           {"$project": {"dir_name": 1, "Movie_count": 1}},
                           {"$sort": {"Movie_count": -1}},
                           {"$limit": n} ])

   for x in movie:
       print(x)

#top_n_director_who_create_max_Movies(5)

#6. Find top N directors who created the maximum number of movies in a given year
def top_n_director_max_movie_in_year(n, year):
   movie = collection_movies.aggregate(
       [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}}}},
        {"$unwind": "$directors"}, {"$match": {"yr": {"$eq": year}}},
        {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
        {"$project": {"director_name": 1, "count": 1}},
        {"$sort": {"count": -1}},
       {"$limit": n}])
   for x in movie:
       print(x)

#top_n_director_max_movie_in_year(7, "1983")

#7 Find top N director with max no. of movie with a given generes
def top_n_directors_with_highest_movie_given_genre(n, genres):
   movie = collection_movies.aggregate(
       [{"$unwind": "$directors"},
       {"$match": {"genres" : {"$eq": genres}}},
        {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
        {"$project": {"director_name": 1, "count": 1}},
        {"$sort": {"count": -1}},
       {"$limit": n}])
   for x in movie:
       print(x)

#top_n_directors_with_highest_movie_given_genre(5, "Short")

#8.Find top N actors who starred in the maximum number of movies
def top_n_actor_starred_in_max_Movies(n):
   movie= collection_movies.aggregate(
       [{"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$project": {"cast": 1, "count": 1}},
        {"$sort": {"count": -1}},
       {"$limit": n}, ])
   for x in movie:
       print(x)

#top_n_actor_starred_in_max_Movies (5)

#9. Find top N actors who starred in the maximum number of movies in a given year
def top_n_actor_max_movie_in_year(n, year):
   movie = collection_movies.aggregate(
       [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}}}},
        {"$unwind": "$cast"},
        {"$match": {"yr": {"$eq": year}}},
        {"$group": {"_id": {"actor_name": "$cast"}, "count": {"$sum": 1}}},
        {"$project": {"actor_name": 1, "count": 1}},
        {"$sort": {"count": -1}},
       {"$limit": n}])
   for x in movie:
       print(x)

#top_n_actor_max_movie_in_year(5, "2000")

#10. Find top N actors who starred in the maximum number of movies for a given genre
def top_n_actor_with_highest_movie_with_given_genre(n, genres):
   movie = collection_movies.aggregate(
       [{"$unwind": "$cast"},
        {"$match": {"genres" : {"$eq": genres}}},
        {"$group": {"_id": {"actor_name": "$directors"}, "count": {"$sum": 1}}},
        {"$project": {"actor_name": 1, "count": 1}},
        {"$sort": {"count": -1}},
       {"$limit": n}])
   for x in movie:
       print(x)

#top_n_actor_with_highest_movie_with_given_genre(5, "Comedy")

#11 Find top N movies for each genre with the highest IMDB rating
def Top_n_movie_for_each_genre(n):
   output = collection_movies.aggregate([{"$unwind": "$genres"}, {"$sort": {"imdb.rating": -1}},
   {"$group":{"_id": "$genres", "title": {"$push": "$title"},
               "rating":{"$push": {"$getField": {"field" : {"$literal": "$numberDouble"},"input": "$imdb.rating"}}}}},
   {"$project": {"_id": 1, "Movies": {"$slice": ['$title', 0, n]},
                   "ratings": {"$slice": ["$rating", 0, n]}}}])
   for i in output:
       print(i)

#Top_n_movie_for_each_genre(2)