from loaddata import *
from insertmethods import *

# 1. Top 10 users with maximum number of comments
def ten_max_comments():
    output = collection_comments.aggregate([{"$group" :{"_id" :{"name" : "$name"}, "totalComments":{"$sum":1}}},
            {"$sort" : {"totalComments":-1}},{"$limit": 10}])

    for x in output:
        print(x)

#ten_max_comments()

#2. Find top 10 movies with most comments

def ten_movies_max_comments():
    output = collection_comments.aggregate([{"$group" : {"_id" : {"movie" : "$movie_id"} , "totalComments" : {"$sum" :1}}},
                                            {"$sort" : {"totalComments" : -1}}, {"$limit" :10}])

    for x in output:
        print(x)

#ten_movies_max_comments()

# 3. Given a year find the total number of comments created each month in that year
def total_comments_in_a_year(value):
     output = collection_comments.aggregate([{"$project":{"_id" :0, "date" :{"$toDate": {"$convert": {"input" : "$date", "to" :"long"}}}}},
                                             {"$group" :{"_id" :{"year" : {"$year":"$date"}, "month" :{"month" : "$date"}}, "totalComment" :{"$sum":1}}},
                                             {"$match": {"_id.year" :{"$eq" :value} }},
                                             {"$sort": {"_id.month":1}}])

     for x in output:
         print(x)

total_comments_in_a_year(2000)