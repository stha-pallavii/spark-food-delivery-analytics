## Baburam - Tasks

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Apache PySpark Final Project-Zomato-Task").getOrCreate()


cleaned_zomato = spark.read.csv('data/cleaned_zomato_df.csv',inferSchema=True,header=True)

# Task 0: convert "NEW" and "-" in rating to "0" and x/5 to x
def convert(strr):
    if(strr == 'NEW' or strr == '-'):
        return "0"
    else:
        value = str(strr).split('/')
        value = value[0]
        return float(value)


convertUDF = udf(lambda string : convert(string),StringType())


cleaned_zomato_rate = cleaned_zomato.withColumn("rating", convertUDF(col('rating')))
cleaned_zomato_rate.toPandas().to_csv('data/cleaned_zomato_rate.csv',index= False)
cleaned_zomato_rate.show()

cleaned_zomato_rating = spark.read.csv('data/cleaned_zomato_rate.csv',inferSchema=True,header=True)

cleaned_zomato_rating.show(5)




## Task 1: Find resturants which can provide Italian items.
from pyspark.sql.functions import split, col
cuisine_list = cleaned_zomato_rating\
    .select(col('name'),col('location'),col('cuisines'),split(col("cuisines"),",").alias("cuisines_list"))\
    .dropDuplicates()


italian_items=cleaned_zomato_rating.filter(col('cuisines').like("%Italian%").alias("Italian Item"))\
    .orderBy(col("votes").desc(),col("rating").desc())

italian_items.toPandas().to_csv('zomato_output_csv/7_italian_items.csv',index = False)
italian_items.show()


#load into database
italian_items\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato_baburam.italian_items",
        properties={"user": "postgres", "password": ""})



# # Task 2: Most liked restaurant in the city Banashankari
location_Banashankari=cleaned_zomato_rating\
    .filter(col("location")== "Banashankari")\
    .orderBy(col("votes").desc(),col("rating").desc())

location_Banashankari.toPandas().to_csv('data/location_Banashankari.csv',index= False)

location_Banashankari.show(10,truncate=False)

#load into database
location_Banashankari\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato_baburam.location_Banashankari",
        properties={"user": "postgres", "password": ""})



# Task 3,4: Change Yes to True and No to False in columns online_order and book_table using UDF.
def online_order_and_book_table(strr):
    if strr == "Yes":
        return True
    
    elif strr == "No":
        return False
    
    else:
        return False

convertUDF = udf(lambda string : online_order_and_book_table(string),StringType())

online_order = cleaned_zomato_rating.withColumn("online_order", convertUDF(col('online_order')))
cleaned_zomato_rating=online_order
online_order.toPandas().to_csv('data/online_order.csv',index= False)
online_order.show()

convertUDF = udf(lambda string : online_order_and_book_table(string),StringType())


book_table = cleaned_zomato_rating.withColumn("book_table", convertUDF(col('book_table')))
cleaned_zomato_rating=book_table
book_table.toPandas().to_csv('data/book_table.csv',index= False)
book_table.show()

#load into database
book_table\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato_baburam.book_table",
        properties={"user": "postgres", "password": ""})


# Task 5: List the best restaurants in each location(best in accordance with rate)
best_restaurants=cleaned_zomato_rating.groupBy("name","location")
best_restaurants_location = best_restaurants.max()
best_restaurants_location.show()

#load into database
best_restaurants_location\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato_baburam.best_restaurants_location",
        properties={"user": "postgres", "password": ""})


# Task 6:Find the total no. of voters in each location (window function)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,udf,col,sum
windowSpec = Window.partitionBy('location')

total_votes_by_city = cleaned_zomato_rating.withColumn('total_votes',sum(col('votes')).over(windowSpec))\
                                .select(col('location'),col('total_votes')).dropDuplicates()

total_votes_by_city.toPandas().to_csv('data/total_votes_by_location.csv',index = False)

total_votes_by_city.show()

#load into database
total_votes_by_city\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato_baburam.total_votes_by_city",
        properties={"user": "postgres", "password": ""})

#Task 7: Find the list of restaurant based on the location and and rating
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("location").orderBy(col("rating").desc())
list_restaurants=cleaned_zomato_rating.withColumn("row_number",row_number().over(windowSpec))
list_restaurants.toPandas().to_csv('data/list_restaurants.csv',index= False)
list_restaurants.show(10)

#load into database
list_restaurants\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato_baburam.list_restaurants",
        properties={"user": "postgres", "password": ""})


