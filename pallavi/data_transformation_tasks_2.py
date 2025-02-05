#import necessary libraries and functions
from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from pyspark.sql.functions import avg, min, max, desc, col, udf, collect_set, corr

from pyspark.sql.types import IntegerType, StringType, FloatType

from pyspark.sql import Window as W  


# create a spark application, read the dataset, and explore the data
spark = SparkSession.builder.appName("Final Solution of Zomato Project")\
    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar')\
    .getOrCreate()

#read the dataset (csv file) into spark dataframe
zomato_df = spark.read.csv('data/cleaned_zomato_dataset_final.csv',inferSchema=True,header=True)

zomato_df.printSchema()

zomato_df = zomato_df.withColumn('approx_cost_two_people', col('approx_cost_two_people').cast(IntegerType()))

#####################################################################################################################
# TASKS

## 1. Find the details of the restaurant that has the facility of a “book table” before.
restaurant_names_df = zomato_df.filter(col('book_table') == 'Yes').select(col('name').alias("Restaurant Name"),\
                      col('location').alias("Location"), col('phone').alias("Contact Number/s"),\
                      col('type').alias('Restaurant Type'), col('rating').alias('Rating'),\
                      col('cuisines').alias("Cuisines Available"), col('dish_liked').alias('Most Liked Dishes'),\
                      col('book_table').alias('Table Booking Facility?'))

restaurant_names_df.show(5)

#load output into csv file
restaurant_names_df.toPandas().to_csv('zomato_output_csv/1.Restaurants_with_book_table_facility.csv', index=False)

# load output into postgres database
restaurant_names_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato.task_1_restaurants_book_table_facility",
        properties={"user": "postgres", "password": ""})


#------------------------------------------------------------------------------------------------------------------------------------------------

## 2. Make a list of restaurants according to the type of restaurant and location of the restaurants.
window_spec = W.partitionBy('type', 'location')

restaurants_list = zomato_df\
                    .withColumn('restaurant list', collect_set('name').over(window_spec))\
                    .select('type', 'location', 'restaurant list')\
                    .distinct().dropna()

restaurants_list.show()

#load into csv file
restaurants_list.toPandas().to_csv('zomato_output_csv/2.Restaurants_list_acc_to_type_location.csv', index=False)

#load into database
restaurants_list\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato.task_2_restaurants_list_by_type_location",
        properties={"user": "postgres", "password": ""})


#------------------------------------------------------------------------------------------------------------------------------------------------

## 3. List the top ten restaurants with the highest rating
top_rated_restaurants = zomato_df.select(col('name').alias("Name of Restaurant"),\
                                      col('rating').alias("Rating"))\
                                      .orderBy(desc(col("rating")))\
                                      .limit(10)

top_rated_restaurants.show()

#load into csv file
top_rated_restaurants.toPandas().to_csv('zomato_output_csv/3.Top_10_rated_restaurants.csv', index=False)

# load into database
top_rated_restaurants\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato.task_3_top_10_rated_restaurants",
        properties={"user": "postgres", "password": ""})


#------------------------------------------------------------------------------------------------------------------------------------------------

## 4. Find the average, maximum, and minimum votes grouped by location
votes_summary_by_location = zomato_df.groupBy("location").agg(avg("votes").alias("average_votes"),\
                                                              max("votes").alias("maximum_votes"),\
                                                              min("votes").alias("minimum_votes")
                                                              )\
                                     .orderBy(desc("average_votes"))
votes_summary_by_location.show()

#load into csv file
votes_summary_by_location.toPandas().to_csv('zomato_output_csv/4.Avg_max_min_Votes_by_Location.csv', index=False)

#load into database
votes_summary_by_location\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato.task_4_avg_max_min_votes_by_location",
        properties={"user": "postgres", "password": ""})


#------------------------------------------------------------------------------------------------------------------------------------------------

## 5. Change rate to float type by removing ‘/5’
def convert(strr):
    if(strr == 'NEW' or strr == '-'):
        return "0"
    else:
        value = str(strr).split('/')
        value = value[0]
        return float(value)

convert_UDF = udf(lambda string : convert(string),StringType())

new_zomato_df = zomato_df.withColumn("rating", convert_UDF(col('rating')))

new_zomato_df = new_zomato_df.withColumn('rating', col('rating').cast(FloatType()))

new_zomato_df.select(col('rating')).show(10)

new_zomato_df.printSchema()

#load into csv file
new_zomato_df.toPandas().to_csv('zomato_output_csv/5.Change_rate_to_float_type_by_removing_5.csv', index=False)

#load into database
new_zomato_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato.task_5_rate_changed_to_float",
        properties={"user": "postgres", "password": ""})

#------------------------------------------------------------------------------------------------------------------------------------------------

## 6. Find the correlation between cost and rating of the restaurants.
correlation_df = new_zomato_df.select(f.corr('approx_cost_two_people', 'rating'))

correlation_df.show()

#load into csv file
correlation_df.toPandas().to_csv('zomato_output_csv/6.Correlation_between_cost_and_rating.csv', index=False)

#load into database
correlation_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql://localhost:5432/spark", "zomato.task_6_correlation_cost_rating",
        properties={"user": "postgres", "password": ""})


################################################################################################################################################

