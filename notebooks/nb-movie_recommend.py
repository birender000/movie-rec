# Databricks notebook source
# DBTITLE 1,Get Data.
# getting the data.
rating_filename = "dbfs:/mnt/Files/Validated/ratings.csv"
movies_filename = "dbfs:/mnt/Files/Validated/movies.csv"




# COMMAND ----------

# DBTITLE 1,Schema creation for Data
# MAGIC %md
# MAGIC A little analysis on the movie.csv
# MAGIC We will create 2 dataframe for our analysis which will make the visualization with databricks display function pretty straightforward.
# MAGIC 1. movies_based_on_time: we will drop the genres here final schema will be(movie_id, name, year)
# MAGIC 2. movies_based_on_genres: final schema would look like (movie_id, name_with_year, one_genre)

# COMMAND ----------

from pyspark.sql.types import *

movies_with_genres_df_schema = StructType().add('ID', IntegerType()).add('title',StringType()).add('genres',StringType())

movies_df_schema = StructType().add("ID", IntegerType()).add("title",StringType())




# COMMAND ----------

# DBTITLE 1,create data frame with data and schema
movies_df = spark.read.csv(path = movies_filename, schema = movies_df_schema, header = True)

movies_with_genre_df = spark.read.csv(path = movies_filename, schema = movies_with_genres_df_schema, header = True)


# COMMAND ----------

# MAGIC %md
# MAGIC Inspecting the Dataframe before the transformation.

# COMMAND ----------

# DBTITLE 1,Show sample data
movies_df.show(4, truncate = False)
movies_with_genre_df.show(4, truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Q1: NO. OF MOVIES PRODUCED IN 2009
# MAGIC 
# MAGIC 
# MAGIC Challenge: extract the year from title.

# COMMAND ----------

# create dataframe by extracting year from title.
from pyspark.sql import functions as F

movies_with_year_df = movies_df.select('ID', 'title', F.regexp_extract('title','\((\d{4})\)',1).alias('year'))

movies_with_year_df.show(4)


# COMMAND ----------

display(movies_with_year_df.filter("year==2009").groupBy('year').count())

# COMMAND ----------

movies_year_with_count_df = movies_with_year_df.groupBy('year').count().cache()

# COMMAND ----------

display(movies_year_with_count_df.filter("year == 2009"))

# COMMAND ----------

# MAGIC %md
# MAGIC Q2: which year most movies have been produced.

# COMMAND ----------

from pyspark.sql.functions import col

display(movies_year_with_count_df.orderBy(col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets move to RATING

# COMMAND ----------

rating_df_schema = StructType().add("userId", IntegerType())\
                               .add("movieId", IntegerType())\
                               .add("rating", DoubleType())
    
    
    
rating_df = spark.read.csv(path = rating_filename, schema = rating_df_schema, header = True)

rating_df.show(4)

# COMMAND ----------

#We will cache both dataframes to access them quickly.
rating_df.cache()
movies_df.cache()

# COMMAND ----------

# Global popularity
# it is good to know the most popular movies and at times it is very hard to just beat popularyity Xavier Amartiam lecture movies with highest average rating here we will put a constraint on the no. of reviews given we will discard the movies where the count of ratings is less than 500.

from pyspark.sql import functions as F

movies_id_with_average_rating_df = rating_df.groupBy("movieId").agg(F.count("rating").alias("count"), F.avg("rating").alias("average"))

movies_id_with_average_rating_df.show(4)


# COMMAND ----------

#this df will have names with movie_id - make it more understandable.

movie_names_with_average_rating_df = movies_id_with_average_rating_df.join(movies_df,movies_id_with_average_rating_df.movieId == movies_df.ID).drop('ID').orderBy('average',ascending = False)

movie_names_with_average_rating_df.show(4)

# COMMAND ----------

# let us see global popularity

display(movie_names_with_average_rating_df.select(F.max('average').alias('highest rating')))

# COMMAND ----------


movie_names_with_average_rating_df.createOrReplaceTempView('movie_rating')
df = spark.sql("select * from movie_rating where average = (select max(average) from movie_rating)")
df.show()

# COMMAND ----------

# DBTITLE 1,Splitting in Train, Test and validation dataset
# as with all the machine learning algorithm in practice we have to tune parameters and the test accuracy for this we  will split the data into 3 parts Train, Test(checking the final accuracy) and validation (optimizing hyperparameters) data.

# we will hold 60% for training, 20% of our data for validation, and leave 20% for testing.

seed = 4

(split_60_df, split_a_20_df, split_b_20_df) = rating_df.randomSplit([0.6,0.2,0.2],seed)

# cache is important after randomSplit
training_df = split_60_df.cache()
validation_df = split_a_20_df.cache()
test_df = split_b_20_df.cache()

print('Training:{0}, validation: {1}, test: {2}\n'.format(training_df.count(), validation_df.count(), test_df.count()))

training_df.show(4)
validation_df.show(4)
test_df.show(4)


# COMMAND ----------

from pyspark.ml.recommendation import ALS
als = ALS()

#reset the parameters for als object
als.setPredictionCol("prediction")\
    .setMaxIter(5)\
    .setSeed(seed)\
    .setRegParam(0.1)\
    .setUserCol("userId")\
    .setItemCol("movieId")\
    .setRatingCol("rating")\
    .setRank(8) #we get rank 8 as optional


#create the model with these parameters
my_ratings_model = als.fit(training_df)
    

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

#create an RSME evalualtor using the label and predicted columns it will essentially calculate the rnse score based on these columns.

reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse")

my_predict_df = my_ratings_model.transform(test_df)



# remove nan values from prediction

predicted_test_my_ratings_df = my_predict_df.filter(my_predict_df.prediction != float('nan'))

# run the previously created RNSE evaluator, reg_eval, on the predicted_test_my_ratings_df dataframe

test_RMSE_my_ratings = reg_eval.evaluate(predicted_test_my_ratings_df)

print("the model had a RMSE on the test set of {0}".format(test_RMSE_my_ratings))
dbutils.widgets.text("input","5","")
ins = dbutils.widgets.get("input")
uid = int(ins)
ll = predicted_test_my_ratings_df.filter(col('userId')==uid)


# COMMAND ----------

display(ll)

# COMMAND ----------

MovieRec = ll.join(movies_df,ll.movieId == movies_df.ID).drop("ID").select("title")

import pandas as pd
MovieRec = MovieRec.toPandas().to_json(orient = "records")

l = dbutils.notebook.exit(MovieRec)
