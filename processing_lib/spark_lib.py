from pyspark import SparkContext
from pyspark.sql import SQLContext
import processing_lib.util as util
import re
import datetime
import json
import pandas as pd
import findspark
findspark.init()
findspark.find()


def create_spark_instance():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    return sc, sqlContext


def df_to_rdd(df, sqlContext):
    tweets_rdd = sqlContext.createDataFrame(df).rdd
    return tweets_rdd


def get_tweet_tuples(tweets_rdd):
    tweet_tuples = tweets_rdd.map(lambda x: (x[0], x[1], util.process_text(x[2]), util.get_date(x[3]), x[4]))\
                             .collect()
    tweet_tuples = tuple(tweet_tuples)
    return tweet_tuples


def get_token_counts_by_country(sc, records):
    rdd = sc.parallelize(records)
    country_date_word_counts_rdd = rdd.map(lambda x: ((x[4], util.get_date(x[3])), list(util.get_tokens(x[4], x[2]))))\
                                      .flatMapValues(lambda x: x)\
                                      .map(lambda x: ((x[0][0], x[0][1], x[1]), 1))\
                                      .reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] > 1)\
                                      .cache()
    sorted_word_counts_rdd = country_date_word_counts_rdd.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))\
                                                         .groupByKey()\
                                                         .mapValues(list)\
                                                         .map(lambda x: (x[0], util.get_sorted_token_list(x[1])))
    tuples = sorted_word_counts_rdd.collect()
    return tuples


def get_consolidated_tokens(sc, data):
    tokens_rdd = sc.parallelize(data)
    combined_tokens_rdd = tokens_rdd.reduceByKey(lambda x, y: x + y)\
                                    .sortBy(lambda x: -x[1]).map(lambda x: x[0])
    return combined_tokens_rdd.collect()[:10]
