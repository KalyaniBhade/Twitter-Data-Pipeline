"""
ExtractLoadTweets.py script performs the following operations:
- Extracts tweets from countries in the database using Twitter's 'recent search' API. Relevant methods can be found in thw twitter_lib module under processing_lib package
- Uses Spark to convert the obtained response into a Postgre friendly tuple format for easy insert into the table
- Loads the fetched tweets into the Postgre table 'tweets' using upsert method (since the API could return repeated tweets in different calls)

"""

import processing_lib.util as util
import processing_lib.spark_lib as sp
import database_lib.postgre_lib as pg
import processing_lib.twitter_lib as tw


if __name__ == "__main__":

    # Extract latest tweet data from Twitter's 'recent search' API
    twitter_response = tw.get_processed_tweets()
    print('Records fetched from Twitter API')

    # Obtain a Dataframe format of the above json response to facilitate Spark processing
    twitter_response_df = util.get_tweets_df(twitter_response)
    print('Tweet DF created')

    # twitter_response_df.drop_duplicates(['country'])

    #Instantiate PySpark
    sc, sqlContext = sp.create_spark_instance()
    print('Spark instance created')

    # Transform initial data using spark to satisfy Postgre insert format
    twitter_response_rdd = sp.df_to_rdd(twitter_response_df, sqlContext)
    tweet_tuples = sp.get_tweet_tuples(twitter_response_rdd)
    print('Tweet tuples obtained')
    sc.stop()

    # Load the transformed data into Postgre table 'tweets'
    conn, cursor = pg.connect_to_postgre()
    print('Connected to PostGre')
    pg.insert_records(cursor, "tweets", tweet_tuples)
