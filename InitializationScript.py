"""
InitializationScript.py initializes the databases by deleted any existing tables in the twitter database, 
creating tables and inserting the required static rows.
Unlike other scripts, this needs to be run only once at Day 0 and can be used later for re-initialization.

The following tables are created:
1. tweets: tweet_id, author_id, tweet, tweet_date, country_id
2. countries: country_id, country_name, latitude, longitude
3. tweets_staging: tweet_id, author_id, tweet, tweet_date, country_id
4. users: user_id, user_name
5. user_countries: user_id, country_id

Currently, only 'tweets' and 'countries' tables are being used. Other tables will be used in subsequent iterations of the project.
"""

import database_lib.postgre_lib as pg
import processing_lib.util as util
import os

if __name__=="__main__":
    try:
        #Connect to Postgre, drop existing tables and create new ones
        conn, cursor = pg.connect_to_postgre()
        pg.drop_tables(cursor)
        pg.create_tables(cursor)

        #Load the countries table with latitude and longitude data for countries
        path = os.getcwd()
        country_tuples = tuple(util.get_country_data(path + '\input_data\countries.csv'))
        pg.insert_records(cursor, "countries", country_tuples)
        print("Initialization successful!")

    except:
        print("Could not initialize the databases")
