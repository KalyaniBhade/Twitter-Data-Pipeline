from pymongo import MongoClient
from dotenv import load_dotenv
import os


def connect_mongoDB():
    load_dotenv()
    mongo_url = os.environ.get('MONGO_URL')
    print(mongo_url)
    try:
        client = MongoClient(mongo_url)
        print("Connected to MongoDB Atlas successfully!")
        return client
    except:
        print("Could not connect to MongoDB")


def insert_data(db, data):
    try:
        db.token_counts.insert_many(data, ordered=False)
        print('Records inserted in MongoDB successfully')
    except:
        print('Could not insert records into MongoDB')


def fetch_data(db, condition={}):
    return db.token_counts.find(condition)

