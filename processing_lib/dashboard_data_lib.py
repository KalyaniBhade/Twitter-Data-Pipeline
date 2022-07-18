from datetime import date, timedelta
from pyspark import SparkContext
from collections import defaultdict
import database_lib.mongoDB_lib as mdb
import processing_lib.spark_lib as sp
import database_lib.postgre_lib as pg
import pandas as pd


def fetch_recent_tokens(n):

    today = date.today()
    dates = []
    for i in range(n):
        condition = {"date": str(today - timedelta(days=i))}
        dates.append(condition)

    client = mdb.connect_mongoDB()
    db = client.TwitterAPI
    all_conditions = {"$or": dates}
    data = mdb.fetch_data(db, all_conditions)
    return list(data)


def format_tokens(tokens):
    token_string = "{"
    for token in tokens[:-1]:
        token_string += token + ", "
    return token_string + tokens[-1] + "}"


def get_final_dataframe(n):

    consolidated_dict = defaultdict(list)
    data = fetch_recent_tokens(n)
    for d in data:
        consolidated_dict[d['country']].extend(d['tokens'])

    sc, sqlContext = sp.create_spark_instance()
    for key, value in consolidated_dict.items():
        #print('Spark processing for ' + key)
        consolidated_dict[key] = sp.get_consolidated_tokens(sc, value)

    dataframe_list = []
    conn, cursor = pg.connect_to_postgre()
    for key, value in consolidated_dict.items():
        row = []
        row.append(str(key))
        data = pg.get_records(cursor=cursor, table_name='countries',
                              columns='latitude, longitude', condition="country_name = '" + key + "'")
        print(data)
        row.append(float(data[0][0]))
        row.append(float(data[0][1]))
        row.append(format_tokens(value))
        dataframe_list.append(row)

    df = pd.DataFrame(dataframe_list, columns=[
                      'Country', 'Latitude', 'Longitude', 'Tokens'])
    sc.stop()
    return df
