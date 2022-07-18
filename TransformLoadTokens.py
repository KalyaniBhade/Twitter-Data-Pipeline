import processing_lib.util as util
import processing_lib.spark_lib as sp
import database_lib.postgre_lib as pg
import database_lib.mongoDB_lib as mdb
from datetime import datetime, timedelta

if __name__ == "__main__":
    conn, cursor = pg.connect_to_postgre()
    print('Connected to PostGre')

    current_date = str(datetime.today() - timedelta(days=1))
    condition = "tweet_date >= to_date('" + current_date + "', 'YYYY-MM-DD')"
    records = pg.get_records(
        cursor=cursor, table_name="tweets", columns="*", condition=condition)
    print('Records fetched from PostGre')

    sc, sqlContext = sp.create_spark_instance()
    print('Spark instance created')
    tuples = sp.get_token_counts_by_country(sc, records)
    token_counts_by_country = util.format_tokens_per_country(tuples)
    print('Tokens obtained for countries')
    sc.stop()

    # load words into mongo
    client = mdb.connect_mongoDB()
    db = client.TwitterAPI
    mdb.insert_data(db, token_counts_by_country)
