
from dotenv import load_dotenv
import psycopg2
import os


def get_postgre_params():
    load_dotenv()
    database = os.environ.get('PG_DATABASE')
    username = os.environ.get('PG_USERNAME')
    password = os.environ.get('PG_PASSWORD')
    host = os.environ.get('PG_HOST')
    port = os.environ.get('PG_PORT')
    return (database, username, password, host, port)


def connect():
    database, username, password, host, port = get_postgre_params()
    conn = psycopg2.connect(database=database, user=username,
                            password=password, host=host, port=port)
    return conn


def drop_tables(cursor):
    delete_commands = ["""DROP TABLE IF EXISTS tweets""",
                       """DROP TABLE IF EXISTS tweets_staging""",
                       """DROP TABLE IF EXISTS countries""",
                       """DROP TABLE IF EXISTS users""",
                       """DROP TABLE IF EXISTS user_countries"""]

    for command in delete_commands:
        cursor.execute(command)
    print("Tables dropped successfully")


def create_tables(cursor):

    create_commands = [
        """
                CREATE TABLE tweets_staging
                (tweet_id VARCHAR(25) NOT NULL PRIMARY KEY,
                 author_id VARCHAR(50) NOT NULL,
                 tweet TEXT NOT NULL,
                 tweet_date DATE,
                 country_id VARCHAR(25));
                """,
        """
                CREATE TABLE tweets
                (tweet_id VARCHAR(25) NOT NULL,
                 author_id VARCHAR(50) NOT NULL,
                 tweet TEXT NOT NULL,
                 tweet_date DATE,
                 country_id VARCHAR(25));
                """,
        """
                CREATE TABLE countries
                (country_id VARCHAR(2) PRIMARY KEY,
                 country_name VARCHAR(50) NOT NULL,
                 latitude decimal,
                 longitude decimal);
                """,
        """
                CREATE TABLE users
                (user_id VARCHAR(50) NOT NULL PRIMARY KEY,
                 user_name VARCHAR(50));
                """,
        """
                CREATE TABLE user_countries
                (user_id VARCHAR(50) NOT NULL,
                 country_id VARCHAR(50) NOT NULL);
                """]
    for command in create_commands:
        cursor.execute(command)
    print("Tables created successfully")
    # cursor.close()


def insert_records(cursor, table_name, records):
    for record in records:
        query = "INSERT INTO {} VALUES {};".format(table_name, record)
        cursor.execute(query)
    print("Records inserted successfully")


def merge_records(cursor):
    query = """
            INSERT INTO tweets t
            VALUES (select * from tweets_staging) s
            ON CONFLICT (t.tweet_id) DO
            UPDATE SET t.tweet_date = s.tweet_date
            """
    cursor.execute(query)
    print("Merged successfully")


def get_records(cursor, table_name, columns="*", condition="1=1"):
    query = "SELECT {} FROM {} WHERE {}".format(columns, table_name, condition)
    cursor.execute(query)
    records = cursor.fetchall()
    return records


def connect_to_postgre():
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()
    return (conn, cur)


def get_spark_sql_connector(spark, tablename):
    database, username, password, host, port = get_postgre_params()
    sqldf = spark.read.format("jdbc").options(
        url=host,
        dbtable=tablename,
        user=username,
        password=password,
        driver='org.postgresql.Driver').load()
    return sqldf
