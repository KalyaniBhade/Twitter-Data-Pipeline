from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import database_lib.postgre_lib as pg
import pandas as pd
import re
import string

stop_words = set(stopwords.words('english'))


def augment_country(json_response, country):
    for data in json_response['data']:
        data['country'] = country
    return json_response


def get_tweets_df(response_data):
    df = pd.DataFrame(response_data)
    df = df.reindex(columns=['id', 'author_id',
                             'text', 'created_at', 'country'])
    return df


def get_date(utc_timestamp):
    '''
    date = 'to_date("' + utc_timestamp.split('T')[0] + '", "YYYY-MM-DD")'
    #print(date)
    return date
    '''
    return utc_timestamp.split('T')[0]


def process_text(text):
    processed_text = re.sub("['\\$]", "", text)
    return processed_text


def get_tokens(country, tweet):
    tokens = tweet.split(" ")
    tokens = [word.lower() for word in tokens if (
        word.lower() not in stop_words and word != country and word.isalpha())]
    return tokens


def get_date(date):
    return str(date)


def get_sorted_token_list(word_counts):
    return sorted(word_counts, key=lambda x: (-x[1], x[0]))


def read_txt_file(filename):
    file_content = []
    with open(filename, "r") as file:
        for f in file:
            file_content.append(f.strip().split(','))
    file.close()
    return file_content


def get_country_data(filename):
    country_data = read_txt_file(filename)
    tupled_data = [(c[0], c[3], c[1].strip(), c[2].strip()) for c in country_data]
    return tupled_data


def format_tokens_per_country(tuples):
    input_data = []
    for t in tuples:
        records = {}
        records['country'] = t[0][0]
        records['date'] = t[0][1]
        records['tokens'] = t[1]
        input_data.append(records)
    return input_data
