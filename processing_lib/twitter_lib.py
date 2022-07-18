
import database_lib.postgre_lib as pg
import processing_lib.util as util
import os
import requests

# Get information about the environment variable for API bearer token
def auth():
    return os.getenv('BEARER_TOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, max_results):

    # Change to the endpoint you want to collect data from
    search_url = "https://api.twitter.com/2/tweets/search/recent"

    # Change params based on the endpoint being used
    query_params = {'query': keyword,
                    'max_results': max_results,
                    'tweet.fields': 'created_at',
                    'expansions': 'author_id'
                    }
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token=None):
    # params object received from create_url function
    params['next_token'] = next_token
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_recent_tweets(keyword):
    bearer_token = auth()
    headers = create_headers(bearer_token)
    max_results = 10
    url = create_url(keyword, max_results)
    json_response = connect_to_endpoint(url[0], headers, url[1])
    return json_response


def get_processed_tweets():
    response_data = []
    operators = "has:hashtags -is:retweet lang:en"
    conn, cursor = pg.connect_to_postgre()
    countries = pg.get_records(
        cursor=cursor, table_name="countries", columns="country_name")
    n = len(countries)
    for i in range(40):
        country = countries[i % n][0]
        keyword = country + " " + operators
        json_response = get_recent_tweets(keyword)
        json_response = util.augment_country(json_response, country)
        response_data.extend(json_response['data'])
    return response_data
