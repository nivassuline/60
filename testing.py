import warnings
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import date
import re
#
warnings.simplefilter(action='ignore')
# elastic_user = "elastic"
# elastic_password = "rYPMyZFtwSyTIYn7gu4R"
#
# elastic_client = Elasticsearch("https://localhost:9200",verify_certs=False,basic_auth=(elastic_user,elastic_password))
#
# response = elastic_client.search(index='netflix_show', body={})
#
# headers = {
#     'Content-Type': 'application/json',
# }
#
# resp = requests.get('https://localhost:9200',auth=HTTPBasicAuth(elastic_user, elastic_password),verify=False)
#                     # headers=headers,
#                     # verify=False,
#                     # auth=HTTPBasicAuth(elastic_user, elastic_password),
#                     # data='localhost:9200/_cat/indices?v')
# print(resp.content)
# print(f'\nHTTP code: {resp.status_code} -- response: {resp}\n')
# print(f'Response text\n{resp.text}')
# #
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers

elastic_user = "elastic"
elastic_password = "rYPMyZFtwSyTIYn7gu4R"
SOURCE = 'netflix_titles.csv'
netflix_df = pd.read_csv(SOURCE)
d = {'col1': [1, 2], 'col2': [3, 4]}
new_df = pd.DataFrame(data=d)

elastic_client = Elasticsearch("https://localhost:9200/", verify_certs=False,
                               basic_auth=(elastic_user, elastic_password))


def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
            "_index": "netflix_shows",
            "_source": document,
        }

print(new_df)
print(helpers.bulk(elastic_client, doc_generator(new_df)))
