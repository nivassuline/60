import warnings
import pandas as pd
import numpy as np
from datetime import date
import re
from elasticsearch import Elasticsearch, helpers
import os, uuid
#
warnings.simplefilter(action='ignore')
# elastic_user = "elastic"
# elastic_password = "rYPMyZFtwSyTIYn7gu4R"
#
# elastic_client = Elasticsearch("https://localhost:9200",verify_certs=False,basic_auth=(elastic_user,elastic_password))
#
# response = elastic_client.search(index='netflix_show', body={})
#

# #
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import helpers

elastic_user = "elastic"
elastic_password = "rf*k*oVGBvw+xRD=FO73"
SOURCE = 'netflix_titles_flitered.csv'
netflix_df = pd.read_csv(SOURCE).dropna()
d = {'col1': [1, 2], 'col2': [3, 4]}
new_df = pd.DataFrame(data=d)



#
# def doc_generator(df):
#     df_iter = df.iterrows()
#     for index, document in df_iter:
#         yield {
#             "_index": "netflix_shows",
#             "_source": document,
#         }
#
#
# helpers.bulk(elastic_client, doc_generator(netflix_df))
# #

# elastic_client.index(index='netflix_showss',id=1,document=netflix_df)

#
# elastic_client = Elasticsearch("https://localhost:9200/", verify_certs=False,
#                                basic_auth=(elastic_user, elastic_password))
#

#
#
# documents = netflix_df.to_dict(orient='records')
#
# helpers.bulk(elastic_client, documents, index='netflix_show', raise_on_error=True)





import requests
from requests.auth import HTTPBasicAuth
elastic_client = Elasticsearch("https://localhost:9200/", verify_certs=False,
                               basic_auth=(elastic_user, elastic_password),http_compress=True)



def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": 'netflix_shows',
                "_id" : "513",
                "_source": document.to_dict(),
            }
    raise StopIteration





# headers = {
#     'Content-Type': 'application/json',
# }
#
# resp = requests.put('https://localhost:9200/netflix_show/_settings',
#                     verify=False,
#                     auth=HTTPBasicAuth(elastic_user, elastic_password),
#                     headers=headers,
#                     data='{"index": {"mapping": {"total_fields": {"limit": "10000"}}}}')
# print(resp.content)
# print(f'\nHTTP code: {resp.status_code} -- response: {resp}\n')
# print(f'Response text\n{resp.text}')

helpers.bulk(elastic_client, doc_generator(netflix_df))
#print(netflix_df.to_dict('records'))
# helpers.streaming_bulk(raise_on_error=)

# data = elastic_client.get(index='netflix_show',id=543)
# print(data)
