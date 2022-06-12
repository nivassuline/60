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
elastic_user = "elastic"
elastic_password = "rYPMyZFtwSyTIYn7gu4R"

# elastic_client = Elasticsearch("https://localhost:9200",verify_certs=False,basic_auth=(elastic_user,elastic_password))
#
# response = elastic_client.search(index='netflix_show', body={})
#
headers = {
    'Content-Type': 'application/json',
}

resp = requests.put('https://localhost:9200/netflix_shows/_settings',
                    verify=False,
                    auth=HTTPBasicAuth(elastic_user, elastic_password),
                    headers=headers,
                    data='{"mappings": {"type": {"properties": {"publisher": {"type": "text","fielddata": true}}}}}')
print(resp.content)
print(f'\nHTTP code: {resp.status_code} -- response: {resp}\n')
print(f'Response text\n{resp.text}')

# from datetime import date
# from datetime import datetime
# import pandas as pd
# from elasticsearch import Elasticsearch
# from elasticsearch import helpers
#
#
# elastic_user = "elastic"
# elastic_password = "rYPMyZFtwSyTIYn7gu4R"
# SOURCE = 'netflix_titles_flitered.csv'
# netflix_df = pd.read_csv(SOURCE)
# d = {'col1': [1, 2], 'col2': [3, 4]}
# new_df = pd.DataFrame(data=d)
#
# elastic_client = Elasticsearch("https://localhost:9200/", verify_certs=False,
#                                basic_auth=(elastic_user, elastic_password))
#
# def safe_date(date_value):
#     return (
#         pd.to_datetime(date_value) if ~ pd.isna(date_value)
#             else  datetime(1970,1,1,0,0)
#     )
#
# netflix_df = netflix_df.replace(np.NaN, "Empty", regex=True)
# netflix_df['date_added'] = netflix_df['date_added'].apply(safe_date)
#
#
# def doc_generator(df):
#     df_iter = df.iterrows()
#     for index, document in df_iter:
#         yield {
#             "_index": "netflix_shows",
#             "_source": document.to_dict(),
#         }
#
#
# print(helpers.bulk(elastic_client, doc_generator(netflix_df)))
