import warnings
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import date
from datetime import datetime
import re

elastic_user = "elastic"
elastic_password = "rYPMyZFtwSyTIYn7gu4R"
warnings.simplefilter(action='ignore')
SOURCE = 'netflix_titles.csv'
SOURCE_FILTERED = 'netflix_titles_flitered.csv'
netflix_df = pd.read_csv(SOURCE)
# netflix_df = netflix_df.loc[(netflix_df['date_added'] >= '2016-01-01')]  # filter all shows added before 2016
today_date = date.today()
date_format = today_date.strftime("%m/%d/%Y")
season = 18000

#
#
# def safe_date(date_value):
#     return (
#         pd.to_datetime(date_value) if ~ pd.isna(date_value)
#             else  datetime(1970,1,1,0,0)
#     )
#
# netflix_df = netflix_df.replace(np.NaN, "Other", regex=True)
# netflix_df['date_added'] = netflix_df['date_added'].apply(safe_date)
# def get_num_of_categories(df):
#     num_of_categories_lst = []
#     for row in range(len(df)):
#         get_catgories = df['listed_in'].iloc[row]
#         num_of_categories_lst.append(get_catgories.count(',') + 1)
#     return num_of_categories_lst
#
#
# def get_date(df):
#     date_lst = []
#     for row in range(len(df)):
#         date_lst.append(date_format)
#     return date_lst
#
#
# def get_duration_in_seconds(df):
#     duration_seconds_lst = []
#     for row in range(len(df)):
#         duration = str(netflix_df['duration'].iloc[row])
#         if 'Season' in duration or 'Seasons' in duration:
#             duration_seconds_lst.append(int(duration[0]) * season)
#         else:
#             try:
#                 num_of_season = re.findall('\d+', str(duration))[0]
#                 duration_seconds_lst.append(int(num_of_season) * 60)
#             except IndexError:
#                 duration = str(netflix_df['rating'].iloc[row])
#                 num_of_season = re.findall('\d+', str(duration))[0]
#                 duration_seconds_lst.append(int(num_of_season) * 60)
#     return duration_seconds_lst
#
#
# def get_directors(df):
#     director_lst = []
#     for row in range(len(df)):
#         director = str(netflix_df['director'].iloc[row])
#         if director == 'nan':
#             pass
#         else:
#             director_lst.append(director)
#     return director_lst
#
#
# netflix_df.insert(11, 'total number of categories', get_num_of_categories(netflix_df))
# netflix_df['current date'] = get_date(netflix_df)
# netflix_df.insert(10, 'duration in seconds', get_duration_in_seconds(netflix_df))
#
#
# def get_avg(df, director_lst):
#     name_and_avg_sec = []
#     for row in range(len(df)):
#         try:
#             dir_sec_lst = []
#             sumof = 0
#             dir_sec = df['duration in seconds'].where(df['director'] == director_lst[row]).dropna()
#             for i in range(len(dir_sec)):
#                 dir_sec_lst.append(int(dir_sec.iloc[i]))
#             num_of_item = len(dir_sec_lst)
#             for i in dir_sec_lst:
#                 sumof += i
#             num_of_item = len(dir_sec_lst)
#             avg_seconds = sumof / num_of_item
#             name_and_avg_sec.append(f"{avg_seconds} - {director_lst[i]}")
#         except IndexError:
#             pass
#     return name_and_avg_sec
#
#
# def get_country_list():
#     country_lst_dupe = []
#     country_lst = []
#     get_countries = netflix_df['country'].dropna()
#     for i in range(len(get_countries)):
#         split_countries = get_countries.iloc[i].split(", ")
#         for x in range(len(split_countries)):
#             country_lst_dupe.append(split_countries[x])
#     for i in country_lst_dupe:
#         if i not in country_lst:
#             if i == '' or i[-1] == ',':
#                 pass
#             else:
#                 country_lst.append(i)
#     return country_lst
#
#
# def get_num_of_catagories_by_list(country):
#     categories_lst_dupe = []
#     categories_lst = []
#     df = netflix_df['country'].dropna()
#     for i in range(len(df)):
#         if country in df.iloc[i]:
#             categories = netflix_df[['country', 'listed_in']].where(netflix_df['country'] == df.iloc[i]).dropna()
#             for i in range(len(categories)):
#                 split_categories = categories['listed_in'].iloc[i].split(", ")
#                 for j in range(len(split_categories)):
#                     categories_lst_dupe.append(split_categories[j])
#     for i in categories_lst_dupe:
#         if i not in categories_lst:
#             if i == '' or i[-1] == ',':
#                 pass
#             else:
#                 categories_lst.append(i)
#     return len(categories_lst)
#
#
# countrys = get_country_list()
#
#
# def non_unique_to_df(lst):
#     country_lst_final = []
#     num_of_categories_final = []
#     non_unique_dict = {}
#     for i in range(len(lst)):
#         country_lst_final.append(lst[i])
#         num_of_categories_final.append(get_num_of_catagories_by_list(lst[i]))
#     non_unique_dict['Country'] = country_lst_final
#     non_unique_dict['Number of non-unique categories'] = num_of_categories_final
#     new_df = pd.DataFrame(data=non_unique_dict)
#     return new_df

# df_new = non_unique_to_df(countrys)
# netflix_df['avg show time per director'] = pd.Series(get_avg(netflix_df, get_directors(netflix_df)))
# print(netflix_df)
# netflix_df.to_csv(SOURCE_FILTERED, index=False)

elastic_client = Elasticsearch("https://localhost:9200",verify_certs=False,basic_auth=(elastic_user,elastic_password))

def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": "netflix_shows",
                "_source": document,
            }

helpers.bulk(elastic_client, doc_generator(netflix_df))