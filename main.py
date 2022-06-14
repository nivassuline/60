import warnings
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import date
from datetime import datetime
import re

warnings.simplefilter(action='ignore')
ES_USER = "elastic"
ES_PASSWORD = "_sl=49UCWmU1TJmkhZf1"
ES_CLIENT = Elasticsearch("https://localhost:9200", verify_certs=False, basic_auth=(ES_USER, ES_PASSWORD))
SOURCE = 'netflix_titles.csv'
NETFLIX_DF = pd.read_csv(SOURCE)  # 2. read the csv into pandas dataframe (using python)
TODAY_DATE = date.today()
DATE_FORMAT = TODAY_DATE.strftime("%m/%d/%Y")
SEASON = 18000


def get_num_of_categories(df):  # - total number of catagories (listed_in column)
    num_of_categories_lst = []
    for row in range(len(df)):
        get_catgories = df['listed_in'].iloc[row]
        num_of_categories_lst.append(get_catgories.count(',') + 1)
    return num_of_categories_lst


def get_date(df):  # - current date
    date_lst = []
    for row in range(len(df)):
        date_lst.append(DATE_FORMAT)
    return date_lst


def get_duration_in_seconds(df):  # - duration in seconds (season=300min)
    duration_seconds_lst = []
    for row in range(len(df)):
        duration = str(NETFLIX_DF['duration'].iloc[row])
        if 'Season' in duration or 'Seasons' in duration:
            duration_seconds_lst.append(int(duration[0]) * SEASON)
        else:
            try:
                num_of_season = re.findall('\d+', str(duration))[0]
                duration_seconds_lst.append(int(num_of_season) * 60)
            except IndexError:
                duration = str(NETFLIX_DF['rating'].iloc[row])
                num_of_season = re.findall('\d+', str(duration))[0]
                duration_seconds_lst.append(int(num_of_season) * 60)
    return duration_seconds_lst


# 4.add columns to the df
NETFLIX_DF = NETFLIX_DF.loc[
    (NETFLIX_DF['date_added'] >= '2016-01-01')]  # 3. Filter all the shows that added before 2016 (date_added column)
NETFLIX_DF.insert(11, 'total number of categories', get_num_of_categories(NETFLIX_DF))
NETFLIX_DF['current date'] = get_date(NETFLIX_DF)
NETFLIX_DF.insert(10, 'duration in seconds', get_duration_in_seconds(NETFLIX_DF))


def get_directors(df):
    director_lst = []
    for row in range(len(df)):
        director = str(NETFLIX_DF['director'].iloc[row])
        if director == 'nan':
            pass
        else:
            director_lst.append(director)
    return director_lst


def get_avg(df, director_lst):  # 5. Add avg show time per director (using duration_in_seconds column)
    name_and_avg_sec = []
    for row in range(len(df)):
        try:
            dir_sec_lst = []
            sumof = 0
            dir_sec = df['duration in seconds'].where(df['director'] == director_lst[row]).dropna()
            for i in range(len(dir_sec)):
                dir_sec_lst.append(int(dir_sec.iloc[i]))
            num_of_item = len(dir_sec_lst)
            for i in dir_sec_lst:
                sumof += i
            num_of_item = len(dir_sec_lst)
            avg_seconds = sumof / num_of_item
            name_and_avg_sec.append(f"{avg_seconds} - {director_lst[i]}")
        except IndexError:
            pass
    return name_and_avg_sec


def get_country_list():
    country_lst_dupe = []
    country_lst = []
    get_countries = NETFLIX_DF['country'].dropna()
    for i in range(len(get_countries)):
        split_countries = get_countries.iloc[i].split(", ")
        for x in range(len(split_countries)):
            country_lst_dupe.append(split_countries[x])
    for i in country_lst_dupe:
        if i not in country_lst:
            if i == '' or i[-1] == ',':
                pass
            else:
                country_lst.append(i)
    return country_lst


def get_num_of_catagories_by_list(country):
    categories_lst_dupe = []
    categories_lst = []
    df = NETFLIX_DF['country'].dropna()
    for i in range(len(df)):
        if country in df.iloc[i]:
            categories = NETFLIX_DF[['country', 'listed_in']].where(NETFLIX_DF['country'] == df.iloc[i]).dropna()
            for i in range(len(categories)):
                split_categories = categories['listed_in'].iloc[i].split(", ")
                for j in range(len(split_categories)):
                    categories_lst_dupe.append(split_categories[j])
    for i in categories_lst_dupe:
        if i not in categories_lst:
            if i == '' or i[-1] == ',':
                pass
            else:
                categories_lst.append(i)
    return len(categories_lst)


countrys = get_country_list()


def non_unique_to_df(lst):
    country_lst_final = []
    num_of_categories_final = []
    non_unique_dict = {}
    for i in range(len(lst)):
        country_lst_final.append(lst[i])
        num_of_categories_final.append(get_num_of_catagories_by_list(lst[i]))
    non_unique_dict['Country'] = country_lst_final
    non_unique_dict['Number of non-unique categories'] = num_of_categories_final
    new_df = pd.DataFrame(data=non_unique_dict)
    return new_df


def safe_date(date_value):  # change all dates formats so elastic can accept them
    return (
        pd.to_datetime(date_value) if ~ pd.isna(date_value)
        else datetime(1970, 1, 1, 0, 0)
    )


NETFLIX_DF['avg show time per director'] = pd.Series(get_avg(NETFLIX_DF, get_directors(NETFLIX_DF)))
NETFLIX_DF = NETFLIX_DF.replace(np.NaN, "Empty", regex=True)  # Replace all Nan with other so elastic can accept them
NETFLIX_DF['date_added'] = NETFLIX_DF['date_added'].apply(safe_date)
NEW_DF = non_unique_to_df(countrys)  # Dataframe that represents the non-unique total number of categories per country


def doc_generator(df):  # Create index, Iter and push rows into index
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
            "_index": "netflix_shows",
            "_source": document.to_dict(),
        }


helpers.bulk(ES_CLIENT, doc_generator(NETFLIX_DF))  # Push all data into index
