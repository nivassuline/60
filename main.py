import pandas as pd
from datetime import date
import re

SOURCE = 'netflix_titles.csv'
SOURCE_FILTERED = 'netflix_titles_flitered.csv'
netflix_df = pd.read_csv(SOURCE)
netflix_df_filtered = pd.read_csv(SOURCE_FILTERED)
today_date = date.today()
date_format = today_date.strftime("%B %d, %Y")
season = 18000

#netflix_df = pd.read_csv(SOURCE,parse_dates=['date_added'])
netflix_df = pd.read_csv(SOURCE)
netflix_df = netflix_df.loc[(netflix_df['date_added'] >= '2016-01-01')] #filter all shows added before 2016
num_of_categories_lst = []
date_lst = []
duration_seconds_lst = []
director_lst = []
for row in range(len(netflix_df)):
    listed_in_categories = netflix_df['listed_in'].iloc[row]
    num_of_categories_lst.append(listed_in_categories.count(',') + 1)
    date_lst.append(date_format)
    duration = str(netflix_df['duration'].iloc[row])
    director = str(netflix_df['director'].iloc[row])
    if 'Season' in duration or 'Seasons' in duration:
        duration_seconds_lst.append(int(duration[0]) * season)
    else:
        try:
            num_of_season = re.findall('\d+', str(duration))[0]
            duration_seconds_lst.append(int(num_of_season) * 60)
        except IndexError:
            duration = str(netflix_df['rating'].iloc[row])
            num_of_season = re.findall('\d+', str(duration))[0]
            duration_seconds_lst.append(int(num_of_season) * 60)
    if director == 'nan':
        pass
    else:
        director_lst.append(director)
netflix_df.insert(11,'total number of categories', num_of_categories_lst)
netflix_df['current date'] = date_lst
netflix_df.insert(10,'duration in seconds', duration_seconds_lst)
netflix_df.to_csv(SOURCE_FILTERED,index=False)
# dir_sec = netflix_df['duration in seconds'].where(netflix_df['director'] == director_lst[55]).dropna()
# print(dir_sec)

for row in range(len(netflix_df)):
    dir_sec = netflix_df['duration in seconds'].where(netflix_df['director'] == director_lst[row]).dropna()
    for i in range(len(dir_sec)):
        print(dir_sec.iloc[i])



# netflix_df['avg show time per director '] = ""
# netflix_df['current date'] = ""
# netflix_df['total number of categories '] = ""
# netflix_df['duration in seconds '] = ""
# netflix_df.to_csv(SOURCE_FILTERED,index=False)


#netflix_df.to_csv(SOURCE_FILTERED,index=False)



#netflix_df = netflix_df.where(netflix_df['date_added'][-1:-4] == '2021' )

#print(netflix_df.where(netflix_df['date_added'][0:3])
#netflix_df = netflix_df.loc[(netflix_df['date_added'] >= '2016-01-01')]
#ship_search(netflix_df,3,'show_id','s6942')
#netflix_df = netflix_df.where([netflix_df['show_id'] == 's6942'])
#netflix_df[(netflix_df['date'] > '2016-01-01')]
#print(netflix_df.where(netflix_df['date_added'] == '2017-01-16'))
