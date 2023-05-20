import requests
import pandas as pd
from bs4 import BeautifulSoup
import csv

walkscore_df = pd.DataFrame(columns=['city_rank', 'neighborhood', 'walk_score', 'transit_score', 'bike_score', 'population', 'city_name', 'state_id'])


neighborhoods_csv = './csvs/zhvi_neighborhoods.csv'

#load in csv file 
with open(neighborhoods_csv) as f:
    reader = csv.reader(f)
    neighborhoods_data = [r for r in reader]

#get distinct list of city and state pairs
city_names = [(x[neighborhoods_data[0].index('city_name')], x[neighborhoods_data[0].index('state_id')]) for x in neighborhoods_data[1:]]
city_names = list(set(city_names))

#for each city, replace spaces with underscores and add to url
for city in city_names:
    if (city[0] == 'Washington' and city[1] == 'DC'):
        url = "https://www.walkscore.com/DC/Washington_D.C."
    else:
        url = "https://www.walkscore.com/" + city[1] + "/" + city[0].replace(" ", "_")
    print("Scraping walkscore url: " + url)
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'hoods-list-table'})
    rows = table.find_all('tr')
    data = []

    # Iterate over rows
    for row in rows:
        columns = row.find_all('td')
        # Extract text from each column
        row_data = [column.text.strip() for column in columns]
        data.append(row_data)

    #remove empty lists
    data = [x for x in data if x != []]
    #append data to walkscore_df
    for row in data:
        row.append(city[0])
        row.append(city[1])
        walkscore_df.loc[len(walkscore_df)] = row
        
#export walkscore_df to csv
walkscore_df.to_csv('./csvs/walkscores.csv', index=False)
