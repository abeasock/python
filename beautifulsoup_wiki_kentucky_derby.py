##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 14JUN2018
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                             Script Information
#-----------------------------------------------------------------------------
# Script                 : beautifulsoup_wiki_kentucky_derby.py
# Brief Description      : Web scraping example using BeautifulSoup to extract
#                          an HTML table & add the data to a pandas DataFrame
# Data used              : https://en.wikipedia.org/wiki/Kentucky_Derby
# Output Files           : kentucky_derby_winners.txt
#
# Notes / Assumptions    : This scraper was last tested 14Jun2018 on the url
#-----------------------------------------------------------------------------
#                            Environment Information
#-----------------------------------------------------------------------------
# Python Version         : 3.6.2
# Anaconda Version       : 4.5.4
# Spark Version          : n/a
# Operating System       : Mac OS X
#-----------------------------------------------------------------------------
#                           Change Control Information
#-----------------------------------------------------------------------------
# Programmer Name/Date   : Change and Reason
#
##############################################################################


import urllib3 #library used to query a website
import pandas as pd
from bs4 import BeautifulSoup #import the Beautiful soup functions to parse the data returned from the website
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# specify the url
url = 'https://en.wikipedia.org/wiki/Kentucky_Derby'

# query the website, grab the HTML, and store in a BeautifulSoup object
def make_soup(url):
    http = urllib3.PoolManager()
    r = http.request("GET", url)
    return BeautifulSoup(r.data)

soup = make_soup(url)

print(soup.prettify())

results = soup.find('table', class_='wikitable sortable')
print(results)


data = {
        'year': [], 
        'winner': [], 
        'jockey': [], 
        'trainer': [],
        'owner': [],
        'distance_miles': [],
        'track_condition': [],
        'time': [],
        'note': []
        }


for result in results.findAll('tr'):
    cols = result.find_all('td')
    if len(cols)==8: #Only extract table body not heading
        data['year'].append(cols[0].find(text=True))
        data['winner'].append(cols[1].find(text=True))
        data['jockey'].append(cols[2].find(text=True))
        data['trainer'].append(cols[3].find(text=True))
        data['owner'].append(cols[4].find(text=True))
        data['distance_miles'].append(cols[5].find(text=True))
        data['track_condition'].append(cols[6].find(text=True))
        data['time'].append(cols[7].find(text=True))
        if cols[1].find('img'):
            data['note'].append(cols[1].find('img')['alt'])
        else:
            data['note'].append('')

derby = pd.DataFrame(data)

derby['triple_crown_winner'] = derby['note'].apply(lambda x: 'Y' if x=='Triple Crown Winner' else '')
derby['filly'] = derby['note'].apply(lambda x: 'Y' if x=='filly' else '')
derby.drop('note', axis=1, inplace=True)

# replace 1 ½ as 1.5 & 1 ¼ as 1.25
derby['distance_miles'] = derby['distance_miles'].replace(u'1 \xbc', u'1.25').replace(u'1 \xbd', u'1.5')

derby.to_csv('kentucky_derby_winners.txt', sep='|')
