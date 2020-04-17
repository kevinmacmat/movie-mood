#!/usr/bin/env python
# coding: utf-8

# # Functions for webscraper script

# In[ ]:


import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import json
from bs4 import BeautifulSoup
import re
import urllib
import os


# # Box Office Mojo Worldwide DF

# In[ ]:


url = 'https://www.boxofficemojo.com/year/world/'
def get_worldwide(url):
    '''
    take in a url and obtain a list of urls to parse through
    '''
    #container for holding dataframe
    container = []
    
    #for each year between 1999, 2019, create a url to easily read the html and output a dataframe of top 100 in year
    for year in range(1999,2020): #1999, 2019   
        df = pd.read_html(url + str(year))       
        container.append(df[0][:100]) 
    #concat the list of dataframes and return one big dataframe
    return pd.concat(container, axis = 0, ignore_index = True)
df = get_worldwide(url)


# # Box Office Mojo - Foreign Markets

# In[ ]:


def get_worldwide_link_list(url):
    '''
    Given the url to a year, obtain a list of working urls to later parse through
    '''
    container = []
    for year in range(2019,1998,-1): #1999, 2019
        url + str(year)
        container.append(url + str(year))
    return container


# In[ ]:


def get_country_hyperlinks(link_list):    #give it a list
    '''
    Given a list of year urls, go into the hyperlink of each table row (title of movie),
    Retrieve a long list of hyperlinks containing country revenue information
    '''
    link_results = []
    for link in link_list:   #use one entry for ex. 2019
        page = requests.get(link) #requests 2019 url
        src = page.content  #turn to page
        soup = BeautifulSoup(src, 'html.parser') #turn 2019 to soup
        all_a = soup.find_all('a') #grab anchors from 2019 soup an put into a list of links
        c = []  
        for x in all_a: #looking through hyper links 'hrefs'
            c.append(x.attrs['href']) #append link to container
            # cleans hyperlinks to only include relevant
        movie_urls_year = []
        for x in c[:119]:         #filters through slice of container to get wanted releasegroup urls
            if 'releasegroup' in x:   
                movie_urls_year.append(x)
        link_results.extend(movie_urls_year)
    return link_results

# get_country_hyperlinks(link_list)


# In[ ]:


def get_countries_for_title(url):
    '''
    For given url, go to site and obtain foriegn countries revenue if possible
    '''
    # Make a request for each title
    html_table = urllib.request.urlopen('https://www.boxofficemojo.com/{}'.format(url)).read()

    # Turn into soup
    soop = BeautifulSoup(html_table, "html.parser")
    
    # Grab title
    title = soop.find('h1', class_='a-size-extra-large').get_text()
    
    # Grab countries
    for table in soop.findChildren(attrs={'class': 'a-align-center'}):  #wat this do again
        for c in table.children:
            if c.name in ['tr', 'th']:
                c.unwrap()

    #obtain a list of dataframes from reading html, this will contain domestic information
    df_market = pd.read_html(str(soop), flavor="bs4") #list of data
    
    #foriegn information will exist in lists with length > 1 
    if len(df_market) > 1:
        list_dataframe = [] #[df1,df2,df3]   [df1,]
        for dataframe in df_market[1:]:  #take foriegn information
            list_dataframe.append(dataframe.droplevel(level=0,axis=1))   #fix column's multi-index
        result = pd.concat(list_dataframe,ignore_index = True)    #concat columnwise 
        result['title'] = title  #create new column with repeating title
        return result
    else:
    #if len is 1, then return empty dataframe with columns
        result = pd.DataFrame(columns = ['Market', 'Release Date', 'Opening', 'Gross', 'title'])
        return result


# In[ ]:


def scrape_actors(url):
    '''Given url from clicking 'cast and crew' button, take in information and return a df with actors'''
    result = []
    
    request_url = url
    response = requests.get(request_url)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    inner_container = []
    
    for entry in soup.find('table', id = 'principalCast').findAll('tr')[1:]:
        #clean and obtain only the actor's name from [actor,char]
        inner_container.append(entry.get_text()[:-8].split('\n\n')[0])  
    result.append(inner_container)
    
    columns = ['Actor 1','Actor 2','Actor 3','Actor 4']
    
    #if the length of Actors is less than 4, fill the remaining with NaN
    while len(inner_container) < 4:
        inner_container.append(np.nan)
    
    actor_frame = pd.DataFrame(result,columns = columns)
    return actor_frame


# In[ ]:


def fill_values(result, result_2):
    ''' 
    helper function for cleaning scraped actors.
    Propogate result_2 dataframe's columns throughout result dataframe;
    result_2 will have max of 4 columns with set column names
    '''
    a1 = result_2.values[0][0]
    a2 = result_2.values[0][1]
    a3 = result_2.values[0][2]
    a4 = result_2.values[0][3]
    
    act_dict = {
        'Actor 1' : a1,
        'Actor 2' : a2,
        'Actor 3' : a3,
        'Actor 4' : a4
    }
    #concat and fill the rest of NaN with values from result 2
    return pd.concat([result, result_2],axis = 1).fillna(value = act_dict)


# In[ ]:


def reroute_twice(url):
    '''
    helper function to get to destination from 2 url links away
    get from release link -> title release -> cast and crew
    '''
    home = 'https://www.boxofficemojo.com'
    
    #first click
    request_url = home + url
    response = requests.get(request_url)
    soup = BeautifulSoup(response.content)
    appended = soup.find('a','a-link-normal mojo-title-link refiner-display-highlight').get('href')

    second = '{}{}'.format(home,appended) #second url
    #get the second click
    response2 = requests.get(second)
    soup2 = BeautifulSoup(response2.content)
    appended2 = soup2.find('a',class_='a-size-base a-link-normal mojo-navigation-tab').get('href')
    final_url = home + appended2
    return final_url


# In[ ]:


def data_save(dataframe, csv_filename):
    '''save final dataframe into existing csv'''
    f = open(csv_filename)
    columns = ['Market', 'Release Date', 'Opening', 'Gross', 'title', 'Actor 1','Actor 2', 'Actor 3', 'Actor 4']
    
    #make existing dataframe to append
    data = pd.read_csv(f)
    df1 = pd.DataFrame(data,columns=columns)
    
    #make input dataframe into dataframe
    df2 = pd.DataFrame(dataframe, columns = columns)
    
    df3 = pd.concat([df1,df2])
    
    #overwrites df to csv
    df3.to_csv('movies_test.csv',index = False)


# In[ ]:


def make_or_clear_movies_csv(csv_filename):
    ''' make a csv if it doesnt exist, else CLEAR the existing one leaving headers. Run once only'''

    headers = pd.DataFrame(columns = ['Market', 'Release Date', 'Opening', 'Gross', 'title', 'Actor 1',
       'Actor 2', 'Actor 3', 'Actor 4'])
    
    if os.path.exists(csv_filename):
        f = open(csv_filename, 'w')
        f.truncate() # CLEAR IT
        headers.to_csv(csv_filename, mode='a') #add headers
        
    else:
        headers.to_csv(csv_filename) #save csv with headers

