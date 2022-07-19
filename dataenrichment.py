import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np


## Add city from the extracted address
def add_company_address(dataframe):
    
    addresses = []

    for j in range(dataframe.shape[0]):
        
        try:
            add = []

            for i in range(len(str(dataframe['Address'][j]).split(','))):

                try:
                    ad = str(dataframe['Address'][j]).split(',')[i].split(':')[1].replace("'", '').lstrip(' ')
                    add.append(ad)
                    addr = ', '.join(string for string in add)
                    address = addr.replace('}','').replace('"', '')
                    #print(address)


                except Exception as E:
                    print("2", j,E)

            addresses.append(address)      
                
        except Exception as E:
            print("1", j,  E)
            
    if len(addresses) == dataframe.shape[0]:
        dataframe['Company_address'] = addresses
        
    else:
        print("Data lengths do not match")
        
    new_dataframe = dataframe.drop("Address", axis=1)
        
    return new_dataframe

## Add city

def add_city(dataframe):
    
    cities = []

    for i, add in enumerate(dataframe['Address']):

        try:

            cities.append(add['region'])

        except Exception as E:
            #print(E)

            try:
                cities.append(add['locality'])

            except Exception as E:
                try:
                    cities.append(add['address_line_2'])
                except Exception as E:
                    cities.append(0)
                
                #print(i, E)
                
    if len(cities) == dataframe.shape[0]:
        dataframe['City'] = cities
        
    else:
        print("Data lengths do not match")
              
    return dataframe

def add_country(dataframe):
    
    countries = []

    for i in range(dataframe.shape[0]):
        countries.append('UK')
        
    if len(countries) == dataframe.shape[0]:
        dataframe['Country'] = countries
        
    else:
        print("Data lengths do not match")
              
    return dataframe



def add_financial_size(dataframe):
    
    financial_size = []

    for i,nw in enumerate(dataframe['Net worth']):

        try:
            if int(nw) >= int(0) and int(nw) < int(2000000):
                financial_size.append("Small")


            elif int(nw) > int(2000000) and int(nw) < int(5000000):
                financial_size.append("Medium") 

            elif int(nw) > int(5000000):
                financial_size.append("Large") 

            else:
                financial_size.append("Unknown")

        except Exception as E:
            financial_size.append("Unknown")
            print(i,E)
            
    if len(financial_size) == dataframe.shape[0]:   
        dataframe["Financial size"] = financial_size
        
    else:
        print("LinkedIn links list length does not match")        
   
            
    return dataframe



def add_linkedin_link(dataframe):
    
    linkedins = []

    for i in range(dataframe.shape[0]):
        
        company = 'https://www.linkedin.com/search/results/companies/?keywords=' + str(dataframe['Company name'][i]).replace(" ", "%20") + '&origin=SWITCH_SEARCH_VERTICAL'
        
        linkedins.append(company)
    
    if len(linkedins) == dataframe.shape[0]:   
        dataframe['Linkedin'] = linkedins
        
    else:
        print("LinkedIn links list length does not match")
        
    return dataframe


def add_importer_exporter(dataframe):
    
    importer_exporter = []
   

    for i in range(dataframe.shape[0]):
        importer_exporter.append("Importer")
        
    if len(importer_exporter) == dataframe.shape[0]:   
        dataframe['Importer/Exporter'] = importer_exporter
        
    else:
        print("Importer/Exporter length does not match")
        
    return dataframe


def add_last_trade_month(dataframe):
    month = []

    for i in range(dataframe.shape[0]):
        month.append("April")
        
    if len(month) == dataframe.shape[0]:   
        dataframe['Last operation month'] = month
        
    else:
        print("Month length does not match")
        
    return dataframe

def add_scores(dataframe):
    
    # health and food = +1
    # importer = +1
    # telephone = +1
    # description = +1
    
    scores = []

    for i in range(dataframe.shape[0]):

        score = 5

        if dataframe['Net worth'][i] != 0:
            score += 1

        scores.append(score)
        
    if len(scores) == dataframe.shape[0]:
        dataframe['Scores'] = scores
    else:
        print("Scores data lengths do not match")

    return dataframe



def add_status(dataframe):
    status = []

    for i in range(dataframe.shape[0]):
        status.append("Open")
        
    if len(status) == dataframe.shape[0]:
        dataframe['Status'] = status
    else:
        print("Status data lengths do not match")
        
    return dataframe



def add_pitch(dataframe):
    pitch = []

    for i in range(dataframe.shape[0]):
        pitch.append(0)
        
    if len(pitch) == dataframe.shape[0]:
        dataframe['Pitch'] = pitch
    else:
        print("Pitch data lengths do not match")

    return dataframe 



def add_user(dataframe):
    user = []

    for i in range(dataframe.shape[0]):
        user.append(0)
        
    if len(user) == dataframe.shape[0]:
        dataframe['User'] = user
    else:
        print("User data lengths do not match")

    return dataframe 

def add_CH_link(dataframe):
    CH_link = []
    
    company_id = dataframe['Company_Id']

    for id in company_id:
         CH_link.append("https://beta.companieshouse.gov.uk/company/" + id)
            
    if len(CH_link) == dataframe.shape[0]: 
        dataframe['CH link'] = CH_link

    else:
        print("User data lengths do not match")

    return dataframe   