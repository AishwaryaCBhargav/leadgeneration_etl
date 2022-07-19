import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np
    

def bing_responses(dataframe, API_key1):
    
    com=[]
    responses = []
    http = []
    N=5

    companies = list(dataframe['Company name'])

    for company in companies:
        comp=re.sub(" ", "%20", company)
        com.append(comp)
# https://api.cognitive.microsoft.com/bing/v7.0/search

    ## changed the link from bingcustomsearch to bing
    for i in range(len(com)):
        try:
            http.append("https://currenciesdirect-ml-bing-search-api.cognitiveservices.azure.com/bing/v7.0/search?q=" + com[i] + "&customconfig=d26c6c8e-f18b-4afc-9d74-14ac4d74b2be&mkt=en-GB")
#             http.append("https://api.cognitive.microsoft.com/bing/v7.0/search?q=" + com[i] + "&customconfig=d26c6c8e-f18b-4afc-9d74-14ac4d74b2be&mkt=en-GB")
        except Exception as E:
            print(E)

            
            
    for i in tqdm(range(len(http))):
        try:
            response = requests.get(http[i], headers={"Accept":"application/json","Ocp-Apim-Subscription-Key":API_key1})
            responses.append(response)
            time.sleep(N)

        except Exception as E:
            responses.append('NA')
            time.sleep(N)
            print(E)

    with open('pickle1/bing_responses', 'wb') as f:
        pickle.dump(responses,f)
        
    
        
    return responses

    
def get_description(dataframe, API_key1) :
    
    description = []
    cos = []
    
    responses = bing_responses(dataframe, API_key1)
    
    for co in responses:
        cos.append(co.json())
       
    
    
    # with try and except
    for num,co in enumerate(cos):

        desc = []

        try:

            for i in range(len(co['webPages']['value'])):

                desc.append(co['webPages']['value'][i]['snippet'])
                str1 = ''.join(desc)
                str2 = re.sub(" \.\.\.", '. ', str1)
            description.append(str2)
            print(num, '\n', str2, '\n', '......')


        except Exception as E:
            print(num, E)
            
            description.append(0)

    with open('pickle1/bing_uncleaned_description.pickle', 'wb') as f:
        pickle.dump(description, f)
        
    return description

def clean_description(dataframe, API_key1):
    
    companies = []
    
    uncleaned_description = get_description(dataframe, API_key1)

    negative_keywords = ['free', 'companies house', 'website', 'employer', 'employment', '.com', 'history', 'redirected', 'hired',
                        'address', ' i ', '.i ' 'learn', 'linkedin', 'home', 'contact', 'com', 'see', 'site','jobs', 'listed', 
                         'tweets', 'facebook', 'profile', 'twitter', 'learn', 'liaise', 'liaison', 'likes', 'subscribe', 'sex', 'sexy',
                         'レポート . 九州・アジア経営塾の取り組み、卒塾生の活動等をお届け', 'about',
                         'します。ご覧いただくには、idとパスワードが必要です。閲覧及びお申込みはこちらから']

    for i in range(len(uncleaned_description)):

        try:
            sentences = str(uncleaned_description[i]).split('.')

            tt = []

            for i,sentence in enumerate(sentences):

                for j,negative_keyword in enumerate(negative_keywords):

                    if negative_keyword in sentence.lower():
                        tt.append(i)         
            er=set(tt)

            rt = []

            for i in er:
                rt.append(sentences[i])

            rt, len(rt)

            yu = [i for i in sentences if i not in rt]
            yu, len(yu)

            erty = '.'.join(string for string in yu)
            erty = erty.rstrip()
            erty = erty.replace("'", '')
            erty = erty.replace("...", '')
            erty = erty.replace("..", '')

            companies.append(erty)  

        except Exception as E:
            print(E)  

    with open('pickle1/cleaned_description.pickle', 'wb') as f:
        pickle.dump(companies, f) 
        
    return companies

## Add source
def add_source_to_description(dataframe, API_key1):

    descr_w_source = []
    
    companies = clean_description(dataframe, API_key1)
    

    for i in range(len(companies)):
        descr_w_source.append(str("Source: Bing Web Search API - ") + str(companies[i]))


    with open('pickle1/cleaned_description_with_source.pickle', 'wb') as f:
        pickle.dump(descr_w_source, f)
        
    return descr_w_source

def get_available_number_of_descriptions(dataframe):

    count = 0
    zero = []

    for i,des in enumerate(dataframe['Company Description']):
        if str(des) != "Source: Bing Web Search API - 0":
            zero.append(i)

            count += 1
            
    print("Available number of descriptions: ", count)


def add_description(dataframe, API_key1):
    
    descr_w_source = add_source_to_description(dataframe, API_key1)
    
    if len(descr_w_source) == dataframe.shape[0]:
        dataframe['Company Description'] = descr_w_source
        
    else:
        print("Description length does not match")
        
    get_available_number_of_descriptions(dataframe)
        
    return dataframe



