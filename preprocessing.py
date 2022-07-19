#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np


# ### Start

# In[2]:


def convert_companies_to_lowercase(dataframe):

    company_names = dataframe['Company name']

    keywords = ['limited', 'ltd']
    company_names_reg = []

    for i in list(company_names.values):
        indi_co = []
        name_string = i
        co_name_lower = str(name_string).lower()
        co_name = co_name_lower.split(' ')

        for j in range(len(co_name)):

            if co_name[j].lower() in keywords:

                co_name[j] = 'limited'
                indi_co.append(co_name[j])
            else:
                indi_co.append(co_name[j])

        company_names_reg.append(indi_co)

    return company_names_reg

def combine_company_names(dataframe):
    
    company_names_reg = convert_companies_to_lowercase(dataframe)

    co_name_combined = [' '.join(j) for j in company_names_reg]

    return co_name_combined

def get_duplicate_indices(dataframe):

#     company_names_reg = convert_companies_to_lowercase(dataframe)
    co_name_combined = combine_company_names(dataframe)

    unique_co = []
    unique_index = []
    duplicate_co = []
    duplicate_index = []

    for i,co in enumerate(co_name_combined): 
        if co not in unique_co: 
            unique_co.append(co) 
            unique_index.append(i)
        else:
            duplicate_co.append(co) 
            duplicate_index.append(i)

    return duplicate_index


def remove_duplicates_from_dataframe(dataframe):

    duplicate_indices = get_duplicate_indices(dataframe)

    dataframe = dataframe.drop(duplicate_indices)
    dataframe = dataframe.reset_index()
    dataframe = dataframe.drop("index", axis=1)

    return dataframe

def select_companies_with_sic_codes(dataframe):
    
    companies_with_sic = dataframe[dataframe['SIC codes'] != 0]
    companies_with_sic = companies_with_sic.reset_index()
    companies_with_sic = companies_with_sic.drop("index", axis=1)
    print("Shape of companies_with_sic: ", companies_with_sic.shape)
    
    with open('pickle1/companies_with_sic_codes.pickle', 'wb') as f:
        pickle.dump(companies_with_sic,f)
    print("companies_with_sic dataframe saved in pickle1")
    
    return companies_with_sic


    
    




