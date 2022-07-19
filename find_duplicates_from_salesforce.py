#!/usr/bin/env python
# coding: utf-8

# In[49]:


import pandas as pd
import pickle
import datetime


salesforce_data = pd.read_excel('../../CFX & Affiliate Modelling Data 20200130.xlsx')
#print("Salesforce data shape: ", salesforce_data.shape)


def regularise_companies(company_names):

    
    all_co_split_names = []
    all_co = []

    for name in list(company_names):

        co_name_lower = str(name).lower()
        co_name = co_name_lower.split(' ')
        all_co_split_names.append(co_name)

    for j, name in enumerate(all_co_split_names):
        individual_co = []
        for k in range(len(name)):
            if name[k] == 'ltd':
                name[k] = 'limited'
                individual_co.append(name[k])
            else:
                individual_co.append(name[k])
        all_co.append(individual_co)

    co_names_regularised = [' '.join(j) for j in all_co]
    
    return co_names_regularised 



def common_member(a, b):     
      
    a_set = set(a) 
    b_set = set(b)     
    
    # check length  
    if len(a_set.intersection(b_set)) > 0: 
        return(a_set.intersection(b_set))   
    else: 
        return("no common elements")


def find_duplicates(dataframe):
    
    backup_company_names = dataframe['Company name']
    salesforce_company_names = salesforce_data['Conpany Name']
    
    backup_companies = regularise_companies(backup_company_names)
    salesforce_companies = regularise_companies(salesforce_company_names)
    
    new_dataframe = dataframe.copy()
    
    new_dataframe['company_names_regularised'] = backup_companies
    #salesforce_data['company_names_regularised'] = salesforce_companies
    
    duplicates = common_member(backup_companies, salesforce_companies)
    
    if duplicates != "no common elements":
        return duplicates, new_dataframe ## new_dataframe holds regularised company names
    else:
        return "Null", new_dataframe
        print("No duplicates found")
    
    

def find_duplicate_indices(dataframe):
    
    duplicates, new_dataframe = find_duplicates(dataframe)
    
    print("Before removing duplicates (Salesforce): ", dataframe.shape)
    
    if duplicates != 'Null':
        
        print("Duplicates found in Salesforce: ", len(duplicates))
    
        duplicates_df = new_dataframe[new_dataframe['company_names_regularised'].isin(duplicates)]
        duplicate_indices = duplicates_df.index
    
        return duplicate_indices
    
    else:
        return 'Null'
        


def drop_duplicates(dataframe):
    
    duplicate_indices = find_duplicate_indices(dataframe)
    
    if duplicate_indices != 'Null':
    
        unique_df = dataframe.drop(index=duplicate_indices)
        reset_unique_df = unique_df.reset_index()
        reset_unique_df = reset_unique_df.drop('index', axis = 1)
        
        print("After removing duplicates (Salesforce): ", reset_unique_df.shape)
        
        return reset_unique_df
    
    else:
        print("No duplicates found in the Salesforce")
        return dataframe
        