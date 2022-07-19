#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import pickle
import psycopg2
import datetime


# In[ ]:


con = psycopg2.connect(database="leadgen", user="aiwa", password="Dev_Data01", host="127.0.0.1", port="5432")
#print("Database opened successfully")

cursor = con.cursor()

cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")

# for table in cursor.fetchall():
#     print(table)
    
def create_pandas_table(sql_query, database = con):
    table = pd.read_sql_query(sql_query, database)
    return table

companies_backup = create_pandas_table("SELECT *  FROM companies_flask")
#print("Companies backup size:", companies_backup.shape)

with open('pickle1/leadgen_platform_companies_backup' + str(datetime.datetime.now().strftime("-%Y-%m-%d-%H-%M")) + '.pickle','wb') as f:
    pickle.dump(companies_backup,f)



def regularise_company_names(company_names):

    
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
    
    new_company_names = dataframe['Company name']
    backup_company_names = companies_backup['company_name']
    
    new_company_names_regularised = regularise_company_names(new_company_names)
    backup_company_names_regularised = regularise_company_names(backup_company_names)
    
    new_dataframe = dataframe.copy()
    
    new_dataframe['company_names_regularised'] = new_company_names_regularised
    #salesforce_data['company_names_regularised'] = salesforce_companies
    
    duplicates = common_member(new_company_names_regularised, backup_company_names_regularised)
    
    if duplicates != "no common elements":
        return duplicates, new_dataframe ## new_dataframe holds regularised company names (lower case and limited)
    else:
        return "Null", new_dataframe
        print("No duplicates found")

    
    
def find_duplicate_indices(dataframe):
    
    duplicates, new_dataframe = find_duplicates(dataframe)
    
    if duplicates != 'Null':
    
        duplicates_df = new_dataframe[new_dataframe['company_names_regularised'].isin(duplicates)]
        duplicate_indices = duplicates_df.index
    
        return duplicate_indices
    
    else:
        return 'Null'
    

def drop_duplicates(dataframe):
    
    print("Before removing duplicates: ", dataframe.shape)
    
    duplicate_indices = find_duplicate_indices(dataframe)
    
    if duplicate_indices != 'Null':
    
        print("Number of duplicates found in the platform: ", len(duplicate_indices))
    
        unique_df = dataframe.drop(index=duplicate_indices)
        reset_unique_df = unique_df.reset_index()
        reset_unique_df = reset_unique_df.drop('index', axis = 1)
        print("After removing duplicates: ", reset_unique_df.shape)
        return reset_unique_df
    
    else:
        print("No duplicates found")
        return dataframe
    
    

