import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np


def select_companies_with_phone(dataframe):
    
    companies_with_phone = dataframe[dataframe['Telephone'] != 0]
    companies_with_phone = companies_with_phone.reset_index()
    companies_with_phone = companies_with_phone.drop("index", axis=1)
    
    with open('pickle1/companies_with_phone.pickle', 'wb') as f:
        pickle.dump(companies_with_phone, f)
    
    return companies_with_phone

def define_x_paths():

    officer_name_paths = []
    active_status_paths = []
    officer_role_paths = []
    officer_occupation_paths = []
    num_of_leads_every_company = 5

    for i in range(1,num_of_leads_every_company+1):

        officer_name_path = '//*[@id="officer-name-' + str(i) + '"]/a/text()'
        officer_name_paths.append(officer_name_path)

        active_status_path = '//*[@id="officer-status-tag-' + str(i) + '"]/text()'
        active_status_paths.append(active_status_path)

        officer_role_path = '//*[@id="officer-role-' + str(i) + '"]/text()'
        officer_role_paths.append(officer_role_path)

        officer_occupation_path = '//*[@id="officer-occupation-' + str(i) + '"]/text()'
        officer_occupation_paths.append(officer_occupation_path)

    return officer_name_paths, active_status_paths, officer_role_paths, officer_occupation_paths



def extract_leads(dataframe, API_key):
    
    officer_name_paths, active_status_paths, officer_role_paths, officer_occupation_paths = define_x_paths()

    companies_id = dataframe['Company_Id']
    
    responses1 = []
    statuses = []
    officers = []
    roles = []
    occupations = []

    # 600 requests in 5 mins
    N=0.6


    # companies_ids - 1000 companies
    for id in tqdm(companies_id):

        try:

            http = "https://beta.companieshouse.gov.uk/company/" + id + "/officers"
            response = requests.get(http, auth=(API_key, ''))

            responses1.append(response)

            content = html.fromstring(response.content)


            ####### Status #######

            for i in range(len(active_status_paths)):


                try:
                    status = content.xpath(active_status_paths[i])[0]
                    statuses.append(status)

                except Exception as E:
                    print("Statuses", E)
                    statuses.append("")


            ####### Officers #######   

            for i in range(len(officer_name_paths)):

                try:   

                    officer = content.xpath(officer_name_paths[i])[0]
                    officers.append(officer)   


                except Exception as E:
                    print("Officers", E)
                    officers.append("")


            ####### Role #######  

            for i in range(len(officer_role_paths)):    

                try:    

                    if officers[i][0] != '':

                        role = content.xpath(officer_role_paths[i])[0].replace('\n                            ','').replace('\n                        ','')
                        roles.append(role)

                    else:
                        roles.append("")

                except:
                    roles.append("")


            ####### Occupation #######     

            for i in range(len(officer_occupation_paths)):        

                try:

                    if officers[i][0] != '':

                        occupation = content.xpath(officer_occupation_paths[i])[0].replace('\n                                ', '').replace('\n                            ', '')
                        occupations.append(occupation)

                    else:
                        occupations.append("")

                except:
                    occupations.append("")

        except:
            responses1.append('response not available for this company id')

    return companies_id, statuses, officers, roles, occupations


def add_to_df(dataframe, API_key):
    
    company_ids = []
    num_leads_in_every_company = 5
    
    companies_id, statuses, officers, roles, occupations = extract_leads(dataframe, API_key)

    for i in companies_id:
        for j in range(num_leads_in_every_company):
            company_ids.append(i)
            
    if len(statuses) == len(officers) and len(roles) == len(officers) and len(occupations) == len(officers):
        
        leads_df = pd.DataFrame(officers, columns = ['Lead'])
        leads_df['Status'] = statuses
        leads_df['Role'] = roles
        leads_df['Occupations'] = occupations
        leads_df.insert(0, 'Company id', company_ids) 
        
    else:
        print("Lengths do not match")
        
    return leads_df

## remove rows containing missing values

def remove_missing_values(dataframe, API_key):
    
    leads_df = add_to_df(dataframe, API_key)

    rows = []

    for i in range(leads_df.shape[0]):

        count = 0

        for j in range(1, leads_df.shape[1]):
            if leads_df.iloc[i,j] == '':
                count += 1

                if count == 4:
                    rows.append(i)

    empty_rows = np.unique(rows)
    rows_to_remove = list(empty_rows)

    ## dropping the rows from dataframe

    leads_df1 = leads_df.drop(rows_to_remove, axis = 0)
    
    ## reset index

    leads_df2 = leads_df1.reset_index()
    leads_df2 = leads_df2.drop('index', axis=1)
    leads_df2.head()
    
    return leads_df2

## remove rows containing 'Resigned' status

def remove_resigned(dataframe, API_key):
    
    leads_df2 = remove_missing_values(dataframe, API_key)

    rows = []

    for i in range(leads_df2.shape[0]):
        if leads_df2.iloc[i]['Status'] == 'Resigned':
            rows.append(i)

    rows_to_remove = list(rows)

    ## dropping the rows from dataframe

    leads_df3 = leads_df2.drop(rows_to_remove, axis = 0)
    
    ## reset index
    
    leads_df3_1 = leads_df3.reset_index()
    leads_df3_1 = leads_df3_1.drop("index", axis=1)
    
    return leads_df3_1

## remove rows containing 'Retired' status


def remove_retired(dataframe, API_key):
    
    leads_df3_1 = remove_resigned(dataframe, API_key)
    
    rows = []

    for i in range(leads_df3_1.shape[0]):
        if leads_df3_1.iloc[i]['Occupations'] == 'Retired':
            rows.append(i)

    rows_to_remove = list(rows)

    ## dropping the rows from dataframe

    leads_df4 = leads_df3_1.drop(rows_to_remove, axis = 0)
    leads_df4 = leads_df4.reset_index()
    leads_df4 = leads_df4.drop("index", axis=1)
    
    return leads_df4


def get_leads_df(dataframe, API_key):
    
    leads_df5 = remove_retired(dataframe, API_key)

    for i in range(leads_df5.shape[0]):
        for j in range(leads_df5.shape[1]):

            if leads_df5.iloc[i,j] == '':
                leads_df5.iloc[i,j] = '0'
    
    leads_df5 = leads_df5.reset_index()
    leads_df5 = leads_df5.drop('index', axis=1)
    
    with open('pickle1/leads_dataframe.pickle', 'wb') as f:
        pickle.dump(leads_df5,f)
    
    return leads_df5
    
    
    

    