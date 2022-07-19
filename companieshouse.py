import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np


def get_id_date_address_postcode(dataframe, API_key):
    
    companies = list(dataframe['Company name'].values)
    
    companies_id = []
    incorp_dates = []
    addresses = []
    postcodes = []

    responses = []

    # 600 requests in 5 mins
    N=2
    counter = 0

    for i,company in enumerate(tqdm(companies)):
        
    
        try:

            http = 'https://api.companieshouse.gov.uk/search/companies?q=' + company + '&items_per_page=100&start_index=0'    
            response = requests.get(http, auth=(API_key, ''))

            responses.append(response)

            with open('pickle2/ch_part1_response' + str(counter) + '.pickle', 'wb') as f:
                pickle.dump(response, f)
                
            counter += 1

            response_json = response.json()

            try:
                companies_details=response_json['items']      
                company_Id=companies_details[0]['company_number']
                companies_id.append(company_Id)

            except Exception as E:
                companies_id.append('NA')
                print("companies_id", i,E)

            try:
                incorp_date = response_json['items'][0]['date_of_creation']
                incorp_dates.append(incorp_date)

            except Exception as E:
                incorp_dates.append('NA')
                print("incorp_dates", i,E)   

            try:
                address = response_json['items'][0]['address']
                addresses.append(address)

            except Exception as E:
                addresses.append('NA')
                print("addresses", i,E)

            try:
                address = response_json['items'][0]['address']
                postcode = address['postal_code']
                postcodes.append(postcode)

            except Exception as E:
                postcodes.append('NA')
                print("postcodes", i,E)

            time.sleep(N)

        except Exception as E:
            responses.append('NA')
            postcodes.append('NA')
            addresses.append('NA')
            incorp_dates.append('NA')
            companies_id.append('NA')
            print(i,E)
            
#     if len(responses) == dataframe.shape[0]:
#         print("Part1 responses lengths match")
        
    with open('pickle1/companieshouse_part1_responses.pickle', 'wb') as f:
        pickle.dump(responses, f)
        
    with open('pickle1/companieshouse_companies_id.pickle', 'wb') as f:
        pickle.dump(companies_id, f)
        
    with open('pickle1/companieshouse_incorp_dates.pickle', 'wb') as f:
        pickle.dump(incorp_dates, f)
        
    with open('pickle1/companieshouse_addresses.pickle', 'wb') as f:
        pickle.dump(addresses, f)
                
    with open('pickle1/companieshouse_postcodes.pickle', 'wb') as f:
        pickle.dump(postcodes, f)
    
#     else:
#         print("part 1 response lengths do not match")
            
    return responses, companies_id, incorp_dates, addresses, postcodes


def get_and_check_data(dataframe, API_key):
    
    responses, companies_id, incorp_dates, addresses, postcodes = get_id_date_address_postcode(dataframe, API_key)
    
    print("Length of part 1 responses: ", len(responses))
    print("Length of company ids: ", len(companies_id))
    print("Length of incorp dates: ", len(incorp_dates))
    print("Length of addresses: ", len(addresses))
    print("Length of postcodes: ", len(postcodes))
      
    count_response = 0
    count_id = 0
    count_add = 0
    count_postcodes = 0
    count_incorp_dates = 0
    
    
    for i in responses:
        if str(i) == '<Response [200]>':
            count_response += 1
    print("Number of successful part 1 responses: ", count_response)
    
    for i in companies_id:
        if i != 'NA':
            count_id += 1
    print("Number of successful IDs: ", count_id)
    
    for i in addresses:
        if i != 'NA':
            count_add += 1
    print("Number of successful address: ", count_add)
    
    for i in postcodes:
        if i != 'NA':
            count_postcodes += 1
    print("Number of successful postcodes: ", count_postcodes)
    
    for i in incorp_dates:
        if i != 'NA':
            count_incorp_dates += 1
    print("Number of successful incorp_dates: ", count_incorp_dates)
    
    return responses, companies_id, incorp_dates, addresses, postcodes
    
    
def add_data_to_df(companies_id, incorp_dates, addresses, postcodes, dataframe):
    
    if len(companies_id) == dataframe.shape[0]:       
        dataframe['Company_Id'] = companies_id
    else:
        print("Companies ID lengths do not match")
        
    
    if len(incorp_dates) == dataframe.shape[0]:    
        dataframe['Incorp_date'] = incorp_dates   
    else:
        print("Incorp dates lengths do not match")
        
        
    if len(addresses) == dataframe.shape[0]:
        dataframe['Address'] = addresses
    else:
        print("Address lengths do not match")
        
        
    if len(postcodes) == dataframe.shape[0]: 
        dataframe['Postcode'] = postcodes
    else:
        print("Postcodes lengths do not match")
        
    return dataframe



def get_sic_code_responses(dataframe, API_key):
    
    b2b_https = []
    b2b_responses = []
    
    count_responses1 = 0
    counter = 0
    N=2
    
    companies_id = list(dataframe['Company_Id'].values)
    
    for id in companies_id:
         b2b_https.append("https://beta.companieshouse.gov.uk/company/" + id)           

    for http in tqdm(b2b_https):
        
        try:
            response2 = requests.get(http, auth=(API_key, ''))
            b2b_responses.append(response2)
            
            with open('pickle2/ch_part2_response' + str(counter) + '.pickle', 'wb') as f:
                pickle.dump(response2, f)
                
            counter += 1
            time.sleep(N)
            
        except Exception as E:
            b2b_responses.append('NA')
            continue 
            print(E)
          
    with open('pickle1/companieshouse_part2_responses.pickle', 'wb') as f:
        pickle.dump(b2b_responses, f) 
            
    for i in b2b_responses:
        if i != 0:
            count_responses1 += 1
    print("Number of successful part 2 responses: ", count_responses1)        
            
    return b2b_responses



def get_sic_codes(dataframe, API_key):
    
    status_path = '//*[@id="company-status"]/text()'
    sic_code = '//*[@id="sic0"]/text()'

    ind_codes = []
    industry_desc = []

    comp_status = []
    
    active=0
    count_sic = 0
    count_desc = 0
    
    b2b_responses = get_sic_code_responses(dataframe, API_key)
    
    for i, response in enumerate(tqdm(b2b_responses)):
    
        try:

            page1 = response
            tree1 = html.fromstring(page1.content)
            soup1 = tree1.xpath(status_path)

            try:
                status = soup1[0].replace("\n                    ","").replace("\n","").replace("                ", "")

            except:
                pass

            if status == 'Active':
                active += 1

                page = response
                tree = html.fromstring(page.content)
                soup = tree.xpath(sic_code)

                try:
                    desc = soup[0].replace("\n                    ","").replace("\n","").replace("                ", "")
                    industry_desc.append(desc)
                    code = desc[0:5]
                    ind_codes.append(code)

                except:
                    industry_desc.append(0)
                    ind_codes.append(0)

            else:
                industry_desc.append(0)
                ind_codes.append(0)
                print(i, "Not active")

        except Exception as E:
            industry_desc.append(0)
            ind_codes.append(0)
            print(E)
            
    with open('pickle1/sic_codes', 'wb') as f:
        pickle.dump(ind_codes, f)
              
    with open('pickle1/industry_code_description', 'wb') as f:
        pickle.dump(industry_desc, f)
            
    print("Length of obtained part 2 responses: ", len(b2b_responses))
    print("Length of obtained sic codes: ", len(ind_codes))
    print("Length of obtained industry descriptions: ", len(industry_desc))
    
    
    for i in ind_codes:
        if i != 0:
            count_sic += 1
    print("Number of successful sic codes: ", count_sic)
    
    
    for i in industry_desc:
        if i != 0:
            count_desc += 1
    print("Number of successful industry description: ", count_desc)
       
    return b2b_responses, ind_codes, industry_desc



def add_sic_codes(ind_codes, industry_desc, dataframe):
    
    if len(ind_codes) == dataframe.shape[0]:
        dataframe['SIC codes'] = ind_codes
        
    if len(industry_desc) == dataframe.shape[0]:
        dataframe['Industry description'] = industry_desc
        
    return dataframe
        
    