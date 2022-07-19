#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np

import preprocessing
import companieshouse
import dataenrichment
import DandB
import description
import dataframe_cleaning
import leads

import find_duplicates_from_salesforce
import find_duplicates_in_platform


# In[3]:


# companies house
API_key = 'uL1dagVtwOO2j1YSCZFAstwdU4PoRDfi8NoqX-UL'

# Bing 

API_key1 = "179b9501124b41c1a424e67ee5867c75"
API_key2 = "7ab95e4752114d4d90a92c923a6c930a"




may_importers = pd.read_csv("importers2005.csv", sep='\t', encoding='latin-1', header=None)
print(may_importers.shape)
may_importers.head()


# In[7]:


may_importers_df = may_importers.iloc[:,0:9]
may_importers_df.columns = ['Month', 'Number', 'Company name', 'Address 1', 'Address 2','Area','City', 'Unnamed', 'Postcode']
may_importers_df = may_importers_df.drop(['Month','Number', 'Unnamed'], axis=1)

## pickling

with open('may_importers_dataframe.pickle', 'wb') as f:
    pickle.dump(may_importers_df,f)





print("Before removing any duplicates ", may_importers_df.shape)



company_df01 = preprocessing.remove_duplicates_from_dataframe(may_importers_df)



# In[11]:


company_df01_1= find_duplicates_from_salesforce.drop_duplicates(company_df01)



# In[12]:


company_df01_3 = find_duplicates_in_platform.drop_duplicates(company_df01_1)


# ### add id, date, address and postcode

# In[ ]:


responses, companies_id, incorp_dates, addresses, postcodes = companieshouse.get_and_check_data(company_df01_3, API_key)


company_df02 = companieshouse.add_data_to_df(companies_id, incorp_dates, addresses, postcodes, company_df01_3)


responses2, ind_codes, industry_desc = companieshouse.get_sic_codes(company_df02, API_key)



company_df03 = companieshouse.add_sic_codes(ind_codes, industry_desc, company_df02)


company_df04 = preprocessing.select_companies_with_sic_codes(company_df03)


# In[39]:


company_df05 = dataenrichment.add_city(company_df04)
company_df06 = dataenrichment.add_company_address(company_df05)
company_df07 = dataenrichment.add_country(company_df06)



essential_cos = [ '47910', '93130', '95220', '10512', '10821', '47190', '62011', '82990', '10612', '10860', '10890'                 ,'10920', '28930', '46170', '46380', '46310', '46342', '46390', '47110', '47290', '47760', '47810'                 ,'56103', '56290', '56103', '56290', '33170', '49200', '49410', '50200', '50400', '51210', '52101', '52102'                 ,'52103', '52211', '52241', '52242', '52243', '53100', '60100', '60200', '61900', '62011', '62012', '62020'                 ,'62030', '62090', '63110', '63120', '63910', '86101', '86102', '86210', '86220', '86230', '86900', '82920'                 ,'28250', '81210', '81222', '26309', '10720' , '84250', '13931', '10320', '95210', '47240', '64306', '10520'                 ,'10730', '10511', '20411', '10832', '95110', '46360', '47230', '58210', '33130', '47220', '64922', '68310']
 


essential_cos_df = company_df07[company_df07['SIC codes'].isin(essential_cos)]
essential_cos_df = essential_cos_df.reset_index(drop=True)


# Enrich data with D&B API



telephonenumbers, num_of_employees, net_worths, netIncomes = DandB.extract_DandB_data(essential_cos_df)



company_df08 = DandB.check_data_and_add(telephonenumbers, num_of_employees, net_worths, netIncomes, essential_cos_df)



cos_with_phonenumbers_df = company_df08[company_df08['Telephone number'] != 0]
print("cos_with_phonenumbers", cos_with_phonenumbers_df.shape)
cos_with_phonenumbers_df = cos_with_phonenumbers_df.reset_index()
cos_with_phonenumbers_df = cos_with_phonenumbers_df.drop("index", axis=1)
print(cos_with_phonenumbers_df.shape)
with open('pickle1/cos_with_phonenumbers_df', 'wb') as f:
    pickle.dump(cos_with_phonenumbers_df,f)
print("cos_with_phonenumbers_df stored in pickle1")


cos_with_phonenumbers_and_networth_df = cos_with_phonenumbers_df[pd.to_numeric(cos_with_phonenumbers_df['Net worth']) > (0)]
print("cos_with_phonenumbers_and_networth_df", cos_with_phonenumbers_and_networth_df.shape)
cos_with_phonenumbers_and_networth_df = cos_with_phonenumbers_and_networth_df.reset_index(drop=True)
print(cos_with_phonenumbers_and_networth_df.shape)
with open('pickle1/cos_with_phonenumbers_and_networth_df', 'wb') as f:
    pickle.dump(cos_with_phonenumbers_and_networth_df,f)
print("cos_with_phonenumbers_and_networth_df stored in pickle1")




#essential_cos_with_phonenumbers
company_df09 = dataenrichment.add_financial_size(cos_with_phonenumbers_and_networth_df)
company_df10 = dataenrichment.add_linkedin_link(company_df09)
company_df11 = dataenrichment.add_importer_exporter(company_df10)
company_df12 = dataenrichment.add_last_trade_month(company_df11)


company_df13 = description.add_description(company_df12, API_key2)


print("Before removing duplicates (platform): ", company_df13.shape)
company_df14_1 = find_duplicates_in_platform.drop_duplicates(company_df13)
print("After removing duplicates (platform): ", company_df14_1.shape)


# In[214]:


print("Before removing duplicates (salesforce): ", company_df14_1.shape)
company_df14_2 = find_duplicates_from_salesforce.drop_duplicates(company_df14_1)
print("After removing duplicates (salesforce): ", company_df14_2.shape)



## Add misc data 
company_df14 = dataenrichment1.add_scores(company_df14_2)
company_df15 = dataenrichment1.add_status(company_df14)
company_df16 = dataenrichment1.add_pitch(company_df15)
company_df17 = dataenrichment1.add_user(company_df16)
company_df18 = dataenrichment1.add_CH_link(company_df17)



## Clean the dataframe
company_df19 = dataframe_cleaning.clean_dataframe(company_df18)


with open('pickle1/companies_final_dataframe.pickle', 'wb') as f:
    pickle.dump(company_df19, f)
    
print("Final dataframe saved in pickle1")



## Leads

leads_df = leads.get_leads_df(company_df19, API_key)