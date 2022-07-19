import pandas as pd
import requests
import time
import re
import pickle
from tqdm import tqdm
from lxml import html
import numpy as np

def clean_dataframe(dataframe):
    dataframe = dataframe.drop(["Address 1", "Address 2", "Area"], axis=1)
    return dataframe
    
def final_processing(companies_with_phone):

#     companies_with_phone.columns = ['Company name', 'City', 'Postcode', 'SIC codes', 'Industry description',
#        'Company_Id', 'Incorp_date', 'Company_address', 'Country',
#        'Num_of_employees', 'Net worth', 'Financial size', 'Linkedin',
#        'Importer/Exporter', 'Last operation month', 'Company Description',
#        'Telephone number', 'Scores', 'Status', 'Pitch', 'User', 'CH_link']

    companies_with_phone['Scores'] = companies_with_phone['Scores'].apply(str)
    companies_with_phone['Pitch'] = companies_with_phone['Pitch'].apply(str)
    companies_with_phone['User'] = companies_with_phone['User'].apply(str)
    companies_with_phone['CH_link'] = companies_with_phone['CH_link'].apply(str)
    
    with open('pickle1/companies_final_dataframe.pickle', 'wb') as f:
        pickle.dump(companies_with_phone, f)
    
    return companies_with_phone