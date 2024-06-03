# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
#Extracts information from website

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df):
    #This function converts the GDP information from Currency
    #format to float value, transforms the information of GDP from
    #USD (Millions) to USD (Billions) rounding to 2 decimal places.

    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    
    return df

def load_to_csv(df, csv_path):
    # This function saves the final dataframe as a `CSV` file in the provided path. Function returns nothing.
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    # This function saves the final dataframe to as a database table with the provided name. Function returns nothing.
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)