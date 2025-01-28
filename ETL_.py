import pandas as pd
from datetime import datetime
import requests
import certifi
import configparser



def get_url_data(url,key):
    headers = {
    'Authorization' : key
    }
    df= pd.DataFrame()
    data=''

    try:
        with requests.get(url, headers=headers) as response:
            data = response.json()
        response = requests.get(url, headers)
        print(response.text)
        #df = pd.DataFrame((response.json()))
    except Exception as e:
        print("Exception occured :",e)
        return df
    return df

def get_data():
    path = './transactional_data_KZN_assessment(1).csv'
    df = pd.read_csv(path)

    return df

def clean_data(df):
   # df.dropna(inplace = True)
    #df.drop_duplicates(inplace = True)
    #df['payload_id'] = df['payload_id'].dropna()
    
    
    df['qty'] = pd.to_numeric(df['qty'],downcast= 'integer',errors='coerce')
    print(df['qty'].dtype)

    df = df[df['qty'] > 10]
    
    return df

def categorize_send(spend_amount):
    caregory = ''

    if spend_amount < 50:
        caregory = 'low'
    elif spend_amount > 50 and spend_amount < 200:
        caregory = 'medium'
    else:
        caregory = 'high'
    return caregory

def load_data(data,table_name = ''):
    cnx = mysql.connector.connect(
    host=config['database']['host'],
    port=config['database']['port'],
    user= config['database']['user'],
    password=config['database']['password'])

    cur = cnx.cursor()

    return


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')

    KEY = "7fa0093b46msh83f1eb189c787b2p1221a9jsn2a036fa9b73"
    URL = 'https://8b1gektg00.execute-api.us-east-1.amazonaws.com/default/engineer-test' 

    DF = get_url_data(URL,KEY)


    #CLEAN_DF = clean_data(DF)
    #CLEAN_DF['Spending Category'] = CLEAN_DF['qty'].apply(categorize_send)

    #print(CLEAN_DF['Spending Category'].head)
