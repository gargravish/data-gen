from faker import Faker
import pandas as pd
import datetime
from pandas.io import gbq
import time

fake=Faker()
Faker.seed(0)

def data_gen():
        
    lat_lng = []
    gb_lat = []
    gb_lng = []
    txn_date = []
    merchant_id = []
    txn_id = []
    amount = []

    for i in range(10):
        lat_lng.append(fake.local_latlng(country_code='GB'))
        gb_lat.append(lat_lng[i][0])
        gb_lng.append(lat_lng[i][1])
        txn_date.append(fake.date_time_between(start_date='-400d', end_date='now',tzinfo=None).isoformat())
        txn_id.append(fake.ean(length=13))
        amount.append(fake.random_number(digits=3, fix_len=False))
        merchant_id.append(fake.random_int(min=1, max=9))

    final_list = list(zip(txn_date,txn_id,merchant_id,amount,gb_lat,gb_lng))
    final_df = pd.DataFrame(final_list,columns=['Txn_Date','Txn_ID','Merchant_ID','Amount','Latitude','Longitude'])
    final_df['Txn_ID'] = pd.to_numeric(final_df['Txn_ID'])
    final_df['Txn_Date'] = pd.to_datetime(final_df['Txn_Date'])
    return final_df

def write_to_bq(final_df):
    final_df.to_gbq(destination_table='raves.user_transactions',project_id='raves-altostrat',if_exists='append')

if __name__ == "__main__":
    n = int(input("Enter the number of transactions (in a set of 10): "))
    for i in range(n):
        df = data_gen()
        print(df)
        print('Uploading to BigQuery')
        write_to_bq(df)
        print('Done!')
        print('Sleeping ',(i+1),' out of ',n)
        time.sleep(10)