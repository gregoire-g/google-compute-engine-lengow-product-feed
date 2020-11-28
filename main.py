from __future__ import absolute_import

import pandas as pd
import numpy as np
#import google.datalab.bigquery as bq
pd.set_option('display.max_columns', None)
from datetime import timedelta
import calendar as cal
import datetime as dt

from google.auth import compute_engine
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import Dataset

#Step 1 -> import lengow flow and define a unique SKU value based on initial EAN + categories
def import_feed(flow):
    df = pd.read_csv(flow, delimiter='|',encoding='ISO-8859-1')
    df = df[['id', 'title', 'description', 'product_type', 'link', 'image_link', 'condition','availability', 'price', 'brand']]

    #---- Define the number of occurence '>' to split the column
    try:
        spe_df = df['product_type'].str.count('>').max().astype(int)

        if spe_df == 1:
            df[['category', 'category_2']] = df['product_type'].str.split('>', expand=True)
        elif spe_df == 2:
            df[['category', 'category_2', 'category_3']] = df['product_type'].str.split('>', expand=True)
        elif spe_df == 3:
            df[['category', 'category_2', 'category_3', 'category_4']] = df['product_type'].str.split('>', expand=True)
        elif spe_df > 3:
            print('fuck it...')
        else:
            df['category'] = df['product_type']
    except:
        spe_df = 0
        df['category'] = df['product_type']

    #---- Cleaning and formating
    df = df.drop(['product_type'], axis=1)
    df['id'] = df['id'].astype(str)
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    if df['brand'].str.count('Urban Decay').max().astype(int) > 0 :
        pass
    elif df['brand'].str.count('BIOTHERM').max().astype(int) > 0 :
        pass
    else:
        df['price'] = df['price'].map(lambda x: x.rstrip('EUR'))
    df['price'] = df['price'].astype(float)
    df = df.replace(np.nan, '', regex=True)

    #---- define the id_sku based on the number of occurence ('>')
    if spe_df == 1:
        df['id_sku'] = df['id'].str[:8] + df['category'].str[:1] + df['category_2'].str[:1]
        df = df[['id_sku', 'id', 'title', 'description', 'condition', 'price', 'category', 'category_2', 'link', 'image_link']]
    elif spe_df == 2:
        df['id_sku'] = df['id'].str[:8] + df['category'].str[:1] + df['category_2'].str[:1] + df['category_3'].str[:1]
        df = df[['id_sku', 'id', 'title', 'description', 'condition', 'price', 'category', 'category_2', 'category_3', 'link', 'image_link']]
    elif spe_df == 3:
        df['id_sku'] = df['id'].str[:8] + df['category'].str[:1] + df['category_2'].str[:1] + df['category_3'].str[:1]
        df = df[['id_sku', 'id', 'title', 'description', 'condition', 'price', 'category', 'category_2', 'category_3', 'category_4', 'link', 'image_link']]
    elif spe_df > 3:
        print('fuck it...')
    else:
        df['id_sku'] = df['id'].str[:8] + df['category'].str[:1]
        df = df[['id_sku', 'id', 'title', 'description', 'condition', 'price', 'category', 'link', 'image_link']]

    print "...%s lines cleaned so far" % (len(df))

    return df

#Step 2 -> import the new flow in BQ
def bq_import(dataframe, brand):
    #------ Define the table name in BQ
    date = dt.date.today() - timedelta(days=30)
    week = dt.datetime(date.year, date.month, cal.mdays[date.month]).strftime('%Y%m%d')
    file_name = brand + '_lengow_flow_export'

    #---- Exporting to Bigquery Python version
    dataframe.to_gbq('lengow_feed.' + file_name, 'loreal-france-luxe',
    chunksize=None,
    if_exists='replace',
    verbose=False,
    table_schema=None)

    print "...%s lines uploaded" % (len(dataframe))


lcm = 'http://flux.lengow.com/shopbot/google-shopping-online-products-display_ads_link/LGW-d2d80591af8903ec7043ced2b44951e2/csv/'
ysl = 'http://flux.lengow.com/shopbot/google-shopping/LGW-35e99950ce31b0e2ca304d5ef57a0769/csv/'
bio = 'http://flux.lengow.com/shopbot/google-shopping-online-products-fr/LGW-6c681a6ca42ba87f4490b22d055e0843/csv/ '
ud = 'http://flux.lengow.com/shopbot/google-shopping-online-products-fr/LGW-93459aed492275c8343ba4b9409941d2/csv/'
kie = 'http://flux.lengow.com/shopbot/google-shopping-online-products-fr/LGW-c287c08633ea0acefd0d9e26441f155f/csv/'

if __name__ == '__main__':
    frames = ['bio', 'ysl', 'lcm', 'ud', 'kie']
    feeds = [bio, ysl, lcm, ud, kie]
    for x, y in zip(frames, feeds):
        print "Product flow: %s" % x
        z = import_feed(y)
        bq_import(z, x)
