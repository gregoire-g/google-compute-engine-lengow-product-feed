#!/usr/bin/env python
# coding: utf-8

import urllib.request as urllib2 
import datetime as dt
import time
from datetime import datetime, timedelta
import calendar
from lxml import html
import requests
import traceback
import urllib3
import xmltodict
import pandas as pd

 
def getxml():
    url = "https://httpnas.lengow.io/FluxArmaniBeautyIT/IN/feed-lengow-giorgioarmani-eu-FR_FR.xml"
    import urllib3
    import xmltodict


    http = urllib3.PoolManager()

    response = http.request('GET', url)
    try:
        data = xmltodict.parse(response.data)
    except:
        print("Failed to parse xml from response (%s)" % traceback.format_exc())
    return data


data = getxml()
df = pd.DataFrame.from_dict(data['channel']['item'])

column_name = list(df.columns.values)
column_name = [x.replace('-', '_') for x in column_name]
df.columns = column_name

for x in column_name:
    df[x] = df[x].astype('str')
    print('Column ' + x + ' converted to string')


def date_parameter():
    today = dt.date.today() - timedelta(days=1)
    last_month = dt.date.today() - timedelta(days=30)
    bq_table_date = pd.to_datetime(dt.datetime(today.year, today.month, today.day)).strftime('%Y%m%d')
    month = calendar.month_name[last_month.month]
    year = str(last_month.year) + "_"
    bq_table_name = 'gio_lengow_flow_export_' + bq_table_date

    return bq_table_name


if __name__ == '__main__':
    bq_table_name = date_parameter()
    table_to_load = df
    table_to_load.to_gbq('lengow_feed.' + bq_table_name, 'loreal-france-luxe',
                    chunksize=None,
                    if_exists='replace',
                    verbose=False)
