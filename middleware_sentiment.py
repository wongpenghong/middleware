# -*- coding: utf-8 -*-
import ast
import datetime
import json
import emoji
import re
import csv
from os import sys

import requests
from google.cloud import bigquery

with open("/root/middleware_sentiment/config/config_middleware_sentiment.json", "r") as read_file:
    conf = json.load(read_file)['middleware_sentiment_classification']

current_date_ds = sys.argv[1]
current_date_ds_nodash = sys.argv[2]
client = bigquery.Client(project=conf['project'])
#------------------------------------------------------------------------------------
def remove_emoji(text):
    return emoji.get_emoji_regexp().sub(u'', text)
#--------------------------------------------------------------------------------
def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]+',' ', text)
#---------------------------------------------------------------------------------
def remove_whitespace(text):
    return " ".join(text.split())
#----------------------------------------------------------------------------------
def sql_bq():
    query = ("""
        SELECT
            *
        FROM
            `{dataset}.{table}`
        WHERE
            DATE(_PARTITIONTIME) = '{current_date_ds}'
            AND BYTE_LENGTH(description) > 8
        """).format(
            dataset = conf['dataset'],
            table = conf['table'],
            current_date_ds = current_date_ds)
    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish
    
    print('sql_bq has success!!') 
    return rows

def query_sentiment_result():
    sql = sql_bq()
    array_desc = []
    
    for row in sql:
        data_no_ascii = remove_non_ascii(row[0])
        data_no_whitespace = remove_whitespace(data_no_ascii)
        array_desc.append(data_no_whitespace)
        
    body = {
        "text": array_desc
    }
    
    r = requests.post(conf['api_sentiment_inc'],
                        headers={"content-type": "application/json"},
                        data=json.dumps(body))
    response = r.content.decode('utf-8').replace("'",'"')
    result =json.loads(response)
    predictResult = result['predict']
    
    arrayResult = ast.literal_eval(predictResult)
    print("total rows : {total}".format(
        total = len(arrayResult)))
    return arrayResult

def sentiment_model():
    array_sentiment = query_sentiment_result()
    arrays = []
    for i in array_sentiment:
        sentiment_category = ['negative','neutral','positive']
        tmp = max(i[0],i[1],i[2])            
        if tmp == i[0]:
            sentiment = sentiment_category[0]
            sentiment_numerical = '0'
        elif tmp == i[1]:
            sentiment = sentiment_category[1]
            sentiment_numerical = '1'
        elif tmp == i[2]:
            sentiment = sentiment_category[2]
            sentiment_numerical = '2'
        arrays.append([sentiment,sentiment_numerical])
    print("print total sentiment list : {total} ".format(
        total = len(arrays)
    ))
    return arrays

def transform():
    desc = []
    created_at = []
    resource = []
    for row in sql_bq():
        data_remove_emoji = remove_emoji(row.description.rstrip().replace('\n', ' '))
        data_non_ascii = remove_non_ascii(data_remove_emoji)
        data_no_whitespace = remove_whitespace(data_non_ascii)
        
        desc.append(data_no_whitespace.lower())
        created_at.append(row.created_at.isoformat())
        resource.append(row.resource)
    
    sentiment = sentiment_model()
    list_first = []
    for i in sentiment:
        i.append(current_date_ds)
        list_first.append(i)
    merged_list = [[desc[j], list_first[j][0],list_first[j][1],list_first[j][2],resource[j].encode("utf-8"),created_at[j]] for j in range(0, len(desc))]
    
    print("total list data : {total} ".format(
        total = len(merged_list)
    ))
    
    return merged_list

def loading_to_csv():
    data = transform()
    with open('/root/middleware_sentiment/data/{dataset}_{table}_{current_date_ds_nodash}.csv'.format(
        dataset = conf['dataset'],
        table = conf['table'],
        current_date_ds_nodash = current_date_ds_nodash), 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)
    csvFile.close()
    print('success load into csv')

def main():
    loading_to_csv()
    
if __name__ == '__main__':
    main()