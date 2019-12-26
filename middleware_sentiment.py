import json
import ast
import ndjson
import pandas as pd
import os
import re
from os import sys
from google.cloud import bigquery
from datetime import datetime
import requests

with open("/root/middleware_sentiment/config/config_etl.json", "r") as read_file:
    CONFIG = json.load(read_file)['config_etl_feedback']
    
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CONFIG['service_account']

class middleware:
    def __init__(self):
        self.project = CONFIG['project']
        self.dataset = CONFIG['dataset']
        self.table = CONFIG['table']
        self.table_destination = CONFIG['table_destination']
        # self.date_nodash = date_nodash
        self.date = sys.argv[1]
        self.date_nodash = sys.argv[2]
        self.path = '{path_data}/digital_listening_Sentiment_{date_nodash}.json'.format(path_data=CONFIG['file_data'],date_nodash=self.date_nodash)
        self.client = bigquery.Client(project=CONFIG['project'])
    
    def load_data_to_bq(self): 
        """
        loads or initalizes the job in BQ
        """
        # for newest data
        bigquery_client = bigquery.Client(CONFIG['project'])
        # print(bigquery_client)
        destination_dataset_ref = bigquery_client.dataset(CONFIG['dataset'])
        destination_table_ref = destination_dataset_ref.table(self.table_destination + '$' + self.date_nodash)
        job_config = bigquery.LoadJobConfig()
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        #using partition by ingestion Time
        job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
        
        with open(self.path, 'rb') as f:
            job = bigquery_client.load_table_from_file(f, destination_table_ref, job_config=job_config)
            job.result()
            print('----->>>> '+self.path+' has success to load!')
            os.remove(self.path)
        
    def transform_text(self,data):
        # transform value
        data = re.sub(r'[^\x00-\x7F]+','', data)
        data = "".join(data.split("\\n"))
        return data
    
    def sentiment_model(self,data):
        """
        Classification Sentiment category and Sentiment Numerical
        """
        arrays = []
        for i in data:
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
            
        return arrays
            
    def get_sentiment(self,df):
        """
        initial hit api model sentiment analysis
        """
        # get column desription
        data_review = df['description']
        # dump to json string
        data = data_review.to_json(orient='records', force_ascii=False)
        # Transformasi data text
        data = self.transform_text(data)
        # Loads to JSON
        json_data = json.loads(data)
        
        # formatting body api sentiment model
        body = {
            "text": json_data
        }
        # Hit api sentiment Model
        resultApi = requests.post(CONFIG["api_sentiment_inc"],
                    headers={"content-type": "application/json"},
                    data=json.dumps(body))
        
        response = resultApi.content.decode('utf-8').replace("'",'"')
        # load to json
        result =json.loads(response)
        # Type date become Str
        predictResult = result['predict']
        # Load to List
        arrayResult = ast.literal_eval(predictResult)
        sentiment = self.sentiment_model(arrayResult)
        
        return sentiment
    
    def get_data(self,verbose=1):
        """
        Get data from some particular table in BigQuery
        """
        
        # For checking how many time this entire process
        if verbose:
            print('executing Process....')
            start = datetime.now()
        
        # Query for get data from dataLake
        query = ("""
        SELECT
            description,resource,created_at
        FROM
            `{dataset}.{table}`
        WHERE
            created_at = '{date}'
            AND BYTE_LENGTH(description) > 8
        """).format(
            dataset = self.dataset,
            table = self.table,
            date = self.date)

        # Save to DataFrame Mobile
        df = self.client.query(query).to_dataframe()
        print('------->>>>> success get dataFrame')
        
        # Get sentiment data
        sentiment = self.get_sentiment(df)
        df_sentiment = pd.DataFrame(sentiment,columns = ['sentiment','sentiment_numerical'])
        df_sentiment['created_date'] = self.date
        print('------->>>>> success get Sentiment')
        
        # transform column DataFrame
        df_final = pd.concat([df,df_sentiment],axis=1)
        print('------->>>>> success Transform column DataFrame')
        
        # Transform dateType to read at json dump
        df_final['created_at'] = pd.to_datetime(df_final['created_at'], format='%Y-%m-%d')
        df_final['created_at'] = df_final['created_at'].dt.strftime('%Y-%m-%d')

        df_final['created_date'] = pd.to_datetime(df_final['created_date'], format='%Y-%m-%d')
        df_final['created_date'] = df_final['created_date'].dt.strftime('%Y-%m-%d')

        data = df_final.to_json(orient='records', force_ascii=False)
        
        # Dump to Json File
        json_data = json.loads(data)
        with open(self.path, 'w') as f:
            ndjson.dump(json_data,f) 
      
        print('------->>>>> success Dump data to Json File')

        # Load Data To BigQuery
        self.load_data_to_bq()
  
        print('------->>>>> success Load Data')
        
        if verbose:
            print('Process executed in ' + str(datetime.now() - start))
   
        return df