import json
import ndjson
import pandas as pd
import os
from os import sys
from google.cloud import bigquery
from datetime import datetime

with open("/root/middleware_sentiment/config/config_etl.json", "r") as read_file:
    CONFIG = json.load(read_file)['config_etl_feedback']
    
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CONFIG['service_account']

class etl_feedback:
    def __init__(self):
        self.project = CONFIG['project']
        self.dataset = CONFIG['dataset']
        self.table_mobile = CONFIG['table_mobile']
        self.dataset_mobile = CONFIG['dataset_mobile']
        self.table_website = CONFIG['table_website']
        self.table = CONFIG['table']
        # self.date_nodash = date_nodash
        self.date = sys.argv[1]
        self.date_nodash = sys.argv[2]
        self.path = '{path_data}/etl_feedback_{date_nodash}.json'.format(path_data=CONFIG['file_data'],date_nodash=self.date_nodash)
        self.client = bigquery.Client(project=CONFIG['project'])
  
    def load_data_to_bq(self): 
        """
        loads or initalizes the job in BQ
        """
        # for newest data
        bigquery_client = bigquery.Client(CONFIG['project'])
        # print(bigquery_client)
        destination_dataset_ref = bigquery_client.dataset(CONFIG['dataset'])
        destination_table_ref = destination_dataset_ref.table(self.table + '$' + self.date_nodash)
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
    
    def get_data(self,verbose=1):
        """
        Get data from some particular table in BigQuery
        """
        
        # For checking how many time this entire process
        if verbose:
            print('executing Process....')
            start = datetime.now()
        
        # Query for get data from Playstore
        query_mobile = (
            """
            SELECT
                Review_Text AS description,
                DATE(Review_Submit_Date_and_Time) AS created_at,
                'mobile' AS resource
            FROM
                `{project}.{dataset_mobile}.{table_mobile}`
            WHERE
                DATE(_PARTITIONTIME) = '{date}' and Review_Text is not null """.format(project=CONFIG['project'],dataset_mobile=self.dataset_mobile,table_mobile=self.table_mobile,date=self.date))

        # Save to DataFrame Mobile
        df_mobile = self.client.query(query_mobile).to_dataframe()
        print('------->>>>> success get data mobile')
        
        # Query for get data from website
        query_website = (
            """
            SELECT
                description AS description,
                DATE(created_at) AS created_at,
                'website' AS resource
            FROM
                `{project}.{dataset_website}.{table_website}`
            WHERE
                DATE(_PARTITIONTIME) = '{date}' and description is not null
            """.format(project=CONFIG['project'],
                       dataset_website=self.dataset,
                       table_website=self.table_website,
                       date=self.date)
        )

        # Save to DataFrame Mobile
        df_website = self.client.query(query_website).to_dataframe()
        print('------->>>>> success get data website')
  
        df_final = pd.concat([df_mobile,df_website])
        print('------->>>>> success Transform table')

        # Transform dateType to read at json dump
        df_final['created_at'] = pd.to_datetime(df_final['created_at'], format='%Y-%m-%d')
        df_final['created_at'] = df_final['created_at'].dt.strftime('%Y-%m-%d')

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