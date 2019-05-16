import requests
import json
import ast
from google.cloud import bigquery
import datetime
    

with open("config_middleware_sentiment.json", "r") as read_file:
    conf = json.load(read_file)['middleware_sentiment_classification']
client = bigquery.Client(project=conf['project'])

#------------------------------------------------------------------------------------

def export_items_to_bigquery(object):
    # Instantiates a client
    now = datetime.datetime.now()
    datenow = now.strftime("%Y%m%d")
    # Prepares a reference to the dataset
    dataset_ref = client.dataset(conf['destination_dataset'])

    table_ref = dataset_ref.table(conf['destination_table']+'$'+ datenow)
    table = client.get_table(table_ref)  # API call
    
    errors = client.insert_rows(table, object)  # API request
    assert errors == []

#----------------------------------------------------------------------------------
def sql_bq():
    query = (
    "SELECT * FROM `{dataset}.{table}`"
    " WHERE DATE(_PARTITIONTIME) = '2017-04-03'").format(
        dataset = conf['dataset'],
        table = conf['table'])
    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish
    
    return rows

def query_sentiment_result():
    sql = sql_bq()
    array_sentiment = []
    array_desc = []
    for row in sql:
        body = {
        "text": [
            row[0]
        ]
        }
        r = requests.post(conf['api_sentiment_inc'],
                        headers={"content-type": "application/json"},
                        data=json.dumps(body))
        
        response = r.content
        responseJson = response.decode('utf-8').replace("'",'"')
        result =json.loads(responseJson)
        predictResult = result['predict']
        arrayResult = ast.literal_eval(predictResult)[0]
        array_sentiment.append(arrayResult)
        
    return array_sentiment

  

def query_classification_result():
    sql = sql_bq()
    array_classification = []
    for row in sql:
        body = {
        "text": [
            row[0]
        ]
        }
        r = requests.post(conf['api_classification_inc'],
                        headers={"content-type": "application/json"},
                        data=json.dumps(body))
        response = r.content
        responseJson = response.decode('utf-8').replace("'",'"')
        result =json.loads(responseJson)
        predictResult = result['predict']
        arrayResult = ast.literal_eval(predictResult)[0]
        array_classification.append(arrayResult)
    return array_classification

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
    return arrays
    

def classification_model():
    array_classification = query_classification_result()
    arrays = []
    
    for i in array_classification:
        class_names = ['Others', 'Loan Application', 'Brand / Corporate','Technical','Contract', 'Second Loan',
                'Suggestion']
        tmp = max(i[0],i[1],i[2],i[3],i[4],i[5],i[6])
        if tmp == i[0]:
            class_names = class_names[0]
            classification_numerical = '0'
        elif tmp == i[1]:
            class_names = class_names[1]
            classification_numerical = '1'
        elif tmp == i[2]:
            class_names = class_names[2]
            classification_numerical = '2'
        elif tmp == i[3]:
            class_names = class_names[3]
            classification_numerical = '3'
        elif tmp == i[4]:
            class_names = class_names[4]
            classification_numerical = '4'
        elif tmp == i[5]:
            class_names = class_names[5]
            classification_numerical = '5'
        elif tmp == i[6]:
            class_names = class_names[6]
            classification_numerical = '6'
        arrays.append([class_names,classification_numerical])
    return arrays
   
    
def transform():
    desc = []
    created_at = []
    resource = []
    for row in sql_bq():
        desc.append(row.description.encode("utf-8"))
        created_at.append(row.created_at.isoformat())
        resource.append(row.resource.encode("utf-8"))
    
    sentiment = sentiment_model()
    classification = classification_model()
    mergeSA = list(map(list.__add__,sentiment,classification))
    
    now = datetime.datetime.now()
    datenow = now.strftime("%Y-%m-%d")
    
    list_first = []
    for i in mergeSA:
        i.append(datenow)
        list_first.append(i)
    
    merged_list = [[desc[j], list_first[j][0],list_first[j][1],list_first[j][2],list_first[j][3],list_first[j][4],resource[j],created_at[j]] for j in range(0, len(desc))]
    arr_final = []
    for k in merged_list:
        arr_final.append(tuple(k))
    
    return arr_final
    
    
def main():
    #print(transform())
    export_items_to_bigquery(transform())
    
if __name__ == '__main__':
    main()
