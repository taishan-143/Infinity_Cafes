import boto3
import csv
import json

def start(event, context):
    data_list = []
    s3 = boto3.resource('s3')
    #for record in event['Records]:-if mutiple files within one event
    bucketname = event['Records'][0]['s3']['bucket']['name']
    print(f"{bucketname}")
    sourceKey = event['Records'][0]['s3']['object']['key']
    print(f"{sourceKey}")
    obj = s3.Object(bucketname, sourceKey)
    data = obj.get()['Body'].read().decode('utf-8').splitlines()
    print(json.dumps({"dataType": f"{type(data)}", "contents": f"{data}"}))
    print("Extracted")
    lines = csv.reader(data)
    for line in lines:
        data_list.append(line)
    print(json.dumps({"dataType": f"{type(data_list)}", "contents": f"{data_list}"}))
    print("SUCCESS")
    return data_list