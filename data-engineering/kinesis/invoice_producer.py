import boto3
import json
from datetime import datetime,date
import calendar
import random as r
import time

my_stream_name = 'gk-data-stream'
kinesis_client = boto3.client('kinesis', region_name='us-east-2')

def put_to_stream(property_timestamp):
    countries=['US','IN',"CN","UK","RS"]
    payload = {
        "InvoiceNo":r.randint(10000,99999),
        "StockCode":str(r.randint(1000,9999)),
        "Description":"Product"+str(r.randint(1,100)),
        "Quantity":r.randint(1,10),
        "InvoiceDate":"2020-01-02",
        "UnitPrice":float(r.randint(1,1000)),
        "CustomerID":"CustID"+str(r.randint(100,999)),
        "Country":r.choice(countries),
    }
    print(payload)
    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=payload["Country"])

while True:
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    put_to_stream( property_timestamp)
    # wait for 5 second
    time.sleep(5)
