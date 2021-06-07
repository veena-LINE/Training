import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.client('dynamodb')
    print("save events ", event)
    
    client = boto3.resource('dynamodb')
    
    table = client.Table("incidents")
    
    for record in event["Records"]:
        item_body = record["body"]
        message = json.loads(item_body)
        print("Writing to dynamo", message)
        
        table.put_item(Item= {'device_id': message["device_id"],'timestamp':  message["timestamp"], 'humidity': message["humidity"], 'temp':  message["temp"]})
            
