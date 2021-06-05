# `Use-case`

### Kinesis stream
Create `ds-order` kinesis stream
<br><br>

### dynamoDB
Create `order` table with `CountryID` as partition key.
<br><br>

### [Lambda: Test code](https://www.base64encode.org/)
```python
{
  "Records": [
    {
      "kinesis": {
        "data": "eyJPcmRlck5vIjogIjEyODkyNyIsICJDdXN0b21lcklEIjogIjM0NTU2IiwgIkNvdW50cnlJRCI6ICJJVCIsICJTdG9ja0NvZGUiOiAiODUxMjNBIiwgIlF1YW50aXR5IjogIjYiLCAiVW5pdFByaWNlIjogIjExLjI5IiwgIkRlc2NyaXB0aW9uIjogImxhdGVyLi4iLCAiT3JkZXJEYXRlIjogIjA2LzA1LzIwMjEgMDQ6NDgiLCAiUGF5bWVudE1vZGUiOiAiRG9nZWNvaW4ifQ=="
      }
    }
  ]
}
```
<br><br>

### Lambda: lambda_function.py
```python
import json
import boto3
import base64
from decimal import Decimal

def lambda_handler(event, context):
   print ("Event", event)
   client = boto3.resource("dynamodb")
   
   orderTable = client.Table("order")
   
   for record in event["Records"]:
       kinesis = record["kinesis"]
       encoded_payload = kinesis["data"]  # base64 string

       # json_payload is sent by kinesis producer
       json_payload = base64.b64decode(encoded_payload) # base64 to json string
       # print()
       # print("decoded ->", json_payload)

       order = json.loads(json_payload)  # load python object from json string

       order['Quantity'] = Decimal(order['Quantity'])
       order['UnitPrice'] = Decimal(order['UnitPrice'])
       order["Amount"] = order["Quantity"] * order["UnitPrice"]
       # print()
       # print("writing to dynamo ->", order)
       
       result = orderTable.put_item(Item=order)

       # print("result ->", result)
       # print()
```
Test code with print() statements enabled.
<br><br>

### Lambda: enable trigger
<br><br>

### bigdata/Kinesis-Order-Producer.ipynb
Run to generate a stream.
```python
import boto3
import random
import uuid
import datetime as dt
import json
import time

kinesis_stream = 'ds-order'

SAMPLES = 1000
DELAY = 1  # second

countries = (
    "USA", "CA", "IN", "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU",
    "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"
)
stock_codes = ('85123A', '71053', '84406B', '84406G', '84406E')
customer_codes = ('17850', '13047', '12583', '17850', '45545', '34556', '24462')
payment_modes = ('Cash', 'Check', 'Debit', 'Credit', 'Wire', 'Mobile', 'Bitcoin', 'Dogecoin')

kinesis_client = boto3.client('kinesis', region_name='us-east-2')


for i in range(SAMPLES):
    number_of_items = random.randint(4, 11)
    
    for j in range(number_of_items):  # MM/dd/yyyy hh:mm
        order = {
            "OrderNo": str(uuid.uuid4().fields[-1])[:6],
            "CustomerID": str(random.choice(customer_codes)),
            "CountryID": random.choice(countries),
            "StockCode": random.choice(stock_codes),
            "Quantity": str(random.randint(1, 10)),
            "UnitPrice": str(round( random.randint(1, 10) * 2 * random.random(), 2 )),
            "Description": "later..",
            "OrderDate": dt.datetime.now().strftime('%m/%d/%Y %H:%M') ,
            "PaymentMode": random.choice(payment_modes),
        }

        order_str = json.dumps(order)
        print(order_str)

        kinesis_client.put_record(
            StreamName=kinesis_stream,
            Data=order_str,
            PartitionKey=order["CountryID"]
        )
    time.sleep(DELAY)
```
Check dynamoDB.
