import json
import base64

def lambda_handler(event, context):
    output = []
    print("transform called ", event)
    #'result' either 'Ok' (succes bucket) or ProcessingFailed (failed bucket)
    # event["records"] consist of 60 seconds catpured data as list
    for record in event["records"]:
        print("record", record)
        # the data is encoded by producer using base64
        # we need to decode data back to normal json
        encoded_payload = record["data"]
        payload = base64.b64decode(encoded_payload)
        print("decoded ", payload)
        payload = json.loads(payload)
        
        payload["Amount"] = payload["UnitPrice"] * payload["Quantity"]
        
        # since now, no producer, and lambda is doing transformation, lambda should encode the json data
        json_payload = json.dumps(payload).encode("utf-8")
        encoded_payload = base64.b64encode(json_payload).decode("utf-8")
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok', # OK, success, it should go to success bucket
            'data': encoded_payload
        }
        
        output.append(output_record)
        
    return {'records': output}
