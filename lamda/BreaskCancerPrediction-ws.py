Samplimport os
import io
import boto3
import json
import csv

# grab environment variables
# connection URL (i.e. backend URL)
URL = os.environ['ConnectionUrl']
gatewayapi = boto3.client("apigatewaymanagementapi",
        endpoint_url = URL)
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime= boto3.client('runtime.sagemaker')

def lambda_handler(event, context):
    print(ENDPOINT_NAME)
    print(URL)
    connectionId=event["requestContext"].get("connectionId")
    print("Received event: " + json.dumps(event, indent=2))
    
    data = json.loads(event["body"])
    payload = data['data']
    print(payload)
    
    print(connectionId)
    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       ContentType='text/csv',
                                       Body=payload)
    print(response)
    result = json.loads(response['Body'].read().decode())
    print(result)
    #print('JRP')
    pred = (result['predictions'][0]['score'] > 0.5)+0
    #pred = int(result['predictions'][0]['score'])
    
    #print(pred)
    predicted_label = 'Malignant' if pred == 1 else 'Benign'
    score = result['predictions'][0]['score'] 
    print(predicted_label)
    print(score)
    #return result
    #return pred
    #return predicted_label, result['predictions'][0]['score'] 
    #return predicted_label
    post_message(connectionId, predicted_label, score)
    return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Message does not exist!'})
    }
    
def post_message(connectionId, prediction, score):
    gateway_resp = gatewayapi.post_to_connection(ConnectionId=connectionId,
                                                 Data=json.dumps({"re": prediction, "score": score}))