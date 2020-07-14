from __future__ import unicode_literals
import logging
import json
import os
import boto3

logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)
URL = os.environ['ConnectionUrl']
gatewayapi = boto3.client("apigatewaymanagementapi",endpoint_url = URL)
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime= boto3.client('runtime.sagemaker')

class PythonObjectEncoder(json.JSONEncoder):
    """Custom JSON Encoder that allows encoding of un-serializable objects
    For object types which the json module cannot natively serialize, if the
    object type has a __repr__ method, serialize that string instead.
    Usage:
        >>> example_unserializable_object = {'example': set([1,2,3])}
        >>> print(json.dumps(example_unserializable_object,
                             cls=PythonObjectEncoder))
        {"example": "set([1, 2, 3])"}
    """

    def default(self, obj):
        if isinstance(obj, (list, dict, str, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)
        elif hasattr(obj, '__repr__'):
            return obj.__repr__()
        else:
            return json.JSONEncoder.default(self, obj.__repr__())



                                                 
def lambda_handler(event, context):
    logger.info('Event: %s' % json.dumps(event))
    logger.info('Context: %s' % json.dumps(vars(context), cls=PythonObjectEncoder))
    logger.info('Endpoint Name: %s' % ENDPOINT_NAME)
    logger.info('URL: %s' % URL)
    connectionId=event["requestContext"].get("connectionId")
    logger.info('Received event: %s' % json.dumps(event, indent=2))
    msg_body = event['body']
    msg_dict = eval(msg_body)
    logger.info('Recieved msg_dict type: %s ' % type(msg_dict))
    logger.info('Received msg_dict: %s ' % msg_dict)
    row_data = msg_dict.get("data")
    logger.info('Recieved row_data type: %s ' % type(row_data))
    logger.info('Received row_data: %s ' % row_data)
    return_data = []
    for dic in row_data:
        for list in dic.values():
            for dic2 in list:
                for val in dic2.values():
                  payload = val
                  logger.info('Payload: %s ' % payload)
                  response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       ContentType='text/csv',
                                       Body=payload)
                  result = json.loads(response['Body'].read().decode())
                  score = str(round(result['predictions'][0]['score'],4)) 
                  post_message(connectionId, score)
    logger.info('Current Session ConnectionId %s ' % connectionId)
    #post_message(connectionId, payload)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Message does not exist!'})
    }

def post_message(connectionId, result):
    gateway_resp = gatewayapi.post_to_connection(ConnectionId=connectionId,
                                                 Data=json.dumps({"result": result}))
