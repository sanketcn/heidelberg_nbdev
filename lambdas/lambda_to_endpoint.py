import os
import json
import base64
import logging
import time
from datetime import datetime


# Read environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
FEATURE_GROUP = os.environ['FEATURE_GROUP_NAME']
FRAUD_THRESHOLD = os.environ['FRAUD_THRESHOLD']
LOG_LEVEL = os.environ['LOG_LEVEL']

logger = logging.getLogger()
if logging._checkLevel(LOG_LEVEL):
    logger.setLevel(LOG_LEVEL)
else:
    logger.setLevel(logging.INFO)

logging.info(f'Setting Logger Level to {logging.getLevelName(logger.level)}')

import boto3

print(f'boto3 version: {boto3.__version__}')
# Create session via Boto3
session = boto3.session.Session()

try:
    featurestore_runtime = boto3.Session().client(service_name='sagemaker-featurestore-runtime')
except:
    logging.error('Failed to instantiate featurestore-runtime client with install.sh script!')

# Allocate SageMaker runtime
sagemaker_runtime = boto3.client('runtime.sagemaker')

logging.info(f'Lambda will call SageMaker ENDPOINT name: {ENDPOINT_NAME}')


def lambda_handler(event, context):
    """ This handler is triggered by incoming Kinesis events,
    which contain a payload encapsulating the transaction data.
    The Lambda will then lookup corresponding records in the
    aggregate feature groups, assemble a payload for inference,
    and call the inference endpoint to generate a prediction.
    """
    logging.debug('Received event: {}'.format(json.dumps(event, indent=2)))

    records = event['Records']
    logging.debug('Event contains {} records'.format(len(records)))
    
    ret_records = []
    for rec in records:
        # Each record has separate eventID, etc.
        event_id = rec['eventID']
        event_source_arn = rec['eventSourceARN']
        logging.debug(f'eventID: {event_id}, eventSourceARN: {event_source_arn}')

        kinesis = rec['kinesis']
        event_payload = decode_payload(kinesis['data'])


        feature_string = assemble_features(event_payload)
        prediction = invoke_endpoint(feature_string)
        
        if prediction is not None:
            sequence_num = kinesis['sequenceNumber']
            ret_records.append({'eventId': event_id,
                            'sequenceNumber': sequence_num,
                            'prediction': prediction,
                            'statusCode': 200})

    return ret_records
                       
def assemble_features( event_payload ):
    inference_features = []
    features = []
    for feature in features:
        inference_features.append(str(event_payload[feature]))

    logging.debug(f'Inference features: {inference_features}')

    # assemble features into CSV-format string
    feature_string = ','.join(inference_features)

    return feature_string


def invoke_endpoint(request_body):
    logging.debug('Passing Request Body (CSV-format): {}'.format(request_body))
    response = sagemaker_runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType='text/csv',
        Body=request_body)        
    logging.info('Inference Response: {}'.format(response))

    probability = json.loads(response['Body'].read().decode('utf-8'))
    return probability


def decode_payload(event_data):
    agg_data_bytes = base64.b64decode(event_data)
    decoded_data = agg_data_bytes.decode(encoding="utf-8") 
    event_payload = json.loads(decoded_data) 
    logging.info(f'Decoded data from kinesis record: {event_payload}')
    return event_payload