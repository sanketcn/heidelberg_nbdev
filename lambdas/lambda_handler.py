import json
import boto3
import os
import csv

STREAM_NAME=os.environ['STREAM_NAME']


def lambda_handler(event, context):
    print("Incoming Event: ", event);
    print("bucket ", context)
    
    # csvfile = s3.get_object(Bucket=bucket, Key=file_key)
    #csvcontent = csvfile['Body'].read().split(b'\n')
    #csv_data = csv.DictReader(csvcontent)
    
    records = event['Records']
    
    key=records['s3']['object']['key']
    bucket=records['s3']['bucket']['name']
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)

    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()

    lines = csv.reader(data)
    headers = next(lines)
    #print('headers: %s' %(headers))
    payloads = []
    # refernece:- https://stackoverflow.com/questions/56849240/how-to-read-csv-file-from-s3-bucket-in-aws-lambda
    for line in lines:
        #print complete line
        #print(line)
        #print index wise
        #print(line[0], line[1])
        j=0
        d={}
        for i in headers:
            if(i!="target"):
                d[i]=line[j]
            j=j+1

        payloads.append(d)
    
    for payload in payloads:
        put_to_stream(payload,time.time(),STREAM_NAME)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    
def put_to_stream(payload,timestamp,STREAM_NAME):
    ret_status = True
    data = json.dumps(payload)
    print(f'Sending a new payload: ', payload)
    response = kinesis_client.put_record(StreamName = STREAM_NAME ,
                                             Data = data,
                                             PartitionKey = 'shard1')
    
    if (response['ResponseMetadata']['HTTPStatusCode'] != 200):
        print("ERROR: Kinesis put_record failed: \n{}".format(json.dumps(response)))
        ret_status = False
        
    return ret_status



def get_cloudwatch_logs_url(start_time, end_time,predict_lambda_name):
    log_group_name = '/aws/lambda/' + predict_lambda_name 
    # get the latest log stream for our Lambda that makes fraud predictions
    cw_client = boto3.client('logs')
    last_cw_evt = 0
    while last_cw_evt < int(start_test_time * 1000):
        streams = cw_client.describe_log_streams(logGroupName=log_group_name,
                                                 orderBy='LastEventTime',
                                                 descending=True)['logStreams']
        last_cw_evt = streams[0]['lastIngestionTime'] #'lastEventTimestamp']
        latest_stream = str(streams[0]['logStreamName']).replace('/', '$252F').replace('[$LATEST]', '$255B$2524LATEST$255D')
        if last_cw_evt < int(start_test_time * 1000):
            print('waiting for updated log stream...')
            time.sleep(10)

    # produce a valid URL to get to that log stream
    region = boto3.session.Session().region_name
    log_group_escaped = log_group_name.replace('/', '$252F')
    cw_url = f'https://console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/{log_group_escaped}'
    time_filter = f'$26start$3D{int(start_test_time * 1000) - 10000}$26end$3D{int(end_test_time * 1000) + 40000}'
    full_cw_url = f'{cw_url}/log-events/{latest_stream}$3FfilterPattern$3DPrediction+{time_filter}'
    print('Updated log stream is ready.')
    return full_cw_url
