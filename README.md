# Heidelberg-Cement-Project





### Overview

In this repository, we provide artifacts that demonstrate how to leverage the Amazon SageMaker Feature Store using streaming feature aggregation.
We use Amazon SageMaker to train a model (using the built-in XGBoost algorithm) with aggregate features. 
We use streaming aggregation with Amazon Kinesis and Amazon Kinesis Data Analytics (KDA) SQL, publishing features in near real time to the feature store.
Finally, we pull the latest aggregate feature values from the feature store at inference time, passing them as input to our model hosted in an Amazon SageMaker endpoint.





Amazon SageMaker Feature Store is a purpose-built repository where you can store and access features so it’s much easier to name, organize, and reuse them across teams. 
SageMaker Feature Store provides a unified store for features during training and real-time inference without the need to write additional code or create manual processes to keep features consistent.


### This implementation shows you how to do the following:

1. Create multiple SageMaker Feature Groups to store aggregate data from a dataset.
1. Train a SageMaker XGBoost model and deploy it as an endpoint for real time inference.
1. Generate simulated data sending them to Amazon Kinesis.
1. Use KDA SQL to aggregate features in near real time, triggering a Lambda function to update feature values in an feature group.
1. Trigger a Lambda function to invoke the SageMaker endpoint and detect fraudulent transactions.
1. There is one more lambda function which is conneced to s3bucket when anything is uploaded to s3bucket this lambda function will trigger and the data which is added to s3 will be put in stream and then the same procedure is going to follow.

### Prerequisites

Prior to running the steps under Instructions, you will need access to an AWS Account where you have full Admin privileges. The CloudFormation template will deploy multiple AWS Lambda functions, IAM Roles, and a new SageMaker notebook instance with this repo already cloned. In addition, having basic knowledge of the following services will be valuable: Amazon Kinesis streams, Amazon Kinesis Data Analytics, Amazon SageMaker, AWS Lambda functions, Amazon IAM Roles.


### Instructions
First you will login to your AWS account with an Admin user or role. This will allow the successful launch of the CloudFormation stack template. You can deploy the stack and explore our example by following these steps:

1. AWS CloudFormation stack is formed in us-east-1
1. View the Kinesis Stream that is used to ingest records.
1. View the Kinesis Data Analytics SQL query that pulls data from the stream.
1. View the Lambda function that receives the initial kinesis events and writes to the FeatureStore.
1. View the Lambda function that receives the final kinesis events and triggers the model prediction.
1. View the Lambda function that receives the new data which is updating in s3


To use these notebooks from SageMaker Studio,Use "sanket-heidelberg" studio only beacuse the stack formed should match the IAM Role(Sagemaker role)

Running the Notebooks

There are a series of notebooks which should be run in order. Follow the step-by-step guide in each notebook:

1. 0_data_prepare.ipynb -> Prepare your data and apply all the transformation here.
1. 1_setup.ipynb -> create feature groups and Kinesis resources ,  igest one-week aggregate features, and create training dataset , train and deploy fraud detection model
1. 2_inference.ipynb -> make predictions on streaming transactions
1. 3_cleanup.ipynb -> clean up resources
1. 4_what_if_tool.ipynb -> what if tool analysis(currently not working)



## DEPLOY LINKS 


### Lambda functions

1. InvokeFraudEndpoint (https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/HeidelbergInvokeFraudEndpointLambda3?tab=configuration)
1. StreamingInjectAggregateFeature( https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/HeidelbergStreamingIngestAggFeatures3?tab=configuration)
1. Lambdatoputincomingdata ( https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/lambda_mlops?tab=configuration)

### Template (CLoudFormation Stack)

https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/template?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false&stackId=arn%3Aaws%3Acloudformation%3Aus-east-1%3A082830052325%3Astack%2Fheidelberg-mlops-stack-3%2Ffc283870-49d8-11eb-b6cb-0ea6aa6eaed9

### LogStream (CloudWatch)

#### ENd point logstream,
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252FHeidelbergInvokeFraudEndpointLambda3

#### Update FeatureStore Logstream
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252FHeidelbergStreamingIngestAggFeatures3

#### Streaming lambda
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252Flambda_mlops


### KDA Application
https://console.aws.amazon.com/kinesisanalytics/home?region=us-east-1#/wizard/hub?applicationName=heidelberg-mlops-app


### Real time lambda update s3 bucket(anything upload on thie will trigger lambda and the project will run)
https://s3.console.aws.amazon.com/s3/buckets/lambda-trigger-mlops?region=us-east-1&tab=objects


### Transformed Data of Heidelberg
https://s3.console.aws.amazon.com/s3/buckets/sagemaker-us-east-1-082830052325?region=us-east-1&prefix=heidelberg/data/&showversions=false


#### Anything uploaded on this s3 bucket https://s3.console.aws.amazon.com/s3/buckets/lambda-trigger-mlops?region=us-east-1&tab=objects will go to the stream and will also trigger invokelambda and also ddata will go to KDA for freaturestore
