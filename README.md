# aws-lambda-based-etl
Process individual files and perform data normalization: s3->secrets manager->RDS & s3

**Use Case:** Creating a serverless data pipeline using AWS Lambda to minimize overhead managing infrastructure and have code invoked only when new files arrive. This serves as a template starting point. Not all scripts may meet your use case. 

**Technical Concept:** A lambda function is triggered when a new text file arrives in a s3 bucket. The file is checked for errors, parses the file name and concatenates it to the data, accesses secrets manager for Aurora PgSQL database credentials, exports normalized text "|" delimited file to s3 bucket, and inserts records into database with lambda function metadata for ETL transformation success/failure. Database and lambda function contained in the same VPC. Assumes VPC is configured to have lambda connect with Secrets Manager for http request. This demo works without a VPC.

**Prerequisites:**
* You have an AWS user account
* You have an IAM role that can access S3, Secrets Manager, RDS, VPC
* You have two s3 buckets ready for use
* You have a Aurora PgSQL database setup with empty table schema setup
* You have Secrets Manager setup storing database credentials
* AWS SDK locally installed for AWS CLI use
* You're familiar with Python
* A desire to learn
* Note: shell scripting based on Windows cmd terminal(I know...I know...)

**Deploy Instructions:** This will be done using the AWS CLI
* Download folder structure in this repository to local desktop(should be in zip form)
* See example shell script below
* Update "region"
* Update "function-name"
* Update "zip-file fileb://" with the file path of downloaded zip file
* Update "role" with IAM role
* Update "environment Variables"
* Copy and paste shell script into Windows cmd prompt(or linux terminal with appropriate syntax)
* Run shell script
* Go to AWS lambda console to verify function creation: https://console.aws.amazon.com/lambda/home
* Manually add s3 trigger for ObjectCreated for your input bucket
  * For AWS CLI based way to create s3 trigger: see example shell scripts below
  
aws cli lambda deploy example:
```sh
aws lambda create-function ^
--region us-east-2 ^
--function-name demo_lambda ^
--zip-file fileb://C:/Users/test_user/Desktop/test.zip ^
--role arn:aws:iam::12345567:role/lambda_basic_execution ^
--handler lambda_handler.handler ^
--runtime python2.7 ^
--timeout 5 ^
--environment Variables={NC_NAMES="field_1|field_2|field_3|field_4|field_5|field_6|field_7|field_8",OUTPUT_BUCKET="input_bucket",SECRET_NAME="test",ENDPOINT_URL="https://secretsmanager.us-east-2.amazonaws.com",REGION_NAME="us-east-2"} ^
--description "demo lambda function" ^
--memory-size 128
```

aws cli s3 trigger add lambda invoke permission example:
```sh
aws lambda add-permission 
--function-name arn:aws:lambda:us-east-2:868413670592:function:demo_lambda ^
--statement-id "invoke_lambda_permission" ^
--action "lambda:InvokeFunction" ^
--principal s3.amazonaws.com --source-arn "arn:aws:s3:::input-bucket" ^
--source-account 1234567890
```

aws cli s3 trigger deploy example:
```sh
aws s3api put-bucket-notification-configuration ^
--bucket ss-incoming-test-sung ^
--notification-configuration fileb://C:/Users/test_user/Desktop/s3_notification.json 
```

**Testing Instructions:**
* Upload a file to your input bucket with example file: ".../input_example/F1M40D180706T071453_ASDF_GH_NC.txt"
  * Raw tab delimited file
* Check output bucket for normalized file-should look like: ".../output_example/F1M40D180706T071453_ASDF_GH_NC.txt"
  * Normalized "|" delimited file containing parsed file name columns

**File Descriptions:**

s3_notification.json: configuration to programatically add s3 trigger to lambda function

within ".../src/"

psycopg2: module to interact with PgSQL databases

global_env_variables.py: paramterized global environment variables for custom field names matching parsed file name, and secrets manager

insert_to_pgsql_functions.py: python script containing UDFs that interact directly with PgSQL database

mpabatchcycle-etl-lambda.py: main orchestrator containing lambda handler which calls upon other modules/scripts

process_file_functions.py: python script containing UDFs that perform ETL operations on file name and file contents

s3_functions.py: python script containing UDFs that interact with s3

**Note:** This code base has been sanitized for template-like use. You'll notice more custom logic around normalizing the s3 object key due to business logic outside the scope of discussion for demo purposes. This isn't something you should blindly git clone and deploy for your use case. Please read through the code and translate the needed components.


