# aws-lambda-based-etl
Process individual files and perform data normalization: s3->secrets manager->RDS & s3

**Use Case:** Creating a serverless data pipeline using AWS Lambda to minimize overhead managing infrastructure and have code invoked only when new files arrive.

**Technical Concept:** A lambda function is triggered when a new text file arrives in a s3 bucket. The file is checked for errors, parses the file name and concatenates it to the data, accesses secrets manager for Aurora PgSql database credentials, inserts records into database, exports normalized csv file to s3 bucket, and inserts records into database with lambda function metadata for ETL transformation success/failure. Database and lambda function contained in the same VPC. Assumes VPC is configured to have lambda connect with Secrets Manager for http request. This can work without VPC. 

**Prerequisites:**
* You have an AWS user account
* You have an IAM role that can access S3, Secrets Manager, RDS, VPC
* You have two s3 buckets ready for use
* You have a Aurora PgSql database setup
* You have Secrets Manager setup storing database credentials
* AWS SDK locally installed for AWS CLI use
* You're familiar with Python
* A desire to learn
* Note: shell scripting based on Windows cmd terminal(I know...I know...)

**Deploy Instructions:** This will be done using the AWS CLI
* Download folder structure in this repository to local desktop(should be in zip form)
* Open "lambda_general_commands.sh"
* Update "region"
* Update "function-name"
* Update "zip-file fileb://" with the file path of downloaded zip file
* Update "role" with IAM role
* Update buckets
* Run "XYZ" shell script(can likely double click file after saving)
* Go to AWS lambda console to verify function creation: "XYZ"

