# -*- coding: utf-8 -*-
"""

This module is used to import the below funcionality for the main function.
-capturing and passing through secrets manager credentials for the database
-opening the database connection
-inserting tailored lambda function logs for each file processed

"""
from __future__ import print_function

import json
import boto3
import psycopg2 #package to interact with PgSQL database 
from datetime import datetime #work with date and time formatting/functions
from botocore.exceptions import ClientError
from global_env_variables import OUTPUT_BUCKET

#AWS documentation for lambda accessing resources in VPC
# https://docs.aws.amazon.com/lambda/latest/dg/vpc.html
# https://docs.aws.amazon.com/lambda/latest/dg/vpc-rds.html
# https://forums.aws.amazon.com/thread.jspa?threadID=279633

def fetch_credentials(secret_name, endpoint_url, region_name):
	"""boilerplate AWS code to capture database credentials"""
	print("capture Aurora DB credentials")
	secret = ""
	binary_secret_data = ""
	session = boto3.session.Session()
	client = session.client(
		service_name="secretsmanager",
		region_name=region_name,
		endpoint_url=endpoint_url
	)

	try:
		get_secret_value_response = client.get_secret_value(
			SecretId=secret_name
			)
		# Decrypted secret using the associated KMS CMK
		# Depending on whether the secret was a string or binary, one of these fields will be populated
		if "SecretString" in get_secret_value_response:
			secret = get_secret_value_response["SecretString"]
		else:
			binary_secret_data = get_secret_value_response["SecretBinary"]
	except ClientError as e:
		if e.response["Error"]["Code"] == "ResourceNotFoundException":
			print("The requested secret " + secret_name + " was not found")
		elif e.response["Error"]["Code"] == "InvalidRequestException":
			print("The request was invalid due to:", e)
		elif e.response["Error"]["Code"] == "InvalidParameterException":
			print("The request had invalid params:", e)

	#Get secret string in JSON
	secretString = json.loads(secret)
	username = secretString["username"]
	# engine = secretString["engine"]
	host = secretString["host"]
	password = secretString["password"]
	dbname = secretString["dbClusterIdentifier"]
	# port = secretString["port"]
	connection_string = "host="+host+" dbname="+dbname+" user="+username+" password="+password
	return connection_string

#Open Connection
def openConnection(connection_string):
	"""create a connection to the Aurora database with connection_string"""
	print("connect to database")
	try:
		print("Opening Connection")
		conn = psycopg2.connect(connection_string)
		return conn
	except Exception as e:
		print (e)
		print("Unexpected error: Could not connect to PostgresSQL instance.")
		raise e

#assume the table we"re loading into always exists
#only care about the insert statement
#capture start and end time manually within lambda function and pass through to insert statement
#variables will be captured at the top of the main lambda script and dynamically updated 

def load_etl_ld_cntl(conn,job_run_id,job_nm,tb_nm,comment,ld_status,ins_row_cnt,start_dtm,end_dtm):
	"""load data into Aurora database based on files/records processed by lambda function"""
	print("loading processed records into Aurora DB")
	try:
		# print the connection string we will use to connect --> REMOVE BEFORE DEV
		print("conn.status", conn.status)
		print("conn.server_version", conn.server_version)
		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
		# insert records
		sql_text ="INSERT INTO etl.ld_cntrl_tb(job_run_id,job_nm, tb_nm, start_dtm, end_dtm, ins_row_cnt, comment,ld_status) VALUES %s ;"
		#Generate list
		data = (job_run_id,job_nm,tb_nm ,start_dtm,end_dtm,ins_row_cnt,comment,ld_status)
		query = cursor.mogrify(sql_text, [data]) #save the query
		print(cursor.mogrify(sql_text, [data])) #prints the query as a check
		cursor.execute(query)	#execute the query
		conn.commit() #confirms changes made to an existing table
		cursor.close() #close db connection
	except Exception as e:
		print("load_etl_ld_cntl: {}".format(e))
		raise e

def insert_records_to_db_error(conn, error_file_name, object_key, job_run_id, job_nm, tb_nm, start_dtm):
	""" conditional function to insert metadata into DB within the quit_or_continue function or any lambda error """
	print("inserting error record metadata into Aurora DB")
	#define dynamic variables for records written to 
	comment = "" #clear out comment
	ld_status = "" #create conditional status
	ins_row_cnt = 0
	end_dtm = ""
	try:
		if error_file_name != "":
			print("empty file error and {0} written to {1}".format(object_key, OUTPUT_BUCKET))
			comment = "empty file error and {0} written to {1}".format(object_key, OUTPUT_BUCKET)
			ld_status = "FAILURE"
			end_dtm = datetime.now()
			load_etl_ld_cntl(conn,job_run_id,job_nm,tb_nm,comment,ld_status,ins_row_cnt,start_dtm,end_dtm) #load records into database
		else:
			print("Error processing object due to unexpected error, investigate cloudwatch logs: {}".format(object_key))
			comment = "Error processing object due to unexpected error, investigate cloudwatch logs: {}".format(object_key)
			ld_status = "FAILURE"
			end_dtm = datetime.now()
			load_etl_ld_cntl(conn,job_run_id,job_nm,tb_nm,comment,ld_status,ins_row_cnt,start_dtm,end_dtm) #load records into database
	except Exception as e:
		print("insert_records_to_db_error: {}".format(e))
		raise e

def insert_records_to_db_success(conn,object_key, job_run_id, job_nm, tb_nm, start_dtm, data):
	""" insert metadata into DB after data has been processed """
	print("inserting record metadata into Aurora DB")
	#define dynamic variables for records written
	comment = ""
	ld_status = "" 
	ins_row_cnt = 0
	end_dtm = ""
	try:
		print("{0} written to batch and processed subfolders within bucket: {1}".format(object_key, OUTPUT_BUCKET))
		comment = "{0} file written to batch and processed subfolders within bucket: {1}".format(object_key, OUTPUT_BUCKET)
		ld_status = "SUCCESS"
		ins_row_cnt = len(data)-1 #assumes removed header data for row count within transformed data
		end_dtm = datetime.now()
		load_etl_ld_cntl(conn,job_run_id,job_nm,tb_nm,comment,ld_status,ins_row_cnt,start_dtm,end_dtm) #load records into database
	except Exception as e:
		print("insert_records_to_db_success: {}".format(e))
		raise e