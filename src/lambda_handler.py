# -*- coding: utf-8 -*-
"""" 
This script is the main orchestrator application. 
Modularized functions imported from other python scripts.
As this lambda function evolves over time, this enables iterative development
and allows additional and modified scripts/modules to serve specific purposes.

If all code is contained within this main script, maintability will grow increasingly difficult long-term.
"""

from __future__ import print_function #python 3 print function syntax
import json #module to work with JSON format files
import urllib #open, read, and write with urls
import boto3 #module to work with AWS services such as S3
import re #module to work with regular expressions
import os #operating system functionality
from datetime import datetime #work with date and time formatting/functions
import psycopg2 #package to interact with PgSQL database 
import sys #used for system operations such as quitting out of the function

#custom module functions imported from other python files in the same directory
from global_env_variables import NC_NAMES, OUTPUT_BUCKET, SECRET_NAME, ENDPOINT_URL, REGION_NAME
from s3_functions import parse_event, pull_from_s3, save_results_in_s3, s3 #import functions built in s3_functions.py
from process_file_functions import  check_file_contents, quit_or_continue, readlines_until_blank, normalize_object_key, transform_object_key_values, process_file_contents, write_output #import functions built in process_file_functions.py
from insert_to_pgsql_functions import fetch_credentials, openConnection, load_etl_ld_cntl, insert_records_to_db_success, insert_records_to_db_error  #import functions built in insert_to_pgsql_functions.py

START_DTM = datetime.now() #capture start time of lambda function 
print("Loading function")

#open connection to database as global variables
#makes lambda more performant as the db connection remains open for consecutive invocations when located outside handler
CONNECTION_STRING = fetch_credentials(SECRET_NAME, ENDPOINT_URL, REGION_NAME) #capture credentials
CONN = openConnection(CONNECTION_STRING) #open connection to database

def handler(event, context):
	""" Get the object from the event and show its content type """
	print("Received event: " + json.dumps(event, indent=2)) #indicates received file and gets context on bucket, what kicked the event off
	bucket = event["Records"][0]["s3"]["bucket"]["name"] #parses the json for relevant information
	key = urllib.unquote_plus(event["Records"][0]["s3"]["object"]["key"].encode("utf8")) 
	print("New File Arrived: " + key) #indicates new unique file
	try:
		object_key = "" #clear out file name variable
		job_run_id = context.aws_request_id #unique request id
		job_nm = context.function_name # name of lambda function invoked
		tb_nm = "mpa-batch-cycle-measures" #name of output table being created with lambda function
		
		#ingest raw file
		bucket_name, object_key = parse_event(event) #capture the bucket and file names
		temp_file_name = pull_from_s3(bucket_name, object_key) #download s3 file into temporary folder
		error_file_name = check_file_contents(temp_file_name, object_key) #populates error_file_name object if there is an error within the file
		quit_or_continue(CONN, error_file_name, object_key, temp_file_name, job_run_id, job_nm, tb_nm, START_DTM) #continues rest of function if no errors or quits entirely
		
		#transform raw file
		data = readlines_until_blank(temp_file_name, object_key) #data is read until blank line and stops, still produces blank line with filename fields
		object_key_norm = normalize_object_key(object_key) #normalize the object key for later processing
		nc_values = transform_object_key_values(object_key_norm) #transform object key for data insertion
		processed_data = process_file_contents(data, object_key, NC_NAMES, nc_values) #list object of transformed data

		#export transformed file
		temp_output_path, processed_file_name = write_output(processed_data, object_key) #write output file to temporary path
		save_results_in_s3(OUTPUT_BUCKET, temp_output_path, processed_file_name) #save results to processed subfolder within bucket
		print("{0} successfully processed and uploaded as {1} to {2}".format(object_key, processed_file_name, OUTPUT_BUCKET))
		temp_output_path, batch_file_name = write_output(processed_data, object_key)
		batch_file_name = processed_file_name.replace("processed", "batch") #save results to batch subfolder within bucket
		save_results_in_s3(OUTPUT_BUCKET, temp_output_path, batch_file_name)
		print("{0} successfully processed and uploaded as {1} to {2}".format(object_key, batch_file_name, OUTPUT_BUCKET))

		#log record metadata in Aurora DB if data is processed successfully
		insert_records_to_db_success(CONN, object_key, job_run_id, job_nm, tb_nm, START_DTM, processed_data)
	except Exception as e:
		print(e)
		insert_records_to_db_error(CONN, error_file_name, object_key, job_run_id, job_nm, tb_nm, START_DTM) #write error metadata to Aurora DB
		print("Error processing object {} from bucket {}.".format(key, bucket))
		raise e