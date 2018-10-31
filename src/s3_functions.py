# -*- coding: utf-8 -*-
"""

This module is used to import the below funcionality for the main function.
-parse the s3 event for relevant metadata
-download file into temporary location
-writing into output s3 bucket

"""
from __future__ import print_function #python 3 print function syntax
import boto3 #module to work with AWS services such as S3
import os #operating system functionality

s3 = boto3.client("s3") #define s3 bucket access

def parse_event(event):
	""" Identifies file to get from incoming event record """
	print("Parsing event")
	try:
		records = event.get("Records", [])
		if len(records) > 0:
			record = records[0]
				
		#region = record.get("awsRegion", "")
		#extracts the JSON relevant information for variable population
		s3data = record.get("s3", {})
		print(s3data)
		bucket = s3data.get("bucket", {})
		bucket_name = bucket.get("name", "")
		object = s3data.get("object", {})
		object_key = object.get("key", "")
		return bucket_name, object_key
	except Exception as e:
		print("parse_event: {}".format(e))
		raise e

def pull_from_s3(bucket_name, object_key):
	""" Pull file from S3 """
	print("Pulling {} from s3".format(object_key))
	try:
		tmp_file_path = "/tmp/original_file.txt" #lambda can only write to tmp folder
		s3.download_file(bucket_name, object_key, tmp_file_path)
		return tmp_file_path
	except Exception as e:
		print("pull_from_s3: {}".format(e))
		raise e

def save_results_in_s3(output_bucket, temp_output_path, processed_file_name):
	""" save resulting file from /tmp/ storage over to S3 output bucket """
	print("Saving results to S3 {}".format(processed_file_name))
	try:
		s3.upload_file(temp_output_path, output_bucket, processed_file_name)
		return None
	except Exception as e:
		print("save_results_in_s3: {}".format(e))
		raise e