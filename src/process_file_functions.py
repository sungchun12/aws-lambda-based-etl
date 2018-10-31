# -*- coding: utf-8 -*-
"""

This module is used to import the below funcionality for the main function.
-check file contents for whether it's empty or not
-quitting or continuing the function based on the above
-normalizing the file name for ETL purposes
-extracting data from the file name for ETL purposes
-processing the file contents into desired format for ETL purposes
-writing output to temporary folder before being exported into output S3 bucket

"""
from __future__ import print_function
import re
import os
from datetime import datetime
import sys

#imports from other script modules
from global_env_variables import OUTPUT_BUCKET
from insert_to_pgsql_functions import insert_records_to_db_error  
from s3_functions import save_results_in_s3 

def check_file_contents(file_path, object_key): 
	""" checks the file for expected/unexpected errors and outputs unexpected errors to their own bucket """
	print("Checking for content errors of file {}".format(object_key))
	error_file_name = ""
	try:
		if os.stat(file_path).st_size == 0:  #check if file is empty
			error_file_name = object_key.replace("Local_Data", "empty_file_error") #replace name for later output
		else:
			print("No empty_file_error detected")
		return error_file_name
	except Exception as e:
		print("check_file_contents: {}".format(e))
		raise e

#include insert into DB within quit or continue
def quit_or_continue(conn, error_file_name, object_key, file_path, job_run_id, job_nm, tb_nm, start_dtm): 
	""" checks the file for expected errors and outputs to their own bucket """
	print("Quit and output the error file OR continue the lambda function based on error check: {}".format(object_key))
	try:
		if error_file_name != "":
			temp_output_path = "/tmp/processed_file.txt"
			with open(temp_output_path, "w") as output_file: #write original file to temporary output folder in lambda
				original_file=open(file_path, "r")
				for record in original_file:
					output_file.write(record)
			save_results_in_s3(OUTPUT_BUCKET, temp_output_path, error_file_name) #output to empty file error subfolder
			print("{0} successfully processed and uploaded as {1} to {2}".format(object_key, error_file_name, OUTPUT_BUCKET))
			insert_records_to_db_error(conn, error_file_name, object_key, job_run_id, job_nm, tb_nm, start_dtm)
			sys.exit("lambda function stopped processing file with error: {}".format(object_key))
		else: 
			print("continue processing file: {}".format(object_key))
	except Exception as e:
		print("quit_or_continue: {}".format(e))
		raise e

def readlines_until_blank(file_path, object_key):
	"""generates list from file until the blank line"""
	print("reading lines in file until blank: {}".format(object_key))
	lines=[]
	try:
		with open(file_path, "r") as input_file:
			while True:
				try:
					line = next(input_file) #iterates through lines
					lines.append(line) #appends into list
					if not line.strip(): #if there"s no line to trim, stop the iterations
						break
				except StopIteration:
					break
			return lines
	except Exception as e:
		print("readlines_until_blank: {}".format(e))
		raise e

#if object key contains ['asdfasdf'], parse based on current method parsing
#if object key contains ['asdf'], parse based on below
#if object key does not match patterns expected, quit out of function
#need to update insert into db function error
#need to update process_file_contents
#get rid of environment variables for NC_NAMES
# 1.	Replace “__” with “_”
# 2.	Replace “_.” With “.”
def normalize_object_key(object_key):
	"""Normalize object_key for later processing"""
	print("normalizing object_key")
	object_key_clean = "" #clear variable
	object_key_norm = "" #clear variable
	try: 
		object_key_clean = re.sub(r"[?!@#$%^&*(){}''=+-]", "", object_key) #clear general error characters
		object_key_norm = object_key_clean.replace("___","_").replace("__.",".") \
		.replace("_.",".").replace("__","_").replace("_Prod_IRTEC_","_") #replace known string errors
		return object_key_norm
	except Exception as e:
		print("normalize_object_key: {}".format(e))
		raise e

#need to incorplate blank load_type value in nc_values at 7th index position for facilities_v1
# derive new values from the file name in the object key:
#\w+ means one or more word character [a-zA-Z0-9_] (depending on LOCALE)
#\d+ means one or more digit [0-9] (depending on LOCALE)
#example: F98M26D180729T210004_TEST_GH_FF => F98 M26 D180729 T210004 TEST GH FF
#ultimately should parse into 98 26 180729 210004 TEST GH FF
def transform_object_key_values(object_key_norm):
	""" Normalize object_key_norm values to create named column headers and values """
	print("normalizing object_key file name for NC_NAMES, nc_values")
	nc_values = "" #clear named column values
	facility_index = 0 #position of the vessel within the file path
	filename_index = 2 #position of the filename within the file path
	facility = object_key_norm.split("/")[facility_index] #parse portion directly related to filename
	filename_value = object_key_norm.split("/")[filename_index] #parse portion directly related to filename
	facilities_v1 = ["demo"]
	facilities_v2 = ["demo2"]
	try:
		if facility in (x for x in facilities_v1): #captures already normalized file names
			nc_values = re.search("(\w+)(M\w+)(D\d+)(T\d+)_(\w+)_(\w+)_(\w+)", object_key_norm).groups() #parse filename
			nc_values = list(nc_values) #convert named columns into list 
			nc_values.insert(6,"") #insert blank value into load_type position
		elif facility in (x for x in facilities_v2):
			nc_values = re.search("(\w+)(M\w+)(D\d+)(T\d+)_(\w+)_(\w+)_(\w+)_(\w+)", object_key_norm).groups() #parse filename
			nc_values = list(nc_values) #convert named columns into list 
		nc_values = [filename_value] + nc_values #add filename value to named column values
		return nc_values
	except Exception as e:
		print("normalize_filename: {}".format(e))
		raise e

def process_file_contents(data, object_key, nc_names, nc_values):
	""" 1) Insert new column headers 2) Add values derived from file name to all records in the dataset """
	print("Processing contents of file {}".format(object_key))
	try:
		data = [x for x in data if x.strip() != ""] #filter out new line breaks "\n" in list

		#derive cycle run timestamp in required format
		datestamp = "20"+nc_values[3][1:] #concatenate "20" to clearly mark 4 character year-no data comes before the year 2000
		timestamp = nc_values[4][1:] #parse out the timestamp
		cycle_timestamp = datestamp+" "+timestamp #concatenate date and time stamp
		cycle_timestamp = str(datetime.strptime(cycle_timestamp,"%Y%m%d %H%M%S")) #format datetime stamp
	
		#parse out relevant strings using indices
		#example: [2][1:]=3rd item in list object and capture string from 2nd position and on
		nc_values = nc_values[0][:-4]+","+nc_values[1][1:]+","+nc_values[2][1:]+","+cycle_timestamp+","+\
		nc_values[5]+","+nc_values[6]+","+nc_values[7]+","+nc_values[8]
	
		#convert string to list
		nc_values=nc_values.split(",")
	
		# separate headers from the rest of the data:
		headers = data[0].split("\t") #split up the 1st line of headers which are tab delimited
		data = [elem.split("\t") for elem in data[1:]] #split up remaining rows of data which are tab delimited
		
		# insert new headers and new values in each row of dataset:
		updated_headers = nc_names + headers #named columns defined in lambda console + raw file headers
		data = [nc_values + elem for elem in data] #named columns values in above variable is appended to the left of the raw file values
		
		# put headers back in element 0:
		data.insert(0, updated_headers)
		return data
	except Exception as e:
		print("process_file_contents: {}".format(e))
		raise e

def write_output(processed_data, object_key):
	""" save results from processing to /tmp/ storage for all processed AND batch files """
	print("Writing output of processed file {}".format(object_key))
	try:
		processed_file_name = object_key.replace("Local_Data", "processed")
		temp_output_path = "/tmp/processed_file.txt"
		with open(temp_output_path, "w") as output_file:
			for record in processed_data:
				record = "|".join(record) #"|" delimited
				output_file.write(record)
		return temp_output_path, processed_file_name
	except Exception as e:
		print("write_output: {}".format(e))
		raise e