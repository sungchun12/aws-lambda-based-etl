# -*- coding: utf-8 -*-
"""

This contains all the global variables provided by the environment variables in the lambda console.
This is used to import shared global variables across all the scripts being used as imported modules.

"""
import os #operating system functionality

# read configs from environment variables contained within AWS lambda console
NC_NAMES = os.getenv("NC_NAMES", "")
NC_NAMES = NC_NAMES.split("|") #convert string to list
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "") #captures processed/batch s3 bucket

# define secrets manager environment variables to access Aurora DB
SECRET_NAME = os.getenv("SECRET_NAME", "") #name of secret
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "") #"https://secretsmanager.us-east-2.amazonaws.com"
REGION_NAME = os.getenv("REGION_NAME", "") #"us-east-2"
