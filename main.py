'''
This simple a Cloud Function responsible for:
- Loading data using schemas
- Loading data from different data file formats 
'''

import json
import logging
import os
import traceback
from datetime import datetime
import io
import re
from six import StringIO
from six import BytesIO

from google.api_core import retry
from google.cloud import bigquery
# from google.cloud import firestore
# from google.cloud import pubsub_v1
from google.cloud import storage
import pytz

import pandas
import yaml

with open("./schemas.yaml") as schema_file:
    config = yaml.load(schema_file)


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'staging'
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()

"""
This is our Cloud Function:
"""
def streaming(data, context):
    bucketname = data['bucket'] 
    filename = data['name']     
    timeCreated = data['timeCreated']
    try:
        for table in config:
            tableName = table.get('name')
            # Check which table the file belongs to and load:
            if re.search(tableName.replace('_', '-'), filename) or re.search(tableName, filename):

                tableSchema = table.get('schema')
                # check if table exists, otherwise create:
                _check_if_table_exists(tableName,tableSchema)

                # Check source file data format. Depending on that we'll use different methods:
                tableFormat = table.get('format')
                if tableFormat == 'NEWLINE_DELIMITED_JSON':
                    _load_table_from_uri(data['bucket'], data['name'], tableSchema, tableName)
                elif tableFormat == 'OUTER_ARRAY_JSON':
                    _load_table_from_json(data['bucket'], data['name'], tableSchema, tableName)
                elif tableFormat == 'SRC':
                    _load_table_as_src(data['bucket'], data['name'], tableSchema, tableName)
                elif tableFormat == 'OBJECT_STRING':
                    _load_table_from_object_string(data['bucket'], data['name'], tableSchema, tableName)
                elif tableFormat == 'DF':
                    _load_table_from_dataframe(data['bucket'], data['name'], tableSchema, tableName)
                elif tableFormat == 'DF_NORMALIZED':
                    _load_table_as_df_normalized(data['bucket'], data['name'], tableSchema, tableName)
        
    except Exception:
        _handle_error()

def _insert_rows_into_bigquery(bucket_name, file_name):
    blob = CS.get_bucket(bucket_name).blob(file_name)
    row = json.loads(blob.download_as_string())
    print('row: ', row)
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_json(table,
                                 json_rows=[row],
                                 row_ids=[file_name],
                                 retry=retry.Retry(deadline=30))

    print(errors)
    if errors != []:
        raise BigQueryError(errors)

def _load_table_from_json(bucket_name, file_name, tableSchema, tableName):
    blob = CS.get_bucket(bucket_name).blob(file_name)
    #! source data file format must be outer array JSON:
    """
    [
    {"id":"1","first_name":"John","last_name":"Doe","dob":"1968-01-22","addresses":[{"status":"current","address":"123 First Avenue","city":"Seattle","state":"WA","zip":"11111","numberOfYears":"1"},{"status":"previous","address":"456 Main Street","city":"Portland","state":"OR","zip":"22222","numberOfYears":"5"}]},
    {"id":"2","first_name":"John","last_name":"Doe","dob":"1968-01-22","addresses":[{"status":"current","address":"123 First Avenue","city":"Seattle","state":"WA","zip":"11111","numberOfYears":"1"},{"status":"previous","address":"456 Main Street","city":"Portland","state":"OR","zip":"22222","numberOfYears":"5"}]}
    ]
    """
    body = json.loads(blob.download_as_string())
    table_id = BQ.dataset(BQ_DATASET).table(tableName)

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    job_config.write_disposition = 'WRITE_APPEND'

    schema = create_schema_from_yaml(tableSchema) 
    job_config.schema = schema

    load_job = BQ.load_table_from_json(
        body,
        table_id,
        job_config=job_config,
        )   

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")


def _load_table_as_src(bucket_name, file_name, tableSchema, tableName):
    # ! source file must be outer array JSON
    # ! this will work for CSV where a row is A JSON string --> SRC column (Snowflake like)
    blob = CS.get_bucket(bucket_name).blob(file_name)
    body = json.loads(blob.download_as_string())
    table_id = BQ.dataset(BQ_DATASET).table(tableName)
    
    schema = create_schema_from_yaml(tableSchema) 
    job_config.schema = schema

    job_config.source_format = bigquery.SourceFormat.CSV,
    # something that doesn't exist in your data file:
    job_config.field_delimiter =";"   
    # Notice that ';' worked because the snippet data does not contain ';'

    job_config.write_disposition = 'WRITE_APPEND',

    data_str = u"\n".join(json.dumps(item) for item in body)
    print('data_str :', data_str)
    data_file = io.BytesIO(data_str.encode())
    print('data_file :', data_file)
    load_job = BQ.load_table_from_file(
        data_file,
        table_id,
        job_config=job_config,
        ) 

    load_job.result()
    print("Job finished.")

def _load_table_from_object_string(bucket_name, file_name, tableSchema, tableName):
    # ! source file must be object string, e.g.:
    """
{"id": "1", "first_name": "John", "last_name": "Doe", "dob": "1968-01-22", "addresses": [{"status": "current", "address": "123 First Avenue", "city": "Seattle", "state": "WA", "zip": "11111", "numberOfYears": "1"}, {"status": "previous", "address": "456 Main Street", "city": "Portland", "state": "OR", "zip": "22222", "numberOfYears": "5"}]}{"id": "2", "first_name": "John", "last_name": "Doe", "dob": "1968-01-22", "addresses": [{"status": "current", "address": "123 First Avenue", "city": "Seattle", "state": "WA", "zip": "11111", "numberOfYears": "1"}, {"status": "previous", "address": "456 Main Street", "city": "Portland", "state": "OR", "zip": "22222", "numberOfYears": "5"}]}
    """
    # ! we will convert body to a new line delimited JSON
    blob = CS.get_bucket(bucket_name).blob(file_name)
    blob = blob.download_as_string().decode()
    # Transform object string data into JSON outer array string:
    blob = json.dumps('[' + blob.replace('}{', '},{') + ']')
    # Load as JSON:
    body = json.loads(blob)
    # Create an array of string elements from JSON:
    jsonReady = [json.dumps(record) for record in json.loads(body)]
    # Now join them to create new line delimited JSON:
    data_str = u"\n".join(jsonReady)
    print('data_file :', data_str)
    # Create file to load into BigQuery:
    data_file = StringIO(data_str)

    table_id = BQ.dataset(BQ_DATASET).table(tableName)
    
    schema = create_schema_from_yaml(tableSchema) 
    job_config.schema = schema

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    job_config.write_disposition = 'WRITE_APPEND',

    load_job = BQ.load_table_from_file(
        data_file,
        table_id,
        job_config=job_config,
        ) 

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")


def _check_if_table_exists(tableName,tableSchema):
    # get table_id reference
    table_id = BQ.dataset(BQ_DATASET).table(tableName)

    # check if table exists, otherwise create
    try:
        BQ.get_table(table_id)
    except Exception:
        logging.warn('Creating table: %s' % (tableName))
        schema = create_schema_from_yaml(tableSchema)
        table = bigquery.Table(table_id, schema=schema)
        table = BQ.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        # BQ.create_dataset(dataset_ref)

def _load_table_from_uri(bucket_name, file_name, tableSchema, tableName):
    # ! source file must be like this:
    """
{"id": "1", "first_name": "John", "last_name": "Doe", "dob": "1968-01-22", "addresses": [{"status": "current", "address": "123 First Avenue", "city": "Seattle", "state": "WA", "zip": "11111", "numberOfYears": "1"}, {"status": "previous", "address": "456 Main Street", "city": "Portland", "state": "OR", "zip": "22222", "numberOfYears": "5"}]}
{"id": "2", "first_name": "John", "last_name": "Doe", "dob": "1968-01-22", "addresses": [{"status": "current", "address": "123 First Avenue", "city": "Seattle", "state": "WA", "zip": "11111", "numberOfYears": "1"}, {"status": "previous", "address": "456 Main Street", "city": "Portland", "state": "OR", "zip": "22222", "numberOfYears": "5"}]}
    """
    # ! source file must be the same.
    #! if source file is not a NEWLINE_DELIMITED_JSON then you need to load it with blob, convert to JSON and then load as file.
    uri = 'gs://%s/%s' % (bucket_name, file_name)
    table_id = BQ.dataset(BQ_DATASET).table(tableName)

    schema = create_schema_from_yaml(tableSchema) 
    print(schema)
    job_config.schema = schema

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    job_config.write_disposition = 'WRITE_APPEND',

    load_job = BQ.load_table_from_uri(
    uri,
    table_id,
    job_config=job_config,
    ) 
        
    load_job.result()
    print("Job finished.")

def _load_table_from_dataframe(bucket_name, file_name, tableSchema, tableName):
    """
    Source data file must be outer JSON
    """
    blob = CS.get_bucket(bucket_name).blob(file_name)
    body = json.loads(blob.download_as_string())
    table_id = BQ.dataset(BQ_DATASET).table(tableName)

    schema = create_schema_from_yaml(tableSchema) 
    job_config.schema = schema

    df = pandas.DataFrame(
    body,
    # In the loaded table, the column order reflects the order of the
    # columns in the DataFrame.
    columns=["id", "first_name","last_name","dob","addresses"],

    )
    df['addresses'] = df.addresses.astype(str)
    df = df[['id','first_name','last_name','dob','addresses']]

    load_job = BQ.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
        ) 

    load_job.result()
    print("Job finished.")


def _load_table_as_df_normalized(bucket_name, file_name, tableSchema, tableName):
    """
    Source data file must be outer JSON
    """
    blob = CS.get_bucket(bucket_name).blob(file_name)
    body = json.loads(blob.download_as_string())
    table_id = BQ.dataset(BQ_DATASET).table(tableName)

    schema = create_schema_from_yaml(tableSchema) 
    job_config.schema = schema

    df = pandas.io.json.json_normalize(data=body, record_path='addresses', 
                            meta=[ 'id'	,'first_name',	'last_name',	'dob']
                            , record_prefix='addresses_'
                            ,errors='ignore')
    
    df = df[['id','first_name','last_name','dob','addresses_status','addresses_address','addresses_city','addresses_state','addresses_zip','addresses_numberOfYears']]

    load_job = BQ.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
        ) 

    load_job.result()
    print("Job finished.")



def _handle_error():
    message = 'Error streaming file. Cause: %s' % (traceback.format_exc())
    print(message)

def create_schema_from_yaml(table_schema):
    schema = []
    for column in table_schema:
        
        schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])

        schema.append(schemaField)

        if column['type'] == 'RECORD':
            schemaField._fields = create_schema_from_yaml(column['fields'])
    return schema

class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)