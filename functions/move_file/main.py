'''
This Cloud function moves a file from one bucket to another
'''

import base64
import os
import logging

from google.cloud import storage

CS = storage.Client()

def move_file(data, context):
    '''This function is executed from a Cloud Pub/Sub'''
    message = base64.b64decode(data['data']).decode('utf-8')
    file_name = data['attributes']['file_name']
    logging.info(data)
    # logging.info('data[\'attributes\'] \'%s\' --- file_name: \'%s\' ',
    #              data['attributes'],
    #              data['attributes']['file_name'])

    source_bucket_name = os.getenv('SOURCE_BUCKET')
    source_bucket = CS.get_bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)

    destination_bucket_name = os.getenv('DESTINATION_BUCKET')
    destination_bucket = CS.get_bucket(destination_bucket_name)

    source_bucket.copy_blob(source_blob, destination_bucket, file_name)
    source_blob.delete()

    logging.info('File \'%s\' moved from \'%s\' to \'%s\': \'%s\'',
                 file_name,
                 source_bucket_name,
                 destination_bucket_name,
                 message)