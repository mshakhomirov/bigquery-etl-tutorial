'''
This helper file is used to delete files by name from Firestore
'''
import os
# import firebase_admin
# from firebase_admin import credentials
from google.cloud import firestore
# from firebase_admin import firestore
# from google.cloud import firestore
import pytz
from datetime import datetime
def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

def _today():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d 00:00:00 %Z')

NAME_SIZE = 160
WHEN_SIZE = 24
MESSAGE_SIZE = 80

def _print_header():
    _print_line()
    print(u'| %s | %s | %s |' % (
        u'File Name'.ljust(NAME_SIZE)[:NAME_SIZE],
        u'When'.ljust(WHEN_SIZE)[:WHEN_SIZE],
        u'Error Message'.ljust(MESSAGE_SIZE)[:MESSAGE_SIZE],))
    _print_line()

def _print_footer():
    _print_line()

def _print_line():
    print(u'+-%s-+-%s-+-%s-+' % (
        u''.ljust(NAME_SIZE, '-')[:NAME_SIZE],
        u''.ljust(WHEN_SIZE, '-')[:WHEN_SIZE],
        u''.ljust(MESSAGE_SIZE, '-')[:MESSAGE_SIZE],))
# .where(u'when', u'<=', '2019-10-12 00:00:00')\
# [START firestore-query]
files_to_delete = [u'table-5_data_new_line_delimited_json.json',u'some_folder\\table-5_data_new_line_delimited_json.json']
db = firestore.Client()
for name in files_to_delete:
    # Get file contents by name:
    doc_ref = db.collection(u'streaming_files_staging').document(name).get()
    data = doc_ref.to_dict()
    doc_ref = db.collection(u'streaming_files_staging').document(name)
    
    print('+ Deleting ', name)
    print('+++ records ', data)
    doc_ref.delete()

print('===============')
print('===============')
for name in files_to_delete:
    doc_ref = db.collection(u'streaming_files').document(name).get()
    data = doc_ref.to_dict()
    print(data, 'records left in ', name)
