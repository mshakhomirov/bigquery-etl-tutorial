'''
This helper file is used to print files not imported into BigQuery
'''
import os
# import firebase_admin
# from firebase_admin import credentials
from google.cloud import firestore
# from firebase_admin import firestore
# from google.cloud import firestore

NAME_SIZE = 40
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

# [START firestore-query]
db = firestore.Client()
docs = db.collection(u'streaming_files_staging')\
    .where(u'success', u'==', False)\
    .stream()
# [END firestore-query]

_print_header()
for doc in docs:
    data = doc.to_dict()
    name = doc.id.ljust(NAME_SIZE)[:NAME_SIZE]
    when = data['when'].ljust(WHEN_SIZE)[:WHEN_SIZE]
    error_message = data['error_message']
    error = (error_message[:MESSAGE_SIZE-2] + '..') \
        if len(error_message) > MESSAGE_SIZE \
        else error_message.ljust(MESSAGE_SIZE)[:MESSAGE_SIZE]
    print(u'| %s | %s | %s |' % (name, when, error))
_print_footer()