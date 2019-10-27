'''
This helper file is used to delete filesfrom Firestore using "where" clause
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
# [START firestore-query]
db = firestore.Client()
docs = db.collection(u'streaming_files_staging')\
    .where(u'when', u'<=', '2019-10-11 21:15:55 UTC')\
    .stream()
# [END firestore-query]
deleted = 0
_print_header()
for doc in docs:
    data = doc.to_dict()
    name = doc.id.ljust(NAME_SIZE)[:NAME_SIZE]
    # name = doc.id
    when = data['when'].ljust(WHEN_SIZE)[:WHEN_SIZE]
    if ('error_message' in data):
        error_message = data['error_message']
        message = (error_message[:MESSAGE_SIZE-2] + '..') \
            if len(error_message) > MESSAGE_SIZE \
            else error_message.ljust(MESSAGE_SIZE)[:MESSAGE_SIZE]
    elif('duplication_attempts' in data):
        message = 'duplication_attempt'
    else:
        message = 'success'
    # print(data)
    print(u'| %s | %s | %s |' % (name, when, message))
    # Now delete doc
    doc.reference.delete()
    deleted = deleted + 1
_print_footer()

print(u'files deleted : ', deleted)