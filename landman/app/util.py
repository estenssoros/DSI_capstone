import os
import boto
from datetime import datetime as dt


def aws_keys():
    '''
    INPUT: None
    OUTPUT: aws access_key and secret_access_key
    '''
    env = os.environ
    access_key = env['AWS_ACCESS_KEY_ID']
    access_secret_key = env['AWS_SECRET_ACCESS_KEY']
    return access_key, access_secret_key


def connect_s3():
    access_key, access_secret_key = aws_keys()

    conn = boto.connect_s3(access_key, access_secret_key)
    bucket_name = 'sebsbucket'

    if conn.lookup(bucket_name) is None:
        raise ValueError('Bucket does not exist! WTF!')
    else:
        b = conn.get_bucket(bucket_name)
        print 'Connected to {}!'.format(bucket_name)
    return b


def get_docs_from_s3(docs, s3_dir='welddocs/', ext='.pdf'):
    '''
    '''
    b = connect_s3()

    for doc in docs:
        fname = ''.join([s3_dir, doc, ext])
        try:
            key = b.new_key(fname)
            key.get_contents_to_filename(fname)
        except Exception as e:
            print e, fname


def clear_docs(directory='welddocs/', ext=['.pdf']):
    for e in ext:
        [os.remove(directory + doc) for doc in os.listdir(directory) if doc.endswith(e)]
