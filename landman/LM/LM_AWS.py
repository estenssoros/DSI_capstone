from __future__ import division
import os
import boto
import pandas as pd


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


def sync_read(r=False):
    '''
    INPUT: boolean(optional)
    OUTPUT: dataframe (optional)

    Downloads the most recent version of new_read.csv from s3.
    If boolean supplied, returns dataframe
    '''
    df = pd.read_csv('https://s3.amazonaws.com/sebsbucket/data/new_read.csv')
    df.to_csv('data/new_read.csv', index=False)
    if r:
        return df


def write_to_s3(fname, directory=None):
    '''
    INPUT: File name, Directory
    OUTPUT: None
    write file at given directory to S3 bucket
    '''
    b = connect_s3()

    if directory:
        fname = directory + fname

    file_object = b.new_key(fname)
    file_object.set_contents_from_filename(fname, policy='public-read')

    print '{} written to {}!'.format(fname, b.name)


def write_all_to_s3(ext, directory):
    '''
    INPUT: file extension, directory
    OUTPUT: None
    uploads all files of a given extension type to s3 in the same file path schema
    '''
    b = connect_s3()
    if not directory.endswith('/'):
        directory += '/'
    for fname in os.listdir(directory):
        if fname.endswith(ext):
            write_to = directory + fname
            file_object = b.new_key(write_to)
            file_object.set_contents_from_filename(write_to, policy='public-read')
            print '{0} written to {1}!'.format(write_to, b.name)


def read_from_s3(fname, directory=None):
    if directory:
        fname = directory + fname
    b = connect_s3()
    try:
        key = b.new_key(fname)
        key.get_contents_to_filename(fname)
    except Exception as e:
        print e


def get_docs_from_s3(limit=10, s3_dir='welddocs/', ext='.pdf', df_col=None):
    '''
    INPUT: limit, s3 directory, file extension, column name (optional)
    OUTPUT: None
    Connects to s3 and pulls the first limit # of documents from new_read.csv
    that are listed as not being read. Immediately uploads new_read.csv to s3.
    Downloads the limit# of documents to specified file with extenion. If no
    column is provided, downloads the first limit # of documents with extension
    from s3 bucke file.
    '''
    b = connect_s3()
    df = sync_read(r=True)
    if df_col:
        if df_col not in df.columns:
            df[df_col] = False
        try:
            sample = df['doc'][df[df_col] == False].sample(limit)
        except:
            sample = df['doc'][df[df_col] == False]

        not_read = sample.values.tolist()
        m = df['doc'].isin(not_read)

        df.loc[m, df_col] = True
        df.to_csv('data/new_read.csv', index=False)
        write_to_s3('data/new_read.csv')

        print '{0} documents remaining in queue'.format(len(df[df[df_col] == False]))
        print '{0:.2f}% complete'.format(len(df[df[df_col] == True]) / len(df) * 100)
        if s3_dir == 'ocrdocs/':
            not_read = [x + '_ocr' for x in not_read]

        for doc in not_read:
            fname = ''.join([s3_dir, doc, ext])
            try:
                key = b.new_key(fname)
                key.get_contents_to_filename(fname)
            except Exception as e:
                print e
    else:
        sample = df['doc'].sample(limit).values.tolist()
        if s3_dir == 'ocrdocs/':
            sample = [x + '_ocr' for x in not_read]

        for doc in sample:
            fname = ''.join([s3_dir, doc, ext])
            try:
                key = b.new_key(fname)
                key.get_contents_to_filename(fname)
            except Exception as e:
                print e


def read_key(key):
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    text = key.get_contents_as_string()
    w_count = len(text.split())
    size = key.size
    return doc, w_count, size


def text_info():
    b = connect_s3()
    keys = [key for key in b.list('textdocs/') if key.name.endswith('.txt')]
    print 'keys read in!'
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(read_key, keys)
    pool.close()
    pool.join()
    return pd.DataFrame(results, columns=['doc', 'w_count', 'size'])
