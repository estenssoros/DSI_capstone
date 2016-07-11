import pandas as pd
import re
import os


def get_data():
    return pd.read_pickle('weld_docs.pickle')


def get_aws_keys():
    env = os.environ
    access_key = env['AWS_ACCESS_KEY_ID']
    access_secret_key = env['AWS_SECRET_ACCESS_KEY']
    return access_key, access_secret_key


def write_to_s3(fname):

    access_key, access_secret_key = get_aws_keys()

    conn = boto.connect_s3(access_key, access_secret_key)
    bucket_name = 'sebsbucket'

    if conn.lookup(bucket_name) is None:
        b = conn.create_bucket(bucket_name, policy='public-read')
    else:
        b = conn.get_bucket(bucket_name)

    file_object = b.new_key(fname)
    file_object.set_contents_from_filename(fname, policy='public-read')
    print '{} written to {}!'.format(fname, bucket_name)


def reg_text(s, exp):
    match = re.findall(exp, s)
    if len(match) == 1:
        return str(match[0])
    elif len(match) > 1:
        return ','.join(match)
    else:
        return ''


def clean_df(df):
    df['text'] = df['text'].str.replace(u'\xa0', u' ').str.replace(',', '')

    items = {'rec_date': r'Rec. Date: (\d\d/\d\d/\d+ \d\d:\d\d:\d\d \w\w)',
             'page_num': r'Num Pages: (\d+)',
             'section': r'Section: ([0-9 A-Z]+) [A-Z][a-z]',
             'township': r'Township: ([0-9 A-Z]+) [A-Z][a-z]',
             'range': r'Range: ([0-9 A-Z]+) [A-Z][a-z]',
             'grantor': r'Grantor: ([A-Z]+) [A-Z][a-z]',
             'grantee': r'Grantee: ([A-Z]+) \n'}

    for k, v in items.iteritems():
        df[k] = df.apply(lambda x: reg_text(x['text'], v), axis=1)

    df['range'] = df['range'].str.replace('SEE RECORD', '')

    cols = ['section', 'township', 'range', 'grantor', 'grantee']
    for col in cols:
        df[col] = df.apply(lambda x: ' '.join(x[col].split()), axis=1)
    return df
if __name__ == '__main__':
    df = get_data()
    df = clean_df(df)
    f_name = 'clean_weld_docs.csv'
    df.to_csv(f_name, ignore_index=True)
    write_to_s3(fname)
    # write_to_s3('weld_docs.pickle')
