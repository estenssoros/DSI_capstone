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


def reg_text(s, exp):
    s = s.replace(u'\xa0', u' ')
    match = re.findall(exp, s)
    if len(match) == 1:
        return str(match[0])
    elif len(match) > 1:
        return ','.join(match)
    else:
        return ''


def extract_text(df):
    items = {'rec_date': r'Rec. Date: (\d\d/\d\d/\d+ \d\d:\d\d:\d\d \w\w)',
             'page_num': r'Num Pages: (\d+)',
             'section': r'Section: (\d+) ',
             'township': r'Township: (\d+) ',
             'range': r'Range: (\d+) ',
             'grantor': r'Grantor: (\D+) Section',
             'grantee': r'Grantee: (\D+) \n'}

    for k, v in items.iteritems():
        df[k] = df.apply(lambda x: reg_text(x['text'], v), axis=1)
    return df
if __name__ == '__main__':
    df = get_data()
    df = extract_text(df)
    # write_to_s3('weld_docs.pickle')
