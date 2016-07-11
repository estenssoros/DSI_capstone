import pandas as pd
from pymongo import MongoClient


def make_df():
    client = MongoClient()
    db = client['landman']
    coll = db['weld_county']
    master = pd.DataFrame(columns=['start_date',
                                   'end_date',
                                   'doc_num',
                                   'doc_type',
                                   'href',
                                   'text'])
    # loop mongo
    for dic in coll.find():
        df = pd.DataFrame(columns=['start_date',
                                   'end_date',
                                   'doc_num',
                                   'doc_type',
                                   'href',
                                   'text'])
        start_date = dic['start_date']
        end_date = dic['end_date']

        for k, doc in dic['results'].iteritems():
            doc_num = doc['doc_num']
            doc_type = doc['doc_type']
            doc_type = doc_type.split('\n')[0]
            href = doc['href']
            text = doc['text']
            df = df.append({'start_date': start_date,
                            'end_date': end_date,
                            'doc_num': doc_num,
                            'doc_type': doc_type,
                            'href': href,
                            'text': text}, ignore_index=True)
        master = master.append(df, ignore_index=True)
        print len(master), start_date
    master = master[~master['doc_num'].duplicated()]
    master.to_pickle('weld_docs.pickle')

if __name__ == '__main__':
    df = pd.read_pickle('weld_docs.pickle')
