from twilio.rest import TwilioRestClient
import json
import os
import pandas as pd
import re
import enchant


def read_json(fname):
    '''
    INPUT: file name, dictionary key
    OUTPUT: url form dictionary
    Read url form json file
    '''
    with open(fname) as f:
        for line in f:
            jdict = json.loads(line)
    return j_dict


def twilio_message(message):
    '''
    INPUT: message
    OUTPUT: None
    Sends SMS via twilio client
    '''
    account = os.environ['TWILIO_ACCOUNT']
    token = os.environ['TWILIO_TOKEN']
    client = TwilioRestClient(account, token)
    message = client.messages.create(to="+13032299207", from_="+17206139570", body=message)


def print_status(j, i, t_1, t_2):
    '''
    INPUT: main loop index, sub loop index, main time, sub time
    OUTPUT: None
    Print current status and time of scraping loop
    '''
    j += 1
    i += 1
    t_1 = (time.time() - t_1) / 60
    t_2 = (time.time() - t_2) / 60
    print '{0}/50 - {1}/5 - sub time: {2:.2f} - total time: {3:.2f}'.format(j, i, t_2, t_1)


def clear_docs_from_dict(clear_dict):
    '''
    INPUT: dictionary
    OUTPUT: None
    Calls clear_docs on extension and files supplied in clear dict
    '''
    for extension, directories in clear_dict.iteritems():
        for directory in directories:
            clear_docs(extension, directory)


def clear_docs(extension, directory):
    '''
    INPUT: file extension, directory
    OUTPUT: None
    Removes all files with given extension from directory
    '''
    print 'removing {0} from {1}'.format(extension, directory)
    for f in os.listdir(directory):
        if f.endswith(extension):
            os.remove(directory + f)


def rename_files(ext, from_dir, to_dir):
    '''
    INPUT: extension, directory, directory
    OUTPUT: None
    Moves files with extension from from_dir to to_dir
    '''
    for fname in os.listdir(from_dir):
        if fname.endswith(ext):
            os.rename(from_dir + fname, to_dir + fname)


def welcome():
    with open("welcome.txt") as f:
        text = f.read()
        print text


def get_words():
    # words = []
    # with open('words.txt') as f:
    #     for line in f:
    #         words.append(line.strip())
    with open('words_by_frequency.txt') as f:
        text = f.read()
    text = text.lower()
    words = re.findall('[a-z]+', text)
    words = [w for w in words if len(w) > 2]
    max_length = max(len(word) for word in words)
    return words, max_length

DICT = enchant.Dict("en_US")

def enchant_text(word):
    if DICT.check(word):
        return word
    else:
        try:
            return DICT.suggest(word)[0]
        except:
            return " "

def clean_text_file():
    with open('clean.txt') as f:
        text = f.read()
    text = text.lower()
    text = re.findall('[a-z]+', text)
    text = [x for x in text if len(x) > 2]

    text = [enchant_text(x) for x in text]
    with open('clean.txt', 'w') as f:
        f.write(' '.join(text).lower())

def update_vocab():
    fname = 'words_by_frequency.txt'
    clean = 'clean.txt'
    with open(clean) as f:
        text = f.read()
    with open(clean, 'w') as f:
        f.write('')

    with open(fname) as f:
        master = f.read()

    master = master + ' ' + text

    with open(fname,'w') as f:
        f.write(master)
    print 'written to {0}'.format(fname)
