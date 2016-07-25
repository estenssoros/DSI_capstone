from twilio.rest import TwilioRestClient
import json
import os
import pandas as pd

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
