'''
LM a python package for webscraping, text segmentation and predictive clustering.
'''


from .LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3, read_from_s3
from .LM_Util import welcome
