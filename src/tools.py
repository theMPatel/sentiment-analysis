#############################################################################
# 
# Author: Milan Patel
# Purpose: All tools
# Date: 06/05/2018
# 
#############################################################################

import os
import re
import sys
from datetime import datetime

wikitable_xpath ='//table[@class="wikitable"]'
wiki_tracklist_xpath = '//table[@class="tracklist"]'
table_header_xpath = './/th'
table_row_xpath = './/tr'
table_data_xpath = './/td'
descendant_xpath = 'descendant::*'
link_xpath = '//a/@href'


def counter(l):

    counts = {}

    for element in l:

        if element in counts:
            counts[element] += 1

        else:
            counts[element] = 1

    return counts

DATE_FMTS = [
    
    '%m/%d/%Y',
    '%Y/%m/%d',
    '%m-%d-%Y',
    '%Y-%m-%d',
    '%Y'
]

def parse_datetime(string):
    
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(string, fmt).date()

        except:
            continue

    raise ValueError('No available format to parse: {}'.format(
        string))

def get_nonjson(obj):

    if isinstance(obj, set):
        return list(obj)

    return obj

