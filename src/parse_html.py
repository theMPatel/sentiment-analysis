#############################################################################
# 
# Author: Milan Patel
# Purpose: Serves as the tools required to parse HTML for this project
# Date: 06/05/2018
# 
#############################################################################

import os
import re
import sys
import logging
import requests
import traceback
from warnings import warn
from html.parser import HTMLParser

_base_url = 'http://en.wikipedia.org/'
_parse_url = 'https://en.wikipedia.org/wiki/List_of_years_in_hip_hop_music'
_year_link_parser = re.compile(r'([0-9]{4})_in_hip_hop_music')
_data_dir = os.path.join(os.getcwd(), 'data_path')

if not os.path.exists(_data_dir):
    os.makedirs(_data_dir)

class WikiLinksParser(HTMLParser):
    """
    Class to specifically parse wiki links for this project
    """

    def __init__(self, *, convert_charrefs=True):
        super().__init__(convert_charrefs=convert_charrefs)
        self._wiki_links = []

    # Overridable -- handle start tag
    def handle_starttag(self, tag, attrs):
        tags_to_process = ('a')
        if tag in tags_to_process:
            # Comes back as a tuple of key-value
            for attr_type, value in attrs:
                if attr_type == 'href':
                    if '/wiki/' in value:
                        self.wiki_links.append(value)

    @property
    def wiki_links(self):
        return self._wiki_links

class HTMLTableCol(object):

    def __init__(self, tag, validator=None):
        self._tag = tag
        self._data = []
        self.set_validator(validator)

    def set_validator(self, validator):

        if callable(validator):
            # Rebind whatever function that is passed to the class
            # so that we can call the validation method directly
            # from the object
            self.validator = validator.__get__(self, type(self))

        else:
            warn('No validation method for table data provided,'
            ' using default', Warning)

    def validator(self, val=None):
        return True

    @property
    def tag(self):
        return self._tag

    @property
    def data(self):
        return self._data

class HTMLTableRow(object):

    def __init__(self):
        self._data = []
    
    def add(self, table_col):
        if not isinstance(table_col, HTMLTableCol):
            raise RuntimeError('Table col must be of type HTMLTableCol'
                ' got {} instead'.format(type(table_col)))

        self._data.append(table_col)

    @property
    def data(self):
        return self._data

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

class HTMLTable(object):

    def __init__(self):
        self._table = []

    def add(self, table_row):
        self._table.append(table_row)

    def __iter__(self):
        return iter(self._table)

table_header_tag = 'th'
table_tag = 'table'
table_data_tag = 'td'
table_row_tag = 'tr'

def check_wiki_table(attrs):

    for attr, value in attrs:
        if attr == 'class' and value == 'wikitable':
            return True

    return False

class WikiTableParser(HTMLParser):

    def __init__(self, convert_charrefs=True):
        super().__init__(convert_charrefs=convert_charrefs)
        self._tables = []
        self._in_table = False
        self._curr_table = None
        self._curr_row = None
        self._curr_col = None

    def handle_starttag(self, tag, attrs):

        print('Start: {}'.format(tag))
        if tag == table_tag:
            if check_wiki_table(attrs):
                self._in_table = True
                self._curr_table = HTMLTable()
                print('\tCreated new table')

        if self._in_table:

            if tag == table_row_tag:
                self._curr_row = HTMLTableRow()
                print('\tCreated new row')

            elif tag in (table_header_tag, table_data_tag):
                self._curr_col = HTMLTableCol(tag)
                print('\tCreated new col')

            if self._curr_col is not None:
                for attr, value in attrs:

                    if attr == 'href':
                        self._curr_col.data.append(value)

    def handle_endtag(self, tag):
        print('End: {}'.format(tag))

        if self._in_table:
            if tag == table_tag:
                self._in_table = False
                self._tables.append(self._curr_table)
                self._curr_table = None
                print('\tClosed table')

            elif tag == table_row_tag:
                self._curr_table.add(self._curr_row)
                self._curr_row = None
                print('\tClosed row')

            elif tag in (table_header_tag, table_data_tag):
                self._curr_row.add(self._curr_col)
                self._curr_col = None
                print('\tClosed col')

    @property
    def tables(self):
        return self._tables
    

def extract_links_match_obj(links, matcher):
    to_return = []
    for l in links:

        match = matcher.search(l)

        if match:
            to_return.append(match.group(0))

    return to_return

def check_html_link(link):
    r = requests.get(link)
    return r.status_code == 200

def get_link_html(link):
    r = requests.get(link)

    if r.status_code == 200:
        return r.text

def parse_html(html):
    pass

def run_workflow(dl_link=_parse_url):
    pass



if __name__ == '__main__':

    to_test = 'https://en.wikipedia.org/wiki/1980_in_hip_hop_music'
    html_data = get_link_html(to_test)

    table_parser = WikiTableParser()
    table_parser.feed(html_data)


    for table in table_parser.tables:

        for row in table:

            for col in row:

                print(col.data)
# if __name__ == '__main__':

#     test_str = '<li><a href="/wiki/1990_in_hip_hop_music" title="1990 in hip hop music">1990 in hip hop music</a></li>'

#     parser = MyHTMLParser()
#     parser.feed(test_str)
#     print(parser._wiki_links)

#     main()