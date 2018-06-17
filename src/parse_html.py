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
import json
import time
import logging
import requests
import traceback
from tqdm import *
import lxml.html as l_html
from functools import partial
from urllib.parse import urljoin
from collections import defaultdict
from multiprocessing import Pool, Queue, Lock

from tools import (
    get_nonjson
)

_base_url = 'http://en.wikipedia.org/'
_parse_url = 'https://en.wikipedia.org/wiki/List_of_years_in_hip_hop_music'
_year_link_parser = re.compile(r'([0-9]{4})_in_hip_hop_music')
_data_dir = os.path.join(os.getcwd(), 'data_path')
_THREADS = 8

if not os.path.exists(_data_dir):
    os.makedirs(_data_dir)

def replace_quotes(string):
    return string.replace('"', '').replace("'", "")

def replace_space(element):
    return element.text_content().replace(u'\xa0', u' ')

def extract_links_match_obj(links, matcher):
    to_return = []
    for l in links:

        match = matcher.search(l)

        if match:
            to_return.append(l)

    return to_return

def check_html_link(link):
    r = requests.get(link)
    return r.status_code == 200

def get_link_html(link):
    r = requests.get(link)

    if r.status_code == 200:
        return r.text

link_xpath = '//a/@href'
def get_wiki_links(link):
    html = get_link_html(link)
    root = l_html.fromstring(html)
    links = []

    for l in root.xpath(link_xpath):

        if 'wiki' in l:
            links.append(urljoin(_base_url, l))

    return links

def get_year_links(link):
    return extract_links_match_obj(
        get_wiki_links(link),
        _year_link_parser
    )

wikitable_xpath ='//table[@class="wikitable"]'
wiki_tracklist_xpath = '//table[@class="tracklist"]'
table_header_xpath = './/th'
table_row_xpath = './/tr'
table_data_xpath = './/td'
descendant_xpath = 'descendant::*'
def get_tables(link, table_xpath=None, prefer_text=False):

    tables_here = []
    html = get_link_html(link)
    root = l_html.fromstring(html)

    for table in root.xpath(table_xpath):
        tables_here.append(parse_table(table, prefer_text))

    return tables_here

def dict_to_list(dict_table):
    for i, row in sorted(dict_table.items()):
        cols = []

        for j, col in sorted(row.items()):
            cols.append(col)

        yield cols

def parse_table(etable, prefer_text):

    dict_table = defaultdict(lambda: defaultdict(str))
    for row_i, row in enumerate(etable.xpath(table_row_xpath)):

        for col_i, col in enumerate(row.xpath('{}|{}'.format(
            table_data_xpath, table_header_xpath))):

            col_span = int(col.get('colspan', 1))
            row_span = int(col.get('rowspan',1))
            links = []
            text = replace_space(col)
            for info in col.xpath(descendant_xpath):
                if 'href' in info.attrib:
                    links.append(info.get('href'))

            links = [l for l in links if 'wiki' in l]
            links = list(map(lambda tail: urljoin(_base_url, tail), links))

            while row_i in dict_table and col_i in dict_table[row_i]:
                col_i += 1

            for i in range(row_i, row_i + row_span):
                for j in range(col_i, col_i + col_span):

                    if prefer_text:
                        dict_table[i][j] = text
                    
                    elif len(links) == 1:
                        dict_table[i][j] = links[0]
                    
                    elif len(links):
                        dict_table[i][j] = links
                    
                    else:
                        dict_table[i][j] = text

    return list(dict_to_list(dict_table))

def get_table_col(tables, attr):

    items = set()

    for table in tables:
        
        if not table:
            continue

        index = -1
        for i, row in enumerate(table):
            lower = list(map(str.lower, row))
            
            if attr in lower:
                index = i
                break

        if index == -1:
            continue

        attr_index = lower.index(attr)

        for row in table[index:]:

            if attr_index >= len(row):
                continue

            if isinstance(row[attr_index], list):
                items.add(frozenset(row[attr_index]))
            else:
                items.add(row[attr_index])

    final_list = []
    for i in items:

        if isinstance(i, frozenset):
            final_list.append(list(i))
        else:
            final_list.append(i)

    return final_list



def execute(f, out_file, *args, **kwargs):

    if os.path.exists(out_file):
        print('Already processed!')

        with open(out_file, 'r') as f:
            data = json.load(f)

        return data

    tqdm.write('Processing')
    to_dump = f(*args, **kwargs)

    with open(out_file, 'w') as out_f:
        json.dump(to_dump, out_f)

def multi_target(f):

    if callable(f):

        lock.acquire()
        try:
            print('PID: {} working'.format(os.getpid()))

        finally:
            lock.release()

        return f()
    
    return None

def lock_init(l):
    global lock
    lock = l

def multi_exec(f, threads, target_arglist, *f_args, **f_kwargs):

    if not isinstance(threads, int):
        raise RuntimeError('Threads must be int type, got: {}'
            ' instead'.format(type(threads)))

    funcs = [partial(f, a, *f_args, **f_kwargs) for a in target_arglist]

    l = Lock()
    with Pool(processes=threads, initializer=lock_init, initargs=(l,)) as p:
        results = p.map(multi_target, funcs)
    
    return results

def parse_wikipedia(dl_link=_parse_url):

    wikilinks_file = os.path.join(_data_dir, 'wiki_links.json')

    wiki_links_data = execute(
        get_year_links,
        wikilinks_file,
        _parse_url
    )

    artists_table = os.path.join(_data_dir, 'artists.json')

    artists_tables = execute(
        multi_exec,
        artists_table,
        get_tables,
        _THREADS,
        wiki_links_data,
        table_xpath=wikitable_xpath,
        prefer_text=True
    )

    all_artists = os.path.join(_data_dir, 'artists_names.json')

    results = []
    for tables in artists_tables:

        results.extend(
            get_table_col(tables, 'artist')
        )

    with open(all_artists, 'w') as f:
        json.dump(results, f)

    return set(results)

if __name__ == '__main__':
    run_workflow()