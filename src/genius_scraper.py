#############################################################################
# 
# Author: Milan Patel
# Purpose: Serves as the tools required to interface with the genius API
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
import lxml.html as l_html
from functools import partial
from urllib.parse import urljoin
from collections import defaultdict
from multiprocessing import Pool, Queue, Lock

from tools import (
    counter,
    get_nonjson
)


RUNTIME_ARGS = {
    'headers' : {
        'Authorization' : 'Bearer {}'
    },
    'token' : None,
    'session': None,
    'default_sleep': 3
}

GENIUS_BASE = 'http://api.genius.com'
DEFAULT_SLEEP = 2
logger = logging.getLogger(__name__)

def set_globals(**kwargs):
    global RUNTIME_ARGS

    for key, value in kwargs.items():

        if key in RUNTIME_ARGS:
            RUNTIME_ARGS[key] = value

def get_artist(artist_id, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    
    url = urljoin(GENIUS_BASE, 'artists/{}'.format(artist_id))
    session = RUNTIME_ARGS.get('session', None)

    if session is None:
        raise RuntimeError('Session object is None!')

    response = session.get(url, params={'text_format':'plain'}).json()
    
    return response.get('response', None)

def get_artist_songs(artist_id, page, sort='title', per_page=50,
    sleep=DEFAULT_SLEEP):

    time.sleep(sleep)
    url = urljoin(GENIUS_BASE, 'artists/{}/songs'.format(artist_id))
    params = {'sort': sort, 'per_page': per_page, 'page': page}
    session = RUNTIME_ARGS.get('session', None)

    if session is None:
        raise RuntimeError('Session object is None!')

    response = session.get(url, params=params).json()
    
    return response.get('response', None)

def get_song(song_id, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    
    url = urljoin(GENIUS_BASE, '/songs/{}'.format(song_id))
    session = RUNTIME_ARGS.get('session', None)

    if session is None:
        raise RuntimeError('Session object is None!')

    response = session.get(url, params={'text_format': 'plain'}).json()
    
    return response.get('Response', None)

def get_search(query, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    
    payload = {'text_format': 'plain'}
    payload.update({'q' : str(query)})

    search_url = urljoin(GENIUS_BASE, 'search/')

    session = RUNTIME_ARGS.get('session', None)

    if session is None:
        raise RuntimeError('Session object is None!')

    response = session.get(search_url, params=payload).json()

    return response.get('response', None)

def consensus_artist(query):
    response = get_search(query)

    if 'hits' not in response:
        return None

    api_artist_ids = []

    for hit in response.get('hits', []):

        artist_info = hit.get('result', {}).get('primary_artist', {})
        
        if not artist_info:
            continue

        artist_id_path = artist_info.get('api_path', '')

        if not artist_id_path:
            continue

        artist_id = artist_id_path.split('/')[-1]
        api_artist_ids.append(artist_id)

    counts = counter(api_artist_ids)

    true_artist = max(counts, key=lambda k: counts[k])

    return true_artist

def get_songs(artist_id):
    
    next_page = 1
    song_ids = set()

    while next_page:

        response = get_artist_songs(artist_id, str(next_page))
        hits = response.get('songs', [])

        if not hits:
            continue

        for hit in hits:

            song_id = hit.get('id', None)

            if song_id is not None:
                song_ids.add(song_id)
                continue

            song_api_path = hit.get('api_path', None)

            if song_api_path is None:
                continue

            song_id = song_api_path.split('/')[-1]

            if song_id.isdigit():
                song_ids.add(int(song_id))

        next_page = response.get('next_page', None)

    return song_ids

def get_song_date(song_id):

    
def extract_lyrics(artists, genius_api):
    pass



if __name__ == '__main__':

    with open('access_token.txt', 'r') as f:
        ACCESS_TOKEN = f.read().strip()

    RUNTIME_ARGS['headers'].update({
        'Authorization': 'Bearer {}'.format(ACCESS_TOKEN)
        })

    RUNTIME_ARGS['token'] = ACCESS_TOKEN

    RUNTIME_ARGS['session'] = requests.Session()
    RUNTIME_ARGS['session'].headers.update(RUNTIME_ARGS['headers'])

    artist_id = consensus_artist('Kanye west')
    print('Kanye West -> {}'.format(artist_id))

    kwest_songs = get_songs(artist_id)

    with open('Kanye_songs.json', 'w') as f:
        json.dump(kwest_songs, f, default=get_nonjson)

    # headers = {'Authorization': 'Bearer {}'.format(ACCESS_TOKEN)}

    # base_url = 'http://api.genius.com'
    # search_url = urljoin(base_url, 'search')
    # data = {'q': 'Kanye West'}

    # response = requests.get(search_url, params=data, headers=headers)

    # json_d = response.json()

    # with open('test.json', 'w') as f:

    #     json.dump(json_d, f)

    # search_url = urljoin(base_url, 'artists/72/songs')
    # response = requests.get(search_url, headers=headers, params={'text_format':'plain'})

    # json_d = response.json()

    # with open('test_song.json', 'w') as f:

    #     json.dump(json_d, f)