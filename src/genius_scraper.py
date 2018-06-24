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
import logging.handlers
import requests
import database
import traceback
import signal
import lxml.html as l_html
from functools import partial
from datetime import datetime
from urllib.parse import urljoin
from collections import defaultdict
from multiprocessing import Process, Queue, Lock, cpu_count

from tools import (
    counter,
    get_nonjson,
    parse_datetime,
    lyrics_xpath
)

from database import (
    initialize_mongo_db,
    initialize_alias,
    db_query,
    db_insert,
    db_update
)

from parse_html import (
    replace_space,
    replace_quotes
)

RUNTIME_ARGS = {
    'headers' : {
        'Authorization' : 'Bearer {}'
    },
    'token' : None,
}

GENIUS_BASE = 'http://api.genius.com'
GENIUS_WEB_BASE = 'http://genius.com'
DEFAULT_SLEEP = 2
PIDS = set()

def set_globals(access_token_path):

    with open(access_token_path, 'r') as f:
        ACCESS_TOKEN = f.read().strip()

    global RUNTIME_ARGS

    RUNTIME_ARGS['headers'].update({
        'Authorization': 'Bearer {}'.format(ACCESS_TOKEN)
        })

    RUNTIME_ARGS['token'] = ACCESS_TOKEN

def get_web_link(url):
    r = requests.get(url)

    if r.status_code == 200:
        return r.content

    else:
        return None

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

    session = requests.Session()
    session.headers.update(RUNTIME_ARGS['headers'])

    response = session.get(url, params=params).json()
    
    return response.get('response', None)

def get_song(song_id, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    
    url = urljoin(GENIUS_BASE, '/songs/{}'.format(song_id))
    
    session = requests.Session()
    session.headers.update(RUNTIME_ARGS['headers'])

    response = session.get(url, params={'text_format': 'plain'}).json()
    
    return response.get('response', None)

def get_album(album_id, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)

    url = urljoin(GENIUS_BASE, 'albums/{}'.format(album_id))

    session = requests.Session()
    session.headers.update(RUNTIME_ARGS['headers'])

    response = session.get(url, params={'text_format':'plain'}).json()

    return response.get('response', None)

def get_search(query, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    
    payload = {'text_format': 'plain'}
    payload.update({'q' : str(query)})

    search_url = urljoin(GENIUS_BASE, 'search/')

    session = requests.Session()
    session.headers.update(RUNTIME_ARGS['headers'])

    response = session.get(search_url, params=payload).json()

    return response.get('response', None)

def consensus_artist(query):

    log = logging.getLogger(str(os.getpid()))
    log.info('POSTing search query to genius...')

    response = get_search(query)

    log.info('Got response from genius!')

    if 'hits' not in response:
        log.info('No hits found for query: {}'.format(query))
        return None

    log.info('Got {} hits from genius'.format(len(response.get('hits'))))

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

    if not counts:
        log.info('Could not resolve true Artist for query: {}'.format(query))
        return None

    true_artist = max(counts, key=lambda k: counts[k])
    log.info('Found true artist {} for query {}'.format(true_artist, query))

    log.info('Attemping to insert into database...')

    success = db_insert(
        str(os.getpid()),
        database.Artist,
        name=query,
        genius_id=true_artist

    )

    if success:
        log.info('Successfully inserted record into database!')
        return true_artist

    else:
        log.info('Failed to insert into database!')
        return None

def get_songs(artist_id):

    log = logging.getLogger(str(os.getpid()))
    log.info('Attempting to get song info for artist: {}'.format(artist_id))
    
    next_page = 1
    song_ids = set()

    while next_page:

        log.info('POSTing artist request to genius')
        response = get_artist_songs(artist_id, str(next_page))
        hits = response.get('songs', [])

        if not hits:
            log.info('No songs found!')
            return None

        log.info('Found {} hits on page: {}'.format(len(hits), next_page))

        for hit in hits:

            song_id = hit.get('id', None)

            if song_id is not None:
                song_ids.add(song_id)
                log.info('Found song id: {} for artist: {}'.format(song_id, artist_id))
                continue

            song_api_path = hit.get('api_path', None)

            if song_api_path is None:
                log.info('No ID found for this hit, continuing')
                continue

            song_id = song_api_path.split('/')[-1]

            if song_id.isdigit():
                song_ids.add(int(song_id))
                log.info('Found song id: {} for artist: {}'.format(song_id, artist_id))

        next_page = response.get('next_page', None)
        log.info('Turning the page to: {}'.format(next_page))

    log.info('Found {} songs for artist: {}'.format(len(song_ids), artist_id))
    log.info('Attempting to write to update database..')

    song_ids = list(song_ids)

    success = db_update(
        str(os.getpid()),
        database.Artist,
        artist_id,
        song_ids=song_ids
    )

    if success:
        log.info('Successfully updated database record for artist: {}'.format(artist_id))
        return song_ids

    else:
        log.info('Failed to update database record for artist: {}'.format(artist_id))
        return None

def get_album_date(album_id):
    
    log = logging.getLogger(str(os.getpid()))
    log.info('POSTing album request to genius')

    album = get_album(album_id).get('album', None)

    if album is None:
        log.info('No album information found!')
        return None

    date_string = album.get('release_date', None)
    date_obj = None

    if date_string:
        log.info('Found date string to parse, parsing...')
        date_obj = parse_datetime(date_string)

    if not date_obj:
        date_parts = album.get('release_date_components', None)

        if not date_parts:
            return None

        date_obj = datetime(**date_parts)

    
    success = db_update(
        str(os.getpid()),
        database.Album,
        album_id,
        release_date=date_obj
    )

    return date_obj

def get_date_info(song):

    log = logging.getLogger(str(os.getpid()))
    date_string = song.get('release_date', None)
    date_obj = None

    if date_string:
        date_obj = parse_datetime(date_string)

    album_info = song.get('album', {})
    if album_info is None:
        album_info = {}

    album_id = album_info.get('id', None)

    # Query the database to see if we can get an album date
    if not date_obj:

        if album_id:

            log.info('No date object, querying databse for album_id')

            results = db_query(
                str(os.getpid()),
                database.Album,
                genius_id=album_id
            )

            if not len(results):
                log.info('Did not have album date information on hand, querying...')
                date_obj = get_album_date(album_id)

            elif len(results) == 1:

                doc = list(results)[0]
                if doc.release_date:
                    date_obj = doc.release_date
                    log.info('Have album release date locally stored!')
                else:
                    log.info('Do not have date information stored')
                    date_obj = get_album_date(album_id)

            else:
                log.info('This should never happen, genius_ids are supposed to be unique!')
                return None

        else:
            if not album_info:
                log.info('Failed to get date from song AND album information')
                return None

    return date_obj

def get_song_info(song_id):


    log = logging.getLogger(str(os.getpid()))

    log.info('Checking to see if we have downloaded this song')

    q_results = db_query(
        str(os.getpid()),
        database.Song,
        genius_id=song_id
    )

    if list(q_results):
        log.info('Already have information for song: {}'.format(song_id))
        return

    log.info('POSTing song request to genius')

    song = get_song(song_id).get('song', None)

    if song is None:
        log.info('No data returned from genius')
        return None

    date_obj = get_date_info(song)
    album_info = song.get('album', {})
    if album_info is None:
        album_info = {}

    album_id = album_info.get('id', None)
    lyrics = extract_lyrics(song)

    success = db_insert(
        str(os.getpid()),
        database.Song,
        genius_id=song_id,
        release_date=date_obj,
        album_id=album_id,
        lyrics=lyrics
    )

    if success:
        log.info('Successfully added found song:'
            ' {} information to database'.format(song_id))
        return success
    else:
        log.info('Failed adding information for'
            ' song: {} to database'.format(song_id))
        return None

meta_matcher = re.compile(r'(\[.*?\])*')
def extract_lyrics(song):

    log = logging.getLogger(str(os.getpid()))
    tail_link = song.get('path', None)
    content = None
    song_id = song.get('id', -1)

    if tail_link is None:
        log.info('Missing web url, could not extract lyrics')

    else:
        log.info('Posting web request to genius...')

        content = get_web_link(urljoin(GENIUS_WEB_BASE, tail_link))

        if content is None:
            log.info('Could not get information for song: {}'.format(song_id))
            return ''

    log.info('Parsing lyric information...')
    root = l_html.fromstring(content)

    results = list(root.xpath(lyrics_xpath))

    if not results:
        log.info('There is no lyrics for song: {}'.format(song_id))
        return 'NO LYRICS'

    final = []
    final.extend(replace_space(lyr) for lyr in results)

    log.info('Found {} lyric instances for song: {}'.format(
        len(final),
        song_id
    ))

    for i in range(len(final)):

        final[i] = meta_matcher.sub('', final[i])
        final[i] = '\n'.join(
            filter(
                lambda string: bool(string.strip()),
                final[i].split('\n')
            )
        )

    return '\n\n'.join(final)

def run_genius_workflow(artist):
    
    try:
        artist_id = consensus_artist(artist)

        if artist_id is None:
            return False

        all_songs = get_songs(artist_id)

        if all_songs is None:
            return False

        for song in all_songs:
            get_song_info(song)

    except:
        log = logging.getLogger(str(os.getpid()))
        log.exception("ERROR!")
        return False

    else:
        return True

def execute(token_path, work_queue, log_queue):

    # Set up the logging (Man I did this the hard way before!)
    # This is so much easier!
    logger = logging.getLogger(str(os.getpid()))
    logger.setLevel(logging.DEBUG)
    
    handler = logging.handlers.QueueHandler(log_queue)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt='%(asctime)s\t%(process)d\t%(funcName)s->%(lineno)d\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    set_globals(token_path)

    # Create a connection for this alias
    initialize_alias('default')
    initialize_alias(str(os.getpid()))

    for artist in iter(work_queue.get, None):

        success = run_genius_workflow(artist)

        if not success:
            logger.error('Failed: {}'.format(artist))


# def testing(artist='Kanye West'):

#     logger = logging.getLogger(str(os.getpid()))
#     logger.setLevel(logging.DEBUG)
#     handler = logging.StreamHandler(sys.stdout)
#     handler.setLevel(logging.DEBUG)
#     logger.addHandler(handler)

#     set_globals(os.path.join(os.getcwd(), 'access_token.txt'))
#     initialize_mongo_db()
#     initialize_alias('default')
#     initialize_alias(str(os.getpid()))
#     print(run_genius_workflow(artist))

def main(out_path, artists_name_file, access_token_path):

    # Make sure the database is live
    initialize_mongo_db()

    cpus = cpu_count()-1

    work_queue = Queue()
    log_queue = Queue()

    log_file_name = os.path.join(out_path, 'genius_scraper.log')

    file_handler = logging.FileHandler(log_file_name)   
    stream_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        fmt='%(asctime)s\t%(process)d\t%(funcName)s->%(lineno)d\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    for h in [file_handler, stream_handler]:
        h.setLevel(logging.DEBUG)
        h.setFormatter(formatter)


    children = [
        Process(
            target=execute, 
            args=(access_token_path, work_queue, log_queue)
        ) for _ in range(cpus)
    ]

    with open(artists_name_file, 'r') as f:
        all_artists = json.load(f)

    for artist in set(all_artists):

        if 'artist' not in artist.lower():
            work_queue.put(artist)

    for _ in range(cpus*2):
        work_queue.put(None)


    for proc in children: proc.daemon=True
    for proc in children: proc.start()

    global PIDS

    for proc in children: PIDS.add(proc.pid)

    queue_listener = logging.handlers.QueueListener(
        log_queue, file_handler, stream_handler)

    queue_listener.start()

    for proc in children: proc.join()

    time.sleep(10)

    queue_listener.stop()

if __name__ == '__main__':

    retval = 0
    try:

        main(
            os.getcwd(),
            '/Users/milanpatel/Documents/projects/sentiment-analysis/src/data_path/artists_names.json',
            '/Users/milanpatel/Documents/projects/sentiment-analysis/src/access_token.txt'
        )
    except:
        traceback.print_exc()

        for pid in PIDS:
            os.kill(pid, signal.SIGKILL)

        retval = 1

    finally:
        sys.exit(retval)

