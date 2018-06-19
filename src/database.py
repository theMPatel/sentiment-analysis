############################################################################
# 
# Author: Milan Patel
# Email: milanpatel94@gmail.com
# 
# Purpose: Module to serve as the functionality for the database connection
# 
############################################################################

import os
import sys
import logging
import subprocess as sp
from mongoengine import (
	Document,
	StringField,
	IntField,
	ListField,
	DateTimeField,
	connect
)
from datetime import datetime

from tools import (
	popen
)

logger = logging.getLogger(__name__)
_DEFAULT_MONGO_PATH = os.path.join(os.getcwd(), 'data_dir')
_DB_NAME = 'hippityhoppity'

class Artist(Document):
	name = StringField(required=True, max_length=200)
	genius_id = IntField(unique=True, min_value=0)
	song_ids = ListField(field=IntField(min_value=0))

class Song(Document):
	genius_id = IntField(required=True, unique=True, min_value=0)
	release_date = DateTimeField(default=datetime(1500, 1, 1))
	lyrics = StringField()
	album_id = IntField(min_value=0)

class Album(Document):
	genius_id = IntField(min_value=0)
	release_date = DateTimeField(default=datetime(1500, 1, 1,))
	song_ids = ListField(field=IntField(min_value=0))

def initialize_mongo_db(directory=_DEFAULT_MONGO_PATH):
	# Initialize the mongo db underneath for serving the database
	if not os.path.exists(directory):
		os.makedirs(directory)

	mongo_args = [
		'mongod',
		'--dbpath',
		directory
	]

	results = popen(mongo_args)

	if results[0]:
		raise RuntimeError('Error starting mongodb: {}\n{}'.format(
			results[1],
			results[2]
		))

def initialize_alias(identity):
	"""
	Use this for multi-procing connections to the database. It is
	best to use 

	connect(
		db=_DB_NAME,
		alias=identity
	)

def query(identity, collection, **query_args):



