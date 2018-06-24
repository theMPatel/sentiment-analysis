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

from mongoengine.connection import (
	get_db
)

from mongoengine.context_managers import (
	switch_db
)

from datetime import datetime

from tools import (
	popen
)

__all__ = (
	'initialize_mongo_db',
	'initialize_alias',
	'query',
	'insert'
)

logger = logging.getLogger(__name__)
_DEFAULT_MONGO_PATH = os.path.join(os.getcwd(), 'data_dir')
_DB_NAME = 'hippityhoppity'

class Artist(Document):
	name = StringField(max_length=200)
	genius_id = IntField(unique=True, min_value=0)
	song_ids = ListField(field=IntField(min_value=0))

class Song(Document):
	genius_id = IntField(required=True, unique=True, min_value=0)
	release_date = DateTimeField(default=datetime(1500, 1, 1))
	lyrics = StringField()
	album_id = IntField(min_value=0)

class Album(Document):
	genius_id = IntField(unique=True, min_value=0)
	release_date = DateTimeField()
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

	results = popen(mongo_args, comm=False)

	if results[0]:
		raise RuntimeError('Error starting mongodb: {}\n{}'.format(
			results[1],
			results[2]
		))

def initialize_alias(identity):
	"""
	Use this for multi-procing connections to the database. It is
	probably good to use the pid of the process as the alias
	so you don't have to store the data anywhere
	"""
	connect(
		db=_DB_NAME,
		alias=identity
	)

def check_collection(identity, collection):

	if issubclass(collection, Document):
		coll_name = collection._get_collection().name
	elif isisntance(collection, str):
		coll_name = collection
	else:
		raise RuntimeError('Collection references must be string or Document type!')

	db = get_db(identity)

	return coll_name in db.collection_names()

def db_query(identity, collection, **query_args):
	"""
	Use this for querying the database
	"""
	if not check_collection(identity, collection):
		return []

	with switch_db(collection, str(identity)) as interface:
		try:
			return interface.objects(**query_args)
		except:
			return []

def db_update(identity, collection, genius_id, **update_args):

	if not check_collection(identity, collection):
		return False

	with switch_db(collection, str(identity)) as interface:
		success = interface.objects(genius_id=genius_id).update_one(**update_args)

	return success

def db_insert(identity, collection: "Subclassed Document class", **query_args):
	"""
	Takes a collection class definition for selecting the appropriate
	collection to do an insert
	"""

	with switch_db(collection, str(identity)) as interface:

		# First check to see if query returns anything:
		query_results = db_query(identity, collection, **query_args)
		
		# for key, value in query_args.items():

		# 	query_results.extend(
		# 		db_query(identity, collection, **{key:value})
		# 	
		
		if query_results:

			if len(query_results) == 1:

				query = list(query_results).pop()
				return query.modify(query=None, **query_args)
			else:
				raise RuntimeError('Too many already existing records for args: {}'.format(
					str(query_args)))

		try:
			interface(**query_args).save()

		except Exception as e:
			log = logging.getLogger(str(os.getpid()))
			log.exception('Failed to insert '
				'into database: {}'.format(str(query_args)))

			return False

		else:
			return True


if __name__ == '__main__':

	initialize_alias(os.getpid())
	record = Album(genius_id=354556)
