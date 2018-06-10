############################################################################
# 
# Author: Milan Patel
# Email: milanpatel94@gmail.com
# 
# This file serves as the insertion point for the beginning of the analysis
# pipeline
# 
# 
############################################################################

import os
import sys
import logging
from . import parse_html

_LOG_FILE_DIR = os.path.join(os.getcwd(), 'logs')

if not os.path.exists(_LOG_FILE_DIR):
	os.makedirs(_LOG_FILE_DIR)

def set_up_logging(
	fmt='{asctime}\t{levelname}\t{message}',
	datefmt='%Y-%m-%d %H:%M:%S',
	outfile='', 
	style='{'):

	log_file_path = os.path.join(_LOG_FILE_DIR, 'sentiment_log.log')
	
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)

	stream_handler = logging.StreamHandler(stream=sys.stdout)
	file_handler = logging.FileHandler(log_file_path)
	formatter = logging.Formatter(fmt=fmt, datefmt=datefmt, style='{')

	for h in [stream_handler, file_handler]:
		h.setFormatter(formatter)
		logger.setHandler(h)

def main():

	

if __name__ == '__main__':

	ret_val = 0

	try:
		main()

	except:
		logging.logException('There was an error in running this analyis')
		ret_val = 1

	sys.exit(ret_val)