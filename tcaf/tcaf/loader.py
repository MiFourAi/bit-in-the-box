import os
import csv
import itertools
import pandas as pd
import numpy as np

import utils

global __ROOT_PATH
__ROOT_PATH = '/Users/wenshuaiye/Kaggle/bitcoin/data'

def set_path(path):
	global __ROOT_PATH
	__ROOT_PATH = path

def get_path():
	return __ROOT_PATH

class Poller(object):

	def __init__(self, starttime, endtime, ):
		pass
	pass

class Subscriber(object):
	pass

class Subscription(object):
	pass

class CsvQuery(object):

	def __init__(self, starttime, endtime, path):
		self._starttime = starttime
		self._endtime = endtime
		self._path = path


	def _ls_file(self):
		all_files = sorted(os.listdir(self._path))
		start_idx = self._searchsorted(all_files, self._starttime)
		end_idx = self._searchsorted(all_files, self._endtime)
		return all_files[start_idx:end_idx + 1]


	def _searchsorted(self, candidates, target):
		idx = np.searchsorted(candidates, target)
		if (idx < len(candidates) and candidates[idx] == target or
			idx == 0):
			return idx
		else:
			return idx - 1


	def _locate_startpoint(self, timestamps):
		return min(np.searchsorted(
				timestamps, utils.to_epoch(self._starttime)),
			len(timestamps) - 1)


	def _locate_endpoint(self, timestamps):
		return self._searchsorted(
			timestamps, utils.to_epoch(self._endtime)) + 1


	def query(self):
		data = pd.DataFrame()
		all_files = self._ls_file()
		for i in xrange(len(all_files)):
			csvfile = all_files[i]
			cur = pd.read_csv(os.path.join(self._path, csvfile))
			start, end = 0, cur.shape[0]
			if i == 0:
				start = self._locate_startpoint(cur.timestamp.values)
			if i == len(all_files) - 1:
				end = self._locate_endpoint(cur.timestamp.values)
			if start > 0 or end < cur.shape[0]:
				cur = cur.iloc[start:end]
			data = pd.concat([data, cur], copy = False)
		return data


class Query(object):

	def __init__(self, starttime, endtime, exchanges, products):
		self._starttime = starttime
		self._endtime = endtime
		self._exchanges = exchanges
		self._products = products
		self._startdate = starttime.split('T')[0]
		self._enddate = endtime.split('T')[0]

	def _walk_paths(self, table):
		cross_products = itertools.product(
			[table], self._exchanges, self._products)
		paths = [os.path.join(get_path(), '/'.join(list(cp))) for cp in
			cross_products]

		for path in paths:
			if os.path.exists(path):
				for date in sorted(os.listdir(path)):
					if date < self._startdate:
						continue
					if date > self._enddate:
						break
					yield os.path.join(path, date)

	def _create_query_driver(self, table):
		return [CsvQuery(self._starttime, self._endtime, path) for path in
			self._walk_paths(table)]

	def query(self, table):
		query_drivers = self._create_query_driver(table)
		data = pd.DataFrame()
		for driver in query_drivers:
			query_result = driver.query()
			data = pd.concat([data, query_result], copy = False)
		return data if data.shape[0] == 0 else data.sort_values('timestamp')


if __name__ == '__main__':
	query_object = Query(
		'20180105T000000', '20180106T000000', ['bitstamp'], ['btcusd'])
	print query_object.query('Order').shape



