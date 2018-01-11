import os
import csv
import itertools
import pandas as pd
import numpy as np
from collections import namedtuple
from collections import defaultdict
from heapq import *

import utils



global __ROOT_PATH
__ROOT_PATH = '/Users/wenshuaiye/Kaggle/bitcoin/data'

def set_path(path):
	global __ROOT_PATH
	__ROOT_PATH = path

def get_path():
	return __ROOT_PATH

def walk_date_paths(base_path, startdate, enddate):
	if os.path.exists(base_path):
		for date in sorted(os.listdir(base_path)):
			if date < startdate:
				continue
			if date > enddate:
				break
			yield os.path.join(base_path, date)


class Subscription(object):

	def __init__(self, starttime, endtime):
		self._starttime = starttime
		self._endtime = endtime
		self._subscribers = {}


	def add_subscriber(self, table, exchanges, products):
		self._subscribers[table] = Subscriber(
				self._starttime,
				self._endtime,
				exchanges,
				products,
				table).process()


	def process(self):
		data_stream = []
		for k, subscriber in self._subscribers.iteritems():
			try:
				entry = subscriber.next()
				heappush(data_stream, entry)
			except StopIteration:
				pass
		
		while len(data_stream) > 0:
			entry = heappop(data_stream)
			table = entry[1].__class__.__name__
			try:
				new_entry = self._subscribers[table].next()
				heappush(data_stream, new_entry)
				yield entry
			except StopIteration:
				pass


class Subscriber(object):

	def __init__(self, starttime, endtime, exchanges, products, table):
		self._starttime = starttime
		self._endtime = endtime
		self._exchanges = exchanges
		self._products = products
		self._startdate = starttime.split('T')[0]
		self._enddate = endtime.split('T')[0]
		self._table = table


	def _walk_paths(self):
		path_dict = defaultdict(list)
		for exchange in self._exchanges:
			for product in self._products:
				path = os.path.join(get_path(), self._table, exchange, product)
				for p in walk_date_paths(path, self._startdate, self._enddate):
					path_dict[(exchange, product)].append(p)

		return path_dict


	def process(self):
		path_dict = self._walk_paths()

		drivers = {}
		data_stream = []
		for k, paths in path_dict.iteritems():
			drivers[k] = CsvDriver(
				self._starttime,
				self._endtime,
				paths.pop(0),
				self._table).poll()

		while len(drivers) > 0:
			deleted_drivers = set()
			for key, driver in drivers.iteritems():
				try:
					entry = driver.next()
				except StopIteration:
					if len(path_dict[key]) > 0:
						drivers[key] = CsvDriver(
							self._starttime,
							self._endtime,
							path_dict[key].pop(0),
							self._table).poll()
					else:
						deleted_drivers.add(key)
						continue
				heappush(data_stream, (getattr(entry, 'timestamp'), entry))
			for key in deleted_drivers:
				drivers.pop(key)

			if len(data_stream) > 0:
				yield heappop(data_stream)


class CsvDriver(object):

	def __init__(self, starttime, endtime, path, table_name):
		self._starttime = starttime
		self._endtime = endtime
		self._path = path
		self._table_name = table_name


	def _ls_file(self):
		all_files = filter(
			lambda x: x.endswith('.csv'), sorted(os.listdir(self._path)))
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


	def poll(self):
		all_files = self._ls_file()
		for i in xrange(len(all_files)):
			csvfile = os.path.join(self._path, all_files[i])
			with open(csvfile, 'rb') as csvf:
				reader = csv.reader(csvf, delimiter=',')
				header = next(reader)
				table = namedtuple(self._table_name, header)
				for row in reader:
					yield table._make(row)


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
		paths = [os.path.join(get_path(), *(list(cp))) for cp in
			cross_products]

		for path in paths:
			for p in walk_date_paths(path, self._startdate, self._enddate):
				yield p

	def _create_query_driver(self, table):
		return [CsvDriver(self._starttime, self._endtime, path, table) for
			path in self._walk_paths(table)]

	def query(self, table):
		query_drivers = self._create_query_driver(table)
		data = pd.DataFrame()
		for driver in query_drivers:
			query_result = driver.query()
			data = pd.concat([data, query_result], copy = False)
		return data if data.shape[0] == 0 else data.sort_values('timestamp')


def main():
	#query_object = Query(
	#	'20180105T000000', '20180106T000000', ['bitstamp'], ['btcusd'])
	#print(query_object.query('Order').shape)
	#test_path = \
	#	"/Users/wenshuaiye/Kaggle/bitcoin/data/Order/bitstamp/btcusd/20180107"
	#csv_driver = CsvDriver(
	#	'20180108T000000', '20180108T00300', test_path, 'Order')
	#print(csv_driver.stream())
	query_object = Subscriber(
		'20180105T000000',
		'20180105T000100',
		['bitstamp'],
		['btcusd'],
		'Order').process()

	print(query_object.next())
	print(query_object.next())
	print(query_object.next())

	subscription = Subscription('20180105T000000', '20180105T000100')
	subscription.add_subscriber('Order', ['bitstamp'], ['btcusd'])
	data_stream = subscription.process()
	print(data_stream.next())
	print(data_stream.next())
	print(data_stream.next())

if __name__ == '__main__':
	main()
