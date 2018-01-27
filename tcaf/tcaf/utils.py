import unittest
import datetime

EPOCH_START = datetime.datetime(1970, 1, 1)
PATTERN_TIME = '%Y%m%dT%H%M%SZ'
PATTERN_TIME_DOT = '%Y%m%dT%H%M%S.%fZ'
PATTERN_DATE = '%Y%m%d'

def _unify_pattern(s):
	pattern = PATTERN_TIME_DOT if '.' in s else PATTERN_TIME
	if '-' not in s and 'Z' not in s:
		s += 'Z'
	return s, pattern

def _should_unify_timestring(s, orig_s):
	if 'Z' in s and 'Z' not in orig_s:
		return True
	return False

def to_epoch(s):
	'''
	string formatted in YYYYMMDDTmmhhss or YYYYMMDDTmmhhss.fffZ (utc) to epoch
	time in milliseconds
	'''
	s, pattern = _unify_pattern(s)
	return int((datetime.datetime.strptime(s, pattern) -
		EPOCH_START).total_seconds() * 1000)

def from_epoch(s):
	'''
	utc epoch time in milliseconds to string in YYYYMMDDTmmhhss.fffuuuZ format
	'''
	return datetime.datetime.utcfromtimestamp(s / 1000.).strftime(PATTERN_TIME_DOT)

def strpdate(s):
	'''
	date string in YYYYMMDD format to datetime
	'''
	return datetime.datetime.strptime(s, PATTERN_DATE)

def strfdate(s):
	'''
	datetime to string in YYYYMMDD format
	'''
	return datetime.datetime.strftime(s, PATTERN_DATE)

def add_minute(s, minute):
	news, pattern = _unify_pattern(s)
	time_obj = datetime.datetime.strptime(news, pattern)
	time_obj += datetime.timedelta(minutes = minute)
	if _should_unify_timestring(news, s):
		return datetime.datetime.strftime(time_obj, pattern)[:-1]
	return datetime.datetime.strftime(time_obj, pattern)

def add_second(s, second):
	news, pattern = _unify_pattern(s)
	time_obj = datetime.datetime.strptime(news, pattern)
	time_obj += datetime.timedelta(seconds = second)
	if _should_unify_timestring(news, s):
		return datetime.datetime.strftime(time_obj, pattern)[:-1]
	return datetime.datetime.strftime(time_obj, pattern)

def add_hour(s, hour):
	news, pattern = _unify_pattern(s)
	time_obj = datetime.datetime.strptime(news, pattern)
	time_obj += datetime.timedelta(hours = hour)
	if _should_unify_timestring(news, s):
		return datetime.datetime.strftime(time_obj, pattern)[:-1]
	return datetime.datetime.strftime(time_obj, pattern)

class TestDatetimeMethods(unittest.TestCase):

	def test_time(self):
		test_time = '20170104T101010'
		self.assertEqual(
			to_epoch(from_epoch(to_epoch(test_time))),
			to_epoch(test_time))
		test_time = '20170104T101010.111Z'
		self.assertEqual(
			to_epoch(from_epoch(to_epoch(test_time))),
			to_epoch(test_time))

	def test_date(self):
		self.assertEqual(strfdate(strpdate('20170101')), '20170101')

	def test_addition(self):
		self.assertEqual(
			"20171027T000005", add_second("20171027T000000", 5))
		self.assertEqual(
			"20171027T000500", add_minute("20171027T000000", 5))
		self.assertEqual(
			"20171027T050000", add_hour("20171027T000000", 5))

if __name__ == '__main__':
	unittest.main()
