import unittest
from datetime import datetime

EPOCH_START = datetime(1970, 1, 1)
PATTERN_TIME = '%Y%m%dT%H%M%SZ'
PATTERN_TIME_DOT = '%Y%m%dT%H%M%S.%fZ'
PATTERN_DATE = '%Y%m%d'


def to_epoch(s):
	'''
	string formatted in YYYYMMDDTmmhhss or YYYYMMDDTmmhhss.fffZ (utc) to epoch
	time in milliseconds
	'''
	pattern = PATTERN_TIME_DOT if '.' in s else PATTERN_TIME
	if '-' not in s and 'Z' not in s:
		s += 'Z'
	return int((datetime.strptime(s, pattern) -
		EPOCH_START).total_seconds() * 1000)

def from_epoch(s):
	'''
	utc epoch time in milliseconds to string in YYYYMMDDTmmhhss.fffuuuZ format
	'''
	return datetime.utcfromtimestamp(s / 1000.).strftime(PATTERN_TIME_DOT)

def strpdate(s):
	'''
	date string in YYYYMMDD format to datetime
	'''
	return datetime.strptime(s, PATTERN_DATE)

def strfdate(s):
	'''
	datetime to string in YYYYMMDD format
	'''
	return datetime.strftime(s, PATTERN_DATE)


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

if __name__ == '__main__':
	unittest.main()
