import time

PATTERN = "%Y%m%dT%H%M%S"

def to_epoch(s):
	return int(time.mktime(time.strptime(s, PATTERN)))

def from_epoch(s):
	return time.strftime(PATTERN, time.localtime(s))

def testit():
	assert from_epoch(to_epoch("20170104T101010")) == \
		"20170104T101010"

if __name__ == "__main__":
	testit()
