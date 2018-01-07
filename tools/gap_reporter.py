import sys
import itertools
import os
from collections import namedtuple
import datetime as dt

sys.path.append('../dcaf/dcaf/')
import utils
import dcaf

dcaf.set_path('../../captured_md')


def _walk_date_paths(md_msg, exchange, product, startdate, enddate):
  base_path = os.path.join(dcaf.get_path(), md_msg, exchange, product)
  if os.path.exists(base_path):
    for date in sorted(os.listdir(base_path)):
      if date < startdate:
        continue
      if date > enddate:
        break
      yield os.path.join(base_path, date)


def _walk_csv_paths(datepath, starttime, endtime):
  def filter_fn(fname):
    if not fname.endswith('.csv'):
      return False
    fname = fname.replace('.csv', '')
    return fname >= starttime and fname <= endtime

  all_csv_files = filter(filter_fn, sorted(os.listdir(datepath)))
  for fname in all_csv_files:
    yield os.path.join(datepath, fname)

CsvInfo = namedtuple(
    'CsvInfo', ['start_seq', 'end_seq', 'start_ts', 'end_ts', 'num_entries'])


def _read_csv_file(csv_filepath):
  with open(csv_filepath, 'rb') as rf:
    lines = rf.readlines()
    headers = lines[0].strip().split(',')
    SEQ_NO_INDEX = headers.index('sequenceNo')
    TS_INDEX = headers.index('timestamp')

    start_seq = int(lines[1].strip().split(',')[SEQ_NO_INDEX])
    end_seq = int(lines[-1].strip().split(',')[SEQ_NO_INDEX])

    start_ts = dt.datetime.fromtimestamp(
        int(lines[1].strip().split(',')[TS_INDEX]) / 1000)  # sec
    end_ts = dt.datetime.fromtimestamp(
        int(lines[-1].strip().split(',')[TS_INDEX]) / 1000)  # sec

    assert end_seq >= start_seq
    return CsvInfo(start_seq, end_seq, start_ts, end_ts, len(lines) - 1)


def log(msg, indent=0):
  print '{}{}'.format(''.join([' ' for _ in xrange(indent)]), msg)

md_msg = 'OrderBook'
exchange = 'gdax'
product_id = 'btcusd'
starttime = '20180105T083119'
endtime = '20180106T000000'


def main():
  startdate = starttime.split('T')[0]
  enddate = endtime.split('T')[0]

  last_seq = float('inf')
  start_file, end_file = None, None
  end_ts = None
  gap_duration = None
  num_entries = 0

  def log_chunk():
    log('{')
    log('start: {}'.format(start_file), indent=2)
    log('end: {}'.format(end_file), indent=2)
    if gap_duration is not None:
      log('gap_since_last: {}'.format(gap_duration), indent=2)
    log('num_entries: {}'.format(num_entries), indent=2)
    log('}')

  for i in _walk_date_paths(md_msg, exchange, product_id, startdate, enddate):
    # print i
    for j in _walk_csv_paths(i, starttime, endtime):
      # print '  {}'.format(j)
      csv_info = _read_csv_file(j)
      start_seq, end_seq, _, _, file_num_entries = csv_info

      if start_seq <= last_seq:
        # when doing the first read, |start_file| is always None, which is not a
        # gap.
        if start_file is not None:
          log_chunk()
          gap_duration = csv_info.start_ts - end_ts

        start_file = j
        num_entries = 0
      last_seq = end_seq
      end_file = j
      end_ts = csv_info.end_ts
      num_entries += file_num_entries

  if start_file is not None:
    log_chunk()

if __name__ == '__main__':
  main()
