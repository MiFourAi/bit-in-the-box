import os
import csv
import re


def remove_csv_exchange_and_product_id(csv_filepath, out_csv_filepath):
  with open(csv_filepath, 'r') as rf, open(out_csv_filepath, 'w') as wf:
    for row in rf:
      row = row.split(',')
      # Hacky. Assuming that exchange, productID are the first two
      # fields
      row = row[2:]
      row = ','.join(row)
      wf.write(row)

ROOT_DIR = '../..'
EXCHANGES = ['gdax', 'bitstamp']

# %YYYYMMMSS%T%HHmmss%.csv
CSV_FILENAME_PATTERN = re.compile('\d{8}T\d{6}\.csv')


def mkdir_if_not_exist(dirpath):
  if not os.path.isdir(dirpath):
    os.makedirs(dirpath)


def main():
  for exchange in EXCHANGES:
    exchange_path = os.path.join(ROOT_DIR, exchange)
    if os.path.isdir(exchange_path):
      for dirpath, _, filenames in os.walk(exchange_path):
        filenames = filter(
            lambda x: CSV_FILENAME_PATTERN.match(x) is not None, filenames)
        # Output the fils to a different folder with '_fixed' appended to the
        # exchange name. This is to avoid corruptting the source data.
        out_dirpath = dirpath.replace(exchange + '/', exchange + '_fixed/')
        mkdir_if_not_exist(out_dirpath)
        for fn in filenames:
          csv_filepath = os.path.join(dirpath, fn)
          out_csv_filepath = os.path.join(out_dirpath, fn)
          print 'input={}, output={}'.format(csv_filepath, out_csv_filepath)

          remove_csv_exchange_and_product_id(csv_filepath, out_csv_filepath)

if __name__ == '__main__':
  main()
