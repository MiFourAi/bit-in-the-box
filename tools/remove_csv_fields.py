import os
import csv
import re


def remove_csv_exchange_and_product_id(csv_filepath):
  tmp_csv_filepath = csv_filepath + '.tmp'
  with open(csv_filepath, 'r') as rf, open(tmp_csv_filepath, 'w') as wf:
    for row in rf:
      row = row.split(',')
      # Hacky. Assuming that exchange, productID are the first two
      # fields
      row = row[2:]
      row = ','.join(row)
      wf.write(row)
  os.rename(tmp_csv_filepath, csv_filepath)

ROOT_DIR = '..'
EXCHANGES = ['gdax', 'bitstamp']

# %YYYYMMMSS%T%HHmmss%.csv
CSV_FILENAME_PATTERN = re.compile('\d{8}T\d{6}\.csv')


def main():
  for exchange in EXCHANGES:
    exchange_path = os.path.join(ROOT_DIR, exchange)
    if os.path.isdir(exchange_path):
      for dirpath, _, filenames in os.walk(exchange_path):
        filenames = filter(
            lambda x: CSV_FILENAME_PATTERN.match(x) is not None, filenames)
        for fn in filenames:
          csv_filepath = os.path.join(dirpath, fn)
          print csv_filepath
          # IMPORTANT!
          # By default the call to remove_csv_exchange_and_product_id is turned
          # off, before enabling it:
          # 1. Make sure you backup the data before using this script!
          # 2. Make sure that ALL the csv files under the exchange directory have
          # "exhange,productID" as the first two fields. This script is very hacky
          # and it could cause great damage to the data if not properly used.

          # remove_csv_exchange_and_product_id(csv_filepath)

if __name__ == '__main__':
  main()
