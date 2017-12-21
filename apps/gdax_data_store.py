import time
import gdax
import os
import csv

import bitb.platform.gdax as bitb_gdax
from bitb.md import *
from bitb.util import SynchronousDiskStorage


def mkdir_if_not_exit(path):
  if not os.path.exists(path):
    os.makedirs(path)

# TODO(mike): Extract the interface of dumper


class GdaxCsvMdDumper(object):

  def __init__(self, product_dir):
    import time
    dt_pat = '%Y%m%dT%H%M%S'
    dt_str = time.strftime(dt_pat, time.localtime())
    date_str, time_str = dt_str.split('T')

    self._md_type_to_headers = {
        MD_ORDER: order_fields(),
        MD_ORDER_EVENT: order_event_fields(),
        MD_TRADE: trade_fields(),
    }
    # Directory structure:
    # <product_pair>/
    # |
    # |- <market_data_type>/
    #    |
    #    |- <date>/
    #       |
    #       |- <episode>/
    #          |
    #          |- 1.csv
    #          |- 2.csv
    #          |- ...
    # an episode contains all the data dumped during this dumper's lifetime and is named as 'E' +
    # the time (in %H%M%S format) this dumper is created.
    self._md_type_to_dirs = {md: os.path.join(
        product_dir, md, date_str, 'E' + time_str) for md in [MD_ORDER, MD_ORDER_EVENT, MD_TRADE]}
    for d in self._md_type_to_dirs.values():
      mkdir_if_not_exit(d)

  def dump(self, mixed_md_items, dump_n):
    md_type_to_items = {MD_ORDER: [], MD_ORDER_EVENT: [], MD_TRADE: []}
    for item in mixed_md_items:
      md_type_to_items[item.md_type].append(item.payload)

    for md_type, md_items in md_type_to_items.iteritems():
      try:
        file_dir = self._md_type_to_dirs[md_type]
        headers = self._md_type_to_headers[md_type]
        filename = os.path.join(file_dir, '{}.csv'.format(dump_n))
        with open(filename, 'w') as wf:
          csv_writer = csv.DictWriter(wf, headers)
          csv_writer.writeheader()
          csv_writer.writerows(md_items)
      except:
        import pdb
        pdb.set_trace()


class GdaxFeedStorageHandler(object):

  def __init__(self, product_id, feed_name, storage):
    self._product_id = product_id
    self._feed_name = feed_name
    self._storage = storage
    self._count = 0

  def handle(self, norm_md):
    self._storage.add_item(norm_md)
    self._count += 1
    if self._count % 1000 == 0:
      print 'Handled {} msgs for {}:{}'.format(self._count, self._product_id, self._feed_name)


class WSC(gdax.WebsocketClient):

  def __init__(self, products_to_handlers):
    super(WSC, self).__init__(
        products=products_to_handlers.keys(), should_print=False)
    self._products_to_handlers = products_to_handlers

  def on_message(self, msg):
    product_id = msg['product_id']
    handler = self._products_to_handlers.get(product_id, None)
    if handler is not None:
      norm_md = bitb_gdax.normalize_md(msg)
      if norm_md is not None:
        handler.handle(norm_md)


def _main():
  products = ['BTC-USD', 'BCH-USD', 'ETH-USD', 'LTC-USD']
  capacity = 10000
  products_to_storages = {}
  products_to_handlers = {}

  for p in products:
    dumper = GdaxCsvMdDumper(product_dir=p)
    storage = SynchronousDiskStorage(capacity, dumper)
    handler = GdaxFeedStorageHandler(
        product_id=p, feed_name='full', storage=storage)
    products_to_storages[p] = storage
    products_to_handlers[p] = handler

  ws_client = WSC(products_to_handlers)
  # ws_client = WSC({'BTC-USD': None})
  ws_client.start()

  try:
    while True:
      time.sleep(10)
  except KeyboardInterrupt:
    for storage in products_to_storages.values():
      storage.force_dump()
    ws_client.close()

if __name__ == '__main__':
  _main()
