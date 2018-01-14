import warnings
from collections import OrderedDict


class OrderInfo(object):

  def __init__(self, side, type, **kwargs):
    side = side.lower()
    assert side in {'b', 's'}
    self._side = side  # 'b' or 's'

    type = type.lower()
    assert type in {'market', 'limit'}
    self._type = type  # 'market' or 'limit'

    self._args = kwargs

  @property
  def side(self):
    return self._side

  @property
  def type(self):
    return self._type

  @property
  def amount(self):
    assert self._type == 'market'
    return float(self._args['amount'])

  @property
  def price(self):
    assert self._type == 'limit'
    return float(self._args['price'])

  @property
  def qty(self):
    assert self._type == 'limit'
    return float(self._args['qty'])

  def get(self, name):
    return self._args[name]


class OrderID(object):

  def __init__(self, oid, **kwargs):
    self._oid = oid
    self._args = kwargs

  @property
  def order_id(self):
    return self._oid

  @property
  def oid(self):
    return self._oid

  def get(self, name):
    return self._args[name]


class IBackTest(object):

  def place_order(self, order):
    """
    order: an OrderInfo instance.
    """
    raise NotImplementedError()

  def is_open(self, oid):
    """
    oid: an OrderID instance.
    """
    raise NotImplementedError()

  def update(self, md):
    raise NotImplementedError()

  @property
  def capital(self):
    raise NotImplementedError()

  @property
  def position(self):
    raise NotImplementedError()


class BackTest(IBackTest):

  def __init__(self, exchange, product, capital, **kwargs):
    super(BackTest, self).__init__()

    self._exchange = exchange
    self._product = product
    self._capital = capital
    self._pending_orders = OrderedDict()
    self._open_orders = OrderedDict()
    self._next_order_id = 1

    self._cur_book = None
    self._cur_time = None

    try:
      self._position = float(kwargs['position'])
    except:
      self._position = 0

    try:
      self._exchange_delay_ms = int(kwargs['exchange_delay_ms'])
    except KeyError:
      self._exchange_delay_ms = 0

    try:
      self._taker_commission_rate = float(kwargs['taker_commission_rate'])
    except KeyError:
      self._taker_commission_rate = 0.0025

    try:
      self._maker_commission_rate = float(kwargs['maker_commission_rate'])
    except KeyError:
      self._maker_commission_rate = 0

  @property
  def exchange(self):
    return self._exchange

  @property
  def product(self):
    return self._product

  @property
  def capital(self):
    return {(self._exchange, self._product): self._capital}

  @property
  def position(self):
    return {(self._exchange, self._product): self._position}

  def _allocate_order_id(self):
    result = self._next_order_id
    self._next_order_id += 1
    return result

  def _raise_on_limit_order(self):
    # TODO(mike) Become smart
    raise NotImplementedError(
        'Author is dumb so limit order is not supported yet...')

  def _move_pending_to_open(self):
    removed_oids = []
    for oid, v in self._pending_orders.iteritems():
      if self._cur_time - v[0] < self._exchange_delay_ms:
        break
      self._open_orders[oid] = v
      removed_oids.append(oid)
    for oid in removed_oids:
      del self._pending_orders[oid]

  def _fill_market_order(self, order):
    """
    This function walks through |current_book| to fill the market |order|.
    """
    book = self._cur_book
    book_fields = book._fields
    eps = 1e-8

    if order.side == 'b':
      remaining_amount = order.amount
      # |order.amount| is amount of the currency (i.e. USD) to spend.
      if self._capital < remaining_amount:
        warnings.warn(
            'Cannot fill buy market order because of insufficient capital.')
        return
      self._capital -= remaining_amount
      remaining_amount *= (1 - self._taker_commission_rate)
      i = 1
      fill_immediately = False
      while remaining_amount > eps:
        price_key = 'asks_price_{}'.format(i)
        qty_key = 'asks_qty_{}'.format(i)
        if price_key not in book_fields or qty_key not in book_fields:
          if i == 1:
            warnings.warn('Order book is crap.')
            break
          # we fill the remaining amount with the last known highest price.
          i -= 1
          fill_immediately = True
          continue
        price = float(getattr(book, price_key))
        qty = float(getattr(book, qty_key))
        filled_amount = min(price * qty, remaining_amount)
        if fill_immediately:
          # This happens when we have exhausted the known book price levels.
          # Therefore we just fill the remaining amount with the last known
          # price.
          filled_amount = remaining_amount
        remaining_amount -= filled_amount
        self._position += (filled_amount / price)
        i += 1

    elif order.side == 's':
      # |order.amount| is qty of the cryptocurrency (i.e. BTC) to spend.
      remaining_qty = order.amount
      if self._position < remaining_qty:
        warnings.warn(
            'Cannot fill sell market order because of insufficient position.')
        return
      self._position -= remaining_qty
      remaining_qty *= (1 - self._taker_commission_rate)
      i = 1
      fill_immediately = False
      while remaining_qty > eps:
        price_key = 'bids_price_{}'.format(i)
        qty_key = 'bids_qty_{}'.format(i)
        if price_key not in book_fields or qty_key not in book_fields:
          if i == 1:
            warnings.warn('Order book is crap.')
            break
          # we fill the remaining qty with the last known lowest price.
          i -= 1
          fill_immediately = True
          continue
        price = float(getattr(book, price_key))
        qty = float(getattr(book, qty_key))
        filled_qty = min(qty, remaining_qty)
        if fill_immediately:
          filled_qty = remaining_qty
        remaining_qty -= filled_qty
        self._capital += (filled_qty * price)
        i += 1

  def _process_open_orders(self):
    if self._cur_book is None or self._cur_time is None:
      warnings.warn(
          'Cannot process any open order because no orderbook data has been received yet.')
      # flush all our open orders
      self._open_orders = OrderedDict()

    # Try to fill the open orders
    removed_oids = []
    for oid, (ts, order) in self._open_orders.iteritems():
      if order.type == 'market':
        self._fill_market_order(order)
      elif order.type == 'limit':
        # TODO(mike):
        # 1. position/captial check
        # 2. if the price crosses the book
        self._raise_on_limit_order()

      removed_oids.append(oid)

    for oid in removed_oids:
      del self._open_orders[oid]

  def place_order(self, order):
    if self._cur_time is None:
      warnings.warn('Cannot place an order when the current time is unknown.')
    # TODO(mike): We should have reduced our capital immediately when placing a
    # buy order to prevent overdraft/reduced position when placing a sell order.
    # However, that is delayed to _process_open_orders. This does not sound a
    # good solution but it reduces complexity for now.
    # The complexity is that: we want to support GTC. Once the order is
    # cancelled, we need to add the capital/position back.
    if order.type == 'limit':
      self._raise_on_limit_order()

    oid = self._allocate_order_id()
    if self._exchange_delay_ms == 0:
      self._open_orders[oid] = (self._cur_time, order)
      self._process_open_orders()
    else:
      self._pending_orders[oid] = (self._cur_time, order)
    return OrderID(oid)

  def is_open(self, oid):
    return oid in self._open_orders

  def update(self, md):
    """
    md: a 2-tuple. 1st: timestamp str, 2nd: actual market data, all fields are strings
    """
    md = md[1]
    self._cur_time = int(md.timestamp)
    md_type = type(md).__name__
    if md_type == 'OrderBook':
      self._cur_book = md

    self._move_pending_to_open()
    self._process_open_orders()


class CompositeBackTest(IBackTest):

  def __init__(self):
    super(CompositeBackTest, self).__init__()
    self._back_tests = {}

  def add_back_test(self, bt):
    assert isinstance(bt, BackTest)
    key = (bt.exchange, bt.product)
    assert key not in self._back_tests
    self._back_tests[key] = bt

  def place_order(self, order):
    e, p = order.get('exchange'), order.get('product')
    return self._back_tests[(e, p)].place_order(order)

  def is_open(self, oid):
    """
    oid: an OrderID instance.
    """
    e, p = oid.get('exchange'), oid.get('product')
    return self._back_tests[(e, p)].is_open(oid)

  def update(self, md):
    """
    md: a 2-tuple. 1st: timestamp str, 2nd: actual market data, all fields are strings
    """
    e, p = md[1].exchange, md[1].product
    self._back_tests[(e, p)].update(md)

  @property
  def capital(self):
    result = {}
    for _, bt in self._back_tests.iteritems():
      result.update(bt.capital)
    return result

  @property
  def position(self):
    result = {}
    for _, bt in self._back_tests.iteritems():
      result.update(bt.position)
    return result

if __name__ == '__main__':
  import loader

  bt1 = BackTest('gdax', 'btcusd', 1000.0)
  bt2 = BackTest('gdax', 'ltcusd', 1000.0)
  bt = CompositeBackTest()
  bt.add_back_test(bt1)
  bt.add_back_test(bt2)

  exchange = 'gdax'
  loader.set_path('../../../captured_md')
  subscription = loader.Subscription('20180110T000000', '20180110T000100')
  subscription.add_subscriber('OrderBook', [exchange], ['btcusd', 'ltcusd'])
  data_stream = subscription.process()

  seen_btc, seen_ltc = False, False
  for e in data_stream:
    if e[1].product == 'ltcusd':
      seen_ltc = True
    if e[1].product == 'btcusd':
      seen_btc = True
    bt.update(e)
    if seen_btc and seen_ltc:
      break
  o1 = OrderInfo('b', 'market', amount=200, exchange=exchange, product='btcusd')
  o2 = OrderInfo('b', 'market', amount=200, exchange=exchange, product='ltcusd')
  bt.place_order(o1)
  bt.place_order(o2)

  print bt.capital
  print bt.position
