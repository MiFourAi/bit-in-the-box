import warnings
from collections import OrderedDict

_MKT = 'market'
_LMT = 'limit'
_EPSILON = 1e-8


class OrderInfo(object):

  def __init__(self, side, type, **kwargs):
    side = side.lower()
    assert side in {'b', 's'}
    self._side = side  # 'b' or 's'

    type = type.lower()
    assert type in {_MKT, _LMT}
    self._type = type  # _MKT or _LMT

    self._args = kwargs

  @property
  def side(self):
    return self._side

  @property
  def type(self):
    return self._type

  @property
  def amount(self):
    assert self._type == _MKT
    return float(self._args['amount'])

  @property
  def price(self):
    assert self._type == _LMT
    return float(self._args['price'])

  @property
  def qty(self):
    assert self._type == _LMT
    return float(self._args['qty'])

  @property
  def buy_amount(self):
    if self._type == _MKT:
      return self.amount
    elif self._type == _LMT:
      return self.price * self.qty
    raise ValueError()

  @property
  def sell_amount(self):
    if self._type == _MKT:
      return self.amount
    elif self._type == _LMT:
      return self.qty
    return ValueError()

  def has(self, name):
    return name in self._args

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

  def cancel(self, oid):
    """
    oid: an OrderID instance.
    """
    raise NotImplementedError()

  def is_open(self, oid):
    """
    oid: an OrderID instance.
    """
    raise NotImplementedError()

  def is_filled(self, oid):
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
    self._filled_order_ids = set()
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

    try:
      self._price_precision = float(kwargs['price_precision'])
    except KeyError:
      self._price_precision = 2

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

  def _fmt_price(self, p):
    prec = self._price_precision
    fmt = '%.' + str(prec) + 'f'
    return fmt % round(float(p), prec)

  def _can_exchange_accept_order(self, order):
    assert self._cur_book is not None

    # Check that we have sufficient capital/position to executue this order.
    if order.side == 'b':
      order_amount = order.buy_amount
      if self._capital < order_amount:
        warnings.warn(
            'Cannot place buy order because of insufficient capital.')
        return False
    else:
      assert order.side == 's'
      order_amount = order.sell_amount
      if self._position < order_amount:
        warnings.warn(
            'Cannot place sell order because of insufficient position.')
        return False

    # Check that limit order prices does not cross the book.
    if order.type == _LMT:
      if order.side == 'b' and order.price >= float(
              self._cur_book.asks_price_1) - _EPSILON:
        warnings.warn(
            'Cannot place buy limit order: price greater than ask price 1.')
        return False
      if order.side == 's' and order.price <= float(
              self._cur_book.bids_price_1) + _EPSILON:
        warnings.warn(
            'Cannot place sell limit order: price smaller than bid price 1.')
        return False

    return True

  def _add_to_open_queue(self, oid, order):
    """
    oid: an int
    order: an OrderInfo instance
    """
    # Remove this check once we pass the tests.
    assert self._can_exchange_accept_order(order)
    assert oid not in self._open_orders, '{} already in open orders.'.format(
        oid)

    self._open_orders[oid] = order

  def _update_cap_or_pos(self, order, order_canceled=False):
    if order.side == 'b':
      amount = order.buy_amount
      amount = -amount if order_canceled else amount
      self._capital -= amount
    else:
      amount = order.sell_amount
      amount = -amount if order_canceled else amount
      self._position -= amount

  def _try_find_price_lv_in_book(self, order):
    price_str = self._fmt_price(order.price)
    price_prefix = 'asks_price_' if order.side == 's' else 'bids_price_'
    book = self._cur_book
    for i in xrange(len(book)):
      if book._fields[i].startswith(price_prefix):
        if self._fmt_price(book[i]) == price_str:
          price_key = book._fields[i]
          qty_key = price_key.replace('price', 'qty')
          return i, price_str, float(getattr(book, qty_key))
    return None

  def _attach_book_snap_info(self, order):
    assert order.type != _MKT

    order.book_seq = self._cur_book.sequenceNo
    res = self._try_find_price_lv_in_book(order)
    if res is None:
      # if we placed the order at a price level that is not recorded on
      # the book, we assume that the order is placed at the head of the
      # queue for that price level.
      order.book_pos = 0
    else:
      order.book_pos = res[2]

  def _try_complete_limit_order(self, oid, order, **kwargs):
    # Sample Trade msg:
    # Trade(exchange='gdax', product='btcusd', exchangeTimestamp='1515571422111', timestamp='1515571422189', sequenceNo='1227482', tradeID='32824839', price='14049.00', qty='0.35679758', makerSide='b', buyOrderID='48469c84-ea90-4ff8-8e73-3faf1429dd9a', sellOrderID='12216ff0-4eb0-45d6-979b-6812ea1e20d1')
    order_filled = False
    order_canceled = False
    try:
      trade = kwargs['trade']
      if order.side == trade.makerSide:
        order_price_str = self._fmt_price(order.price)
        trade_price = float(trade.price)
        trade_price_str = self._fmt_price(trade_price)

        if order_price_str == trade_price_str:
          order.book_pos -= float(trade.qty)
          if order.book_pos + order.qty < _EPSILON:
            order_filled = True
        elif (order.side == 'b' and trade_price < order.price) or (
                order.side == 's' and trade_price > order.price):
          order_filled = True
    except KeyError:
      pass
    # this check makes sure that the order book has changed.
    if order.book_seq < self._cur_book.sequenceNo:
      # Cross book happens, treat the order as executed. This is *conservative*,
      # because think about the case where we place a buy limit order and the
      # ask_price_1 goes below this price: this means that the price is falling
      # yet we are buying high. The better move would be to cancel this order.
      # However, if the strategy can profit in this case, then it might perform
      # better if we choose to cancel instead of fill here.
      if (order.side == 's' and float(self._cur_book.bids_price_1) >= order.price) or (
              order.side == 'b' and float(self._cur_book.asks_price_1) <= order.price):
        order_filled = True
    if order_filled:
      if order.side == 'b':
        # sell_amount is qty
        self._position += order.sell_amount * (1 - self._maker_commission_rate)
      else:
        # buy_amount is qty * price
        self._capital += order.buy_amount * (1 - self._maker_commission_rate)
    else:
      try:
        if self._cur_time - order.timestamp >= order.get('lifetime'):
          # we've decreased the capital/position when this order arrived at the
          # exchange, now we need to revert that.
          self._update_cap_or_pos(order, order_canceled=True)
          order_canceled = True
      except KeyError:
        order_canceled = False
    if order_filled:
      self._filled_order_ids.add(oid.order_id)
    return order_filled or order_canceled

  def _exchange_recv_order(self, oid, order):
    """
    Args:
      oid: an int
      ts: an int, time in milliseconds since epoch
      order: an OrderInfo instance

    Returns:
      boolean: True if it is added to |_open_orders| successfully.
    """
    if self._can_exchange_accept_order(order):
      self._update_cap_or_pos(order)
      order.timestamp = self._cur_time
      if order.type == _MKT:
        # shortcut the open order queue, fill it directly.
        self._fill_market_order(oid, order)
      else:
        self._attach_book_snap_info(order)
        self._add_to_open_queue(oid, order)
      return True
    return False

  def _fill_market_order(self, oid, order):
    """
    This function walks through |current_book| to fill the market |order|.
    """
    book = self._cur_book
    book_fields = book._fields

    if order.side == 'b':
      remaining_amount = order.buy_amount * (1 - self._taker_commission_rate)
      i = 1
      fill_immediately = False
      while remaining_amount > _EPSILON:
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
      remaining_qty = order.sell_amount * (1 - self._taker_commission_rate)
      i = 1
      fill_immediately = False
      while remaining_qty > _EPSILON:
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
    self._filled_order_ids.add(oid.order_id)

  def _drain_pending_queue(self):
    removed_oids = []
    for oid, (ts, order) in self._pending_orders.iteritems():
      if self._cur_time - ts < self._exchange_delay_ms:
        break
      self._exchange_recv_order(oid, order)
      removed_oids.append(oid)
    for oid in removed_oids:
      del self._pending_orders[oid]

  def _process_open_orders(self, **kwargs):
    if len(self._open_orders) == 0:
      return

    assert self._cur_time is not None and self._cur_book is not None

    # Try to fill the open orders
    completed_oids = []
    for oid, order in self._open_orders.iteritems():
      completed = True
      if order.type == _MKT:
        self._fill_market_order(oid, order)
      elif order.type == _LMT:
        completed = self._try_complete_limit_order(oid, order, **kwargs)

      if completed:
        completed_oids.append(oid)

    for oid in completed_oids:
      del self._open_orders[oid]

  def place_order(self, order):
    if self._cur_time is None or self._cur_book is None:
      warnings.warn('Cannot place an order when the book/time is unknown.')
      return None

    oid = self._allocate_order_id()
    if self._exchange_delay_ms == 0:
      self._exchange_recv_order(oid, order)
    else:
      self._pending_orders[oid] = (self._cur_time, order)
    return OrderID(oid, exchange=self.exchange, product=self.product)

  def cancel(self, oid):
    oid = oid.order_id
    if oid in self._open_orders:
      order = self._open_orders[oid]
      self._update_cap_or_pos(order, order_canceled=True)
      del self._open_orders[oid]
      return True
    elif oid in self._pending_orders:
      del self._pending_orders[oid]
      return True
    return False

  def is_open(self, oid):
    return oid.order_id in self._open_orders

  def is_filled(self, oid):
    return oid.order_id in self._filled_order_ids

  def update(self, md):
    """
    md: a 2-tuple. 1st: timestamp str, 2nd: actual market data, all fields are strings
    """
    md = md[1]
    self._cur_time = int(md.timestamp)
    md_type = type(md).__name__
    if md_type == 'OrderBook':
      self._cur_book = md

    self._drain_pending_queue()
    if md_type == 'Trade':
      self._process_open_orders(trade=md)
    else:
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

  def is_filled(self, oid):
    """
    oid: an OrderID instance.
    """
    e, p = oid.get('exchange'), oid.get('product')
    return self._back_tests[(e, p)].is_filled(oid)

  def update(self, md):
    """
    md: a 2-tuple. 1st: timestamp str, 2nd: actual market data, all fields are strings
    """
    e, p = md[1].exchange, md[1].product
    self._back_tests[(e, p)].update(md)

  def cancel(self, oid):
    """
    oid: an OrderID instance
    """
    e, p = oid.get('exchange'), oid.get('product')
    return self._back_tests[(e, p)].cancel(oid)

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
  o1 = OrderInfo('b', _MKT, amount=200, exchange=exchange, product='btcusd')
  o2 = OrderInfo('b', _MKT, amount=200, exchange=exchange, product='ltcusd')
  bt.place_order(o1)
  bt.place_order(o2)

  print bt.capital
  print bt.position
