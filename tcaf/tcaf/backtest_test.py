import unittest
from collections import namedtuple
from backtest import *

# For simplicity, each side has two price levels
OrderBookData = namedtuple('OrderBook',
                           ['timestamp',
                            'asks_price_2',
                            'asks_qty_2',
                            'asks_price_1',
                            'asks_qty_1',
                            'bids_price_1',
                            'bids_qty_1',
                            'bids_price_2',
                            'bids_qty_2', ])


class TestBacktest(unittest.TestCase):

  def setUp(self):
    warnings.simplefilter('error')
    self._exchange = 'test_exchange'
    self._product = 'test_coin'
    self._ep = (self._exchange, self._product)
    self._test_book_events = [
        # timestamp, OrderBookData
        (1, OrderBookData(1, 102.0, 2.0, 101.0, 1.0, 100.0, 1.0, 99.0, 2.0)),
        (10, OrderBookData(10, 103.0, 1.0, 102.0, 1.0, 101.0, 1.0, 100.0, 1.0)),
        (20, OrderBookData(20, 101.0, 1.0, 100.0, 2.0, 99.0, 2.0, 98.0, 1.0)),
    ]

  def test_book_is_empty(self):
    bt = BackTest(self._exchange, self._product, 1000.0)
    with self.assertRaises(UserWarning):
      order = OrderInfo('b', 'market', amount=1)
      bt.place_order(order)

  def test_buy_insufficient_capital(self):
    bt = BackTest(self._exchange, self._product, 1000.0)
    bt.update(self._test_book_events[0])
    with self.assertRaises(UserWarning):
      order = OrderInfo('b', 'market', amount=1001.0)
      bt.place_order(order)

  def test_sell_insufficient_position(self):
    bt = BackTest(self._exchange, self._product, 0, position=1.0)
    bt.update(self._test_book_events[0])
    with self.assertRaises(UserWarning):
      order = OrderInfo('s', 'market', amount=1.1)
      bt.place_order(order)

  def test_limit_order_disabled(self):
    bt = BackTest(self._exchange, self._product, 0, position=1.0)
    bt.update(self._test_book_events[0])
    with self.assertRaises(NotImplementedError):
      order = OrderInfo('b', 'limit', price=100, qty=1.0)
      bt.place_order(order)

  def test_buy_1_price_level(self):
    capital, buy_amount = 1000, 101 * 0.5
    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        taker_commission_rate=0)
    bt.update(self._test_book_events[0])

    order = OrderInfo('b', 'market', amount=buy_amount)
    bt.place_order(order)
    self.assertAlmostEqual(bt.position[self._ep], 0.5)
    self.assertAlmostEqual(bt.capital[self._ep], capital - buy_amount)

  def test_buy_2_price_levels(self):
    capital, buy_amount = 1000, 101 * 1.0 + 102 * 1.0
    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        taker_commission_rate=0)
    bt.update(self._test_book_events[0])

    order = OrderInfo('b', 'market', amount=buy_amount)
    bt.place_order(order)
    self.assertAlmostEqual(bt.position[self._ep], 2.0)
    self.assertAlmostEqual(bt.capital[self._ep], capital - buy_amount)

  def test_buy_all(self):
    capital, buy_amount = 1000, 101 * 1.0 + 102 * 4.0
    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        taker_commission_rate=0)
    bt.update(self._test_book_events[0])

    order = OrderInfo('b', 'market', amount=buy_amount)
    bt.place_order(order)
    self.assertAlmostEqual(bt.position[self._ep], 5.0)
    self.assertAlmostEqual(bt.capital[self._ep], capital - buy_amount)

  def test_sell_1_price_level(self):
    capital = 0
    position, sell_qty = 5.0, 0.5
    bt = BackTest(
        self._exchange,
        self._product,
        capital, position=position,
        taker_commission_rate=0)
    bt.update(self._test_book_events[0])

    order = OrderInfo('s', 'market', amount=sell_qty)
    bt.place_order(order)
    self.assertAlmostEqual(bt.position[self._ep], position - sell_qty)
    self.assertAlmostEqual(bt.capital[self._ep], sell_qty * 100.0)

  def test_sell_2_price_levels(self):
    capital = 0
    position, sell_qty = 5.0, 1.0 + 1.0
    bt = BackTest(
        self._exchange,
        self._product,
        capital, position=position,
        taker_commission_rate=0)
    bt.update(self._test_book_events[0])

    order = OrderInfo('s', 'market', amount=sell_qty)
    bt.place_order(order)
    self.assertAlmostEqual(bt.position[self._ep], position - sell_qty)
    self.assertAlmostEqual(bt.capital[self._ep], 1.0 * 100 + 1.0 * 99.0)

  def test_sell_all(self):
    capital = 0
    position, sell_qty = 5.0, 1.0 + 3.0
    bt = BackTest(
        self._exchange,
        self._product,
        capital, position=position,
        taker_commission_rate=0)
    bt.update(self._test_book_events[0])
    order = OrderInfo('s', 'market', amount=sell_qty)
    bt.place_order(order)
    self.assertAlmostEqual(bt.position[self._ep], position - sell_qty)
    self.assertAlmostEqual(bt.capital[self._ep], 1.0 * 100 + 3.0 * 99)

  def test_commission_rate_buy(self):
    capital, buy_amount = 1000, 101 * 0.5
    taker_commission_rate = 0.01

    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        taker_commission_rate=taker_commission_rate)
    bt.update(self._test_book_events[0])
    order = OrderInfo('b', 'market', amount=buy_amount)
    bt.place_order(order)
    self.assertAlmostEqual(
        bt.position[self._ep], 0.5 * (1 - taker_commission_rate))
    self.assertAlmostEqual(bt.capital[self._ep], capital - buy_amount)

  def test_commission_rate_sell(self):
    capital = 0
    position, sell_qty = 5.0, 1.0
    taker_commission_rate = 0.01
    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        position=position,
        taker_commission_rate=taker_commission_rate)
    bt.update(self._test_book_events[0])
    order = OrderInfo('s', 'market', amount=sell_qty)
    bt.place_order(order)
    self.assertAlmostEqual(
        bt.position[self._ep], position - sell_qty)
    self.assertAlmostEqual(
        bt.capital[self._ep], capital + 100.0 * sell_qty * (1 - taker_commission_rate))

  def test_exchange_delay(self):
    # our order will be executed at the second orderbook update
    capital, buy_amount = 1000, 102 * 0.5
    exchange_delay_ms = 5
    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        exchange_delay_ms=exchange_delay_ms,
        taker_commission_rate=0)

    bt.update(self._test_book_events[0])
    order = OrderInfo('b', 'market', amount=buy_amount)
    bt.place_order(order)
    # order has not arrived at the exchange yet
    self.assertAlmostEqual(bt.capital[self._ep], capital)
    self.assertAlmostEqual(bt.position[self._ep], 0)

    # order has arrived
    bt.update(self._test_book_events[1])
    self.assertAlmostEqual(bt.capital[self._ep], capital - buy_amount)
    self.assertAlmostEqual(bt.position[self._ep], 0.5)

  def test_market_buy_sell_seq(self):
    capital = 1000
    position = 0
    bt = BackTest(
        self._exchange,
        self._product,
        capital,
        position=position,
        taker_commission_rate=0)
    orders = [
        OrderInfo('b', 'market', amount=101.0 * 1.0 + 102.0 * 0.5),
        OrderInfo('s', 'market', amount=1.0),
        OrderInfo('b', 'market', amount=100.0 * 1.0)
    ]

    for md, o in zip(self._test_book_events, orders):
      bt.update(md)
      if o.side == 'b':
        bt.place_order(o)
      else:
        bt.place_order(o)
    self.assertAlmostEqual(
        bt.capital[self._ep], capital - (101.0 + 102.0 * 0.5) + 1.0 * 101.0 - 100.0)
    self.assertAlmostEqual(bt.position[self._ep], position + 1.5 - 1.0 + 1.0)

if __name__ == '__main__':
  unittest.main()
