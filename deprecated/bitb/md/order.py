from bitb.md.constants import *
from bitb.md.market_data_base import MarketDataBase


_order_fields = [ORDER_ID, TIMESTAMP, PRICE, QTY, SIDE]


def order_fields():
  return _order_fields


class Order(MarketDataBase):

  def __init__(self, payload):
    super(Order, self).__init__(MD_ORDER, payload)


def make_order(order_id, timestamp, price, qty, side, **kwargs):
  assert is_side_encoded(side)
  payload = {
      ORDER_ID: order_id,
      TIMESTAMP: timestamp,
      PRICE: price,
      QTY: qty,
      SIDE: side,
  }
  return Order(payload)
