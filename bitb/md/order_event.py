from bitb.md.constants import *
from bitb.md.market_data_base import MarketDataBase

_order_event_fields = [ORDER_ID, TIMESTAMP, NEW_QTY, ORDER_EVENT_TYPE]


def order_event_fields():
  return _order_event_fields


class OrderEvent(MarketDataBase):

  def __init__(self, payload):
    super(OrderEvent, self).__init__(MD_ORDER_EVENT, payload)


def make_order_event(order_id, timestamp, new_qty, order_event_type, **kwargs):
  assert is_order_event_type_encoded(order_event_type)
  payload = {
      ORDER_ID: order_id,
      TIMESTAMP: timestamp,
      NEW_QTY: new_qty,
      ORDER_EVENT_TYPE: order_event_type,
  }
  return OrderEvent(payload)
