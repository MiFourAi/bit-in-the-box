from bitb.md.constants import *
from bitb.md.market_data_base import MarketDataBase

_trade_fields = [TRADE_ID, TIMESTAMP, BUY_ORDER_ID,
                 SELL_ORDER_ID, PRICE, QTY, MAKER_SIDE]


def trade_fields():
  return _trade_fields


class Trade(MarketDataBase):

  def __init__(self, payload):
    super(Trade, self).__init__(MD_TRADE, payload)


def make_trade(trade_id, timestamp, buy_order_id, sell_order_id, price, qty, maker_side, **kwargs):
  assert is_side_encoded(maker_side)
  payload = {
      TRADE_ID: trade_id,
      TIMESTAMP: timestamp,
      BUY_ORDER_ID: buy_order_id,
      SELL_ORDER_ID: sell_order_id,
      PRICE: price,
      QTY: qty,
      MAKER_SIDE: maker_side,
  }
  return Trade(payload)
