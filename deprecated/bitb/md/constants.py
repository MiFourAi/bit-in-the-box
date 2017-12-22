# TODO(mike): needs more refactor!

# Type of market data
MD_TYPE = 'market_data_type'

MD_ORDER = 'Order'
MD_ORDER_EVENT = 'OrderEvent'
MD_TRADE = 'Trade'

# Common MD field names/values
ORDER_ID = 'order_id'
PRICE = 'price'
QTY = 'qty'
NEW_QTY = 'new_qty'
SIDE = 'side'
ORDER_TYPE = 'order_type'  # market/limit/ioc ...
TIME = 'time'
TIMESTAMP = 'timestamp'

TRADE_ID = 'trade_id'
BUY_ORDER_ID = 'buy_order_id'
SELL_ORDER_ID = 'sell_order_id'
MAKER_SIDE = 'maker_side'

BUY = 'buy'
SELL = 'sell'
BID = 'bid'
ASK = 'ask'

SIDE_BUY = 'b'
SIDE_SELL = 's'

ORDER_EVENT_TYPE = 'order_event_type'
# enum value of order events
OET_OPEN = 'op'
OET_CREATED = 'op'
OET_CANCELED = 'cx'
OET_CHANGED = 'ch'
OET_FILLED = 'fl'

# Exchange: gdax,
SIZE = 'size'

# Exchange: bitstamp,
AMOUNT = 'amount'


def is_side_encoded(side):
  return side in {SIDE_BUY, SIDE_SELL}


def encode_side(side_str):
  side_str = side_str.lower()
  m = {BUY: SIDE_BUY, BID: SIDE_BUY, SELL: SIDE_SELL, ASK: SIDE_SELL}
  try:
    return m[side_str]
  except KeyError:
    raise ValueError('Unknown side={}'.format(side_str))


def is_order_event_type_encoded(oet):
  return oet in {OET_OPEN, OET_CREATED, OET_CANCELED, OET_CHANGED, OET_FILLED}
