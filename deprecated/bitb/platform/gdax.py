import calendar
import time

from bitb.md import *

REMAINING_SIZE = 'remaining_size'
NEW_SIZE = 'new_size'
REASON = 'reason'
MAKER_ORDER_ID = 'maker_order_id'
TAKER_ORDER_ID = 'taker_order_id'


_TS_PATTERN = '%Y-%m-%dT%H:%M:%S.%fZ'
_TS_MICROS_LEN = len('.000000Z')
MILLI_PER_SEC = 1000


def to_epoch_micros_utc(ts):
  micros = ts[-_TS_MICROS_LEN:]
  result = int(calendar.timegm(time.strptime(ts, _TS_PATTERN))) * MILLI_PER_SEC
  result += int(micros[1:-1]) / 1000  # remove heading '.' and trailing 'Z'
  return result


def normalize_md(msg):
  msg_type = msg['type']

  try:
    if msg_type == 'received':
      return normalize_md_received(msg)
    elif msg_type == 'open':
      return normalize_md_open(msg)
    elif msg_type == 'done':
      return normalize_md_done(msg)
    elif msg_type == 'change':
      return normalize_md_change(msg)
    elif msg_type == 'match':
      return normalize_md_trade(msg)
  except:
    print msg
    raise


def normalize_md_received(msg):
  if msg[ORDER_TYPE].lower() == 'market':
    # We only handle Limit orders
    return None

  order_id = msg[ORDER_ID]
  time_since_epoch = to_epoch_micros_utc(msg[TIME])
  price = msg[PRICE]
  qty = msg[SIZE]
  side = encode_side(msg[SIDE])

  order = make_order(order_id=order_id, timestamp=time_since_epoch,
                     price=price, qty=qty, side=side)
  return order


def normalize_md_open(msg):
  order_id = msg[ORDER_ID]

  time_since_epoch = to_epoch_micros_utc(msg[TIME])
  # Limit orders will always have REMAINING_SIZE in it
  new_qty = msg[REMAINING_SIZE]
  order_event = make_order_event(
      order_id=order_id, timestamp=time_since_epoch, new_qty=new_qty, order_event_type=OET_OPEN)
  return order_event


def normalize_md_done(msg):
  if REMAINING_SIZE not in msg:
    # this is not a limit order
    return None

  order_id = msg[ORDER_ID]
  time_since_epoch = to_epoch_micros_utc(msg[TIME])
  new_qty = msg[REMAINING_SIZE]

  reason = msg[REASON].lower()
  assert reason in {'canceled', 'filled'}
  oet = OET_CANCELED if reason == 'canceled' else OET_FILLED
  order_event = make_order_event(
      order_id=order_id, timestamp=time_since_epoch, new_qty=new_qty, order_event_type=oet)
  return order_event


def normalize_md_change(msg):
  if NEW_SIZE not in msg:
    # this is not a limit order
    return None

  order_id = msg[ORDER_ID]
  time_since_epoch = to_epoch_micros_utc(msg[TIME])
  new_qty = msg[NEW_SIZE]
  order_event = make_order_event(
      order_id=order_id, timestamp=time_since_epoch, new_qty=new_qty, order_event_type=OET_CHANGED)
  return order_event


def normalize_md_trade(msg):
  trade_id = msg[TRADE_ID]
  time_since_epoch = to_epoch_micros_utc(msg[TIME])
  price = msg[PRICE]
  qty = msg[SIZE]
  maker_side = msg[SIDE]
  if maker_side == BUY:
    buy_order_id = msg[MAKER_ORDER_ID]
    sell_order_id = msg[TAKER_ORDER_ID]
  else:
    buy_order_id = msg[TAKER_ORDER_ID]
    sell_order_id = msg[MAKER_ORDER_ID]
  maker_side = encode_side(maker_side)
  trade = make_trade(trade_id=trade_id, timestamp=time_since_epoch, buy_order_id=buy_order_id,
                     sell_order_id=sell_order_id, price=price, qty=qty, maker_side=maker_side)
  return trade
