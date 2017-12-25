const moment = require('moment');
const num = require('num');
const events = require('events');
const assert = require('assert');
const RBTree = require('bintrees').RBTree;
const Gdax = require('gdax');
const FeedHandler = require('./feed_handler.js').FeedHandler;
const pricef = require('./utils.js').pricef;
const md = require('./market_data.js');

///////////////////////////////////////////////////////////////

var convertExchangeTimestamp = function(ts) {
  return moment(ts).toDate().getTime();
}

var abbreviateSide = function(side) {
  side = side.toLowerCase();
  if (side === 'buy' || side === 'bid') {
    return 'b';
  } else if (side === 'sell' || side === 'ask') {
    return 's';
  }
  throw side;

}

///////////////////////////////////////////////////////////////

// Notes:
// - This class is STATEFUL.
// - Should be used per product, because each product needs its own order-book.

var GdaxDefaultMsgNormalizer = function(productID) {
  this.exchange = 'gdax';
  this.productID = productID;
  this.sequence = 0;
  this.orderBook = null;
}

GdaxDefaultMsgNormalizer.prototype._nextSequence = function() {
  var result = this.sequence;
  this.sequence += 1;
  return result;
}

GdaxDefaultMsgNormalizer.prototype.process = function(msg) {
  assert(msg.product_id == this.productID);

  const msg_type = msg['type'];
  switch (msg_type) {
    case 'open':
      return this._handleOrderOpen(msg);
    case 'done':
      return this._handleOrderDone(msg);
    case 'changed':
      return this._handleOrderChanged(msg);
    case 'match':
      return this._handleMatch(msg);
    case 'snapshot':
      return this._handleOrderBookSnapshot(msg);
    case 'l2update':
      return this._handleOrderBookUpdate(msg);
  }
  return null;
}

var makeOrder = function(exchange, productID, msg, qty, action, sequenceNo) {
  var options = {
    exchange: exchange,
    productID: productID,
    exchangeTimestamp: convertExchangeTimestamp(msg.time),
    timestamp: (new Date().getTime()),
    sequenceNo: sequenceNo,
    orderID: msg.order_id,
    side: abbreviateSide(msg.side),
    price: pricef(msg.price),
    qty: msg.remaining_size,
    action: action
  };
  return new md.Order(options);
}

GdaxDefaultMsgNormalizer.prototype._handleOrderOpen = function(msg) {
  var order = makeOrder(this.exchange, this.productID, msg, msg.remaining_size, 'open', 
    this._nextSequence());
  return order;
}

GdaxDefaultMsgNormalizer.prototype._handleOrderDone = function(msg) {
  if ((!'price' in msg) || (!'remaining_size' in msg)) {
    return null;
  }

  var order = makeOrder(this.exchange, this.productID, msg, msg.remaining_size, msg.reason, 
    this._nextSequence());
 return order;
}

GdaxDefaultMsgNormalizer.prototype._handleOrderChanged = function(msg) {
  if ((!'old_size' in msg) || (!'new_size' in msg)) {
    return null;
  }

  var order = makeOrder(this.exchange, this.productID, msg, msg.new_size, 'change', 
    this._nextSequence());
  return order;
}

GdaxDefaultMsgNormalizer.prototype._handleMatch = function(msg) {
  const makerSide = abbreviateSide(msg.side);
  var buyOrderID;
  var sellOrderID;
  if (makerSide == 'b') {
    buyOrderID = msg.maker_order_id;
    sellOrderID = msg.taker_order_id;
  } else {
    buyOrderID = msg.taker_order_id;
    sellOrderID = msg.maker_order_id;
  }

  var tradeOptions = {
    tradeID: msg.trade_id,
    price: pricef(msg.price),
    qty: msg.size,
    makerSide: makerSide,
    buyOrderID: buyOrderID,
    sellOrderID: sellOrderID,
    exchange: this.exchange,
    productID: this.productID,
    exchangeTimestamp: convertExchangeTimestamp(msg.time),
    timestamp: (new Date().getTime()),
    sequenceNo: this._nextSequence()
  };
  return new md.Trade(tradeOptions);
}

var makeOrderBook = function(exchange, productID, bids, asks, sequenceNo, exchangeTimestamp) {
  var options = {
    exchange: exchange,
    productID: productID,
    exchangeTimestamp: exchangeTimestamp,
    timestamp: (new Date().getTime()),
    sequenceNo: sequenceNo,
    bids: bids,
    asks: asks
  };
  return new md.OrderBook(options);
}

GdaxDefaultMsgNormalizer.prototype._handleOrderBookSnapshot = function(msg) { 
  var msgBids = msg.bids;
  var bids = new RBTree(function(a, b) {
    return a.price.cmp(b.price);
  });
  for (var i = 0; i < msgBids.length; ++i) {
    bids.insert({price: pricef(msgBids[i][0]), qty: msgBids[i][1]});
  }

  var msgAsks = msg.asks;
  var asks = new RBTree(function(a, b) {
    return a.price.cmp(b.price);
  })
  for (var i = 0; i < msgAsks.length; ++i) {
    asks.insert({price: pricef(msgAsks[i][0]), qty: msgAsks[i][1]});
  }

  this.orderBook = {
    bids: bids,
    asks: asks,
  };

  return makeOrderBook(this.exchange, this.productID, bids, asks, this._nextSequence(), 
    /*exchangeTimestamp=*/0);
}

GdaxDefaultMsgNormalizer.prototype._handleOrderBookUpdate = function(msg) { 
  assert(this.orderBook);

  for (var i = 0; i < msg.changes.length; ++i) {
    var change = msg.changes[i];
    var side = abbreviateSide(change[0]);

    var tree = (side == 'b' ? this.orderBook.bids : this.orderBook.asks);
    var price = pricef(change[1]);
    var qty = change[2];

    // RBTree will fail the insertion if it already contains the key. Therefore we need to remove
    // first, then insert if qty > 0.
    tree.remove({price: price});
    if (!num(qty).eq(0)) {
      tree.insert({price: price, qty: qty});
    }
  }
  
  return makeOrderBook(this.exchange, this.productID, this.orderBook.bids, this.orderBook.asks, 
    this._nextSequence(), convertExchangeTimestamp(msg.time));
}

///////////////////////////////////////////////////////////////

// Convert the RBTree based gdax OrderBook to list based.
var GdaxFixDepthOrderBookConverter = function(bookDepth) {
  this.bookDepth = bookDepth || 30;
}

GdaxFixDepthOrderBookConverter.prototype.convert = function(orderBook) {
  var bidsTree = orderBook.bids, asksTree = orderBook.asks;
  const depth = Math.min(this.bookDepth, bidsTree.size, asksTree.size);
  if (depth !== this.bookDepth) {
    console.log('Warning! depth=', depth, ' cannot match configured bookDepth=', this.bookDepth);
  }

  var bids = [], asks = [];
  var itr = bidsTree.iterator();
  for (var i = 0; i < depth; ++i) {
    var item = itr.prev();
    bids.push(item);
  }

  itr = asksTree.iterator();
  for (var i = 0; i < depth; ++i) {
    var item = itr.next();
    asks.push(item);
  }

  return {bids: bids, asks: asks};
}

///////////////////////////////////////////////////////////////

// Notes: 
// - the usage of |channels| requires modifying gdax's package. gdax's Websocket doesn't 
// support customized channels yet.
// - valid products: 'BTC-USD', 'BCH-USD', 'ETH-USD', 'LTC-USD', ...
// - valid channels: 'full', 'level2'
var GdaxFeedHandler = function(productIDs, channels) {
  FeedHandler.call(this, productIDs, channels);
  this.exchange = 'gdax';
  
  this.productToNormalizers = {};
  for (var i = 0; i < productIDs.length; ++i) {
    var p = productIDs[i];
    this.productToNormalizers[p] = new GdaxDefaultMsgNormalizer(p);
  }
}

GdaxFeedHandler.prototype = Object.create(FeedHandler.prototype);

GdaxFeedHandler.prototype._handleGdaxMsg = function(msg) {
  if (msg.type == 'subscriptions' || !'product_id' in msg) {
    return;
  }

  msg = this.productToNormalizers[msg.product_id].process(msg);
  if (msg) {
    this.eventEmitter.emit('message', msg);
  }
}

GdaxFeedHandler.prototype.start = function() {
  if (this.ws) {
    this.ws.disconnect();
    this.ws.removeAllListeners();
  }

  this.ws = new Gdax.WebsocketClient(this.productIDs, this.channels);
  this.ws.on('message', this._handleGdaxMsg.bind(this));
}

module.exports.GdaxDefaultMsgNormalizer = GdaxDefaultMsgNormalizer;
module.exports.GdaxFixDepthOrderBookConverter = GdaxFixDepthOrderBookConverter;
module.exports.GdaxFeedHandler = GdaxFeedHandler;