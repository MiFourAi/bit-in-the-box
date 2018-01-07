const WebSocket = require('ws');
const num = require('num');
const assert = require('assert');
const RBTree = require('bintrees').RBTree;
const FeedHandler = require('./feed_handler.js').FeedHandler;
const pricef = require('./utils.js').pricef;
const md = require('./market_data.js');

var BitfinexDefaultMsgNormalizer = function(productID, bookDepth) {
	this.exchange = 'bitfinex';
	this.productID = productID.replace('t', '').toLowerCase();
	// session arb. bitfinex publicates duplicated messages via multiple sessions
	this.tradeIDs = {};
	// price == 0 -> order delete
	// price > 0 and order id in this dictionary -> order changed
	// price > 0 and order id not in this dictionary -> order new
	this.rawOrders = {};
	this.sequence = 0;
	this.orderBook = null;
	this.trades = false;
	this.orders = false;
}

BitfinexDefaultMsgNormalizer.prototype._nextSequence = function() {
	var result = this.sequence;
	this.sequence += 1;
	return result;
}

BitfinexDefaultMsgNormalizer.prototype.process = function(msg) {
	if (msg.length > 1 && msg[1] == 'hb') {
		return null;
	}
	const msg_type = msg[0];
    switch (msg_type) {
		case 'book':
			if (this.orderBook !== null) {
				return this._handleOrderBookUpdates(msg);
			} else {
				return this._handleOrderBookSnapshots(msg);
			}
		case 'trades':
			if (this.trades) {
				return this._handleTradeUpdates(msg);
			} else {
				return this._handleTradeSnapshots(msg);
			}
		case 'orders':
			if (this.orders) {
				return this._handleOrderUpdates(msg);
			} else {
				return this._handleOrderSnapshots(msg);
			}
	}
	return null;
}

var makeOrder = function(
	exchange, productID, id, prc, quantity, action, sequenceNo) {
    var options = {
        exchange: exchange,
        productID: productID,
        exchangeTimestamp: null,
        timestamp: (new Date().getTime()),
        sequenceNo: sequenceNo,
        orderID: id,
        side: quantity > 0 ? 'b' : 's',
        price: pricef(prc),
        qty: Math.abs(quantity),
        action: action
    };
    return new md.Order(options);
}

BitfinexDefaultMsgNormalizer.prototype._extractOrderActionPrice = function(orderID, prc) {
	if (orderID in this.rawOrders) {
		if (prc > 0) {
			this.rawOrders[orderID] = prc;
			return {action: 'changed', price: prc};
		} else {
			delete this.rawOrders['orderID'];
			return {action: 'canceled', price: this.rawOrders[orderID]};
		}
	} else {
		if (prc > 0) {
			this.rawOrders[orderID] = prc;
			return {action: 'new', price: prc};
		} else {
			// we miss the order new but receive a order delete.
			return {action: 'canceled', price: null};
		}
	}
}

BitfinexDefaultMsgNormalizer.prototype._handleOrderSnapshots = function(msg) {
	var orders = [];
	for (var snap of msg[1]) {
		var actionPrice = this._extractOrderActionPrice(snap[1][0], snap[1][1]);
		orders.push(makeOrder(
			this.exchange, this.productID, snap[1][0], actionPrice.price,
			snap[1][2], actionPrice.action, this._nextSequence()));
	}
	this.orders = true;
	return orders; 
}

BitfinexDefaultMsgNormalizer.prototype._handleOrderUpdates = function(msg) {
    var actionPrice = this._extractOrderActionPrice(msg[1][0], msg[1][1]);
    var order = makeOrder(
        this.exchange, this.productID, msg[1][0], actionPrice.price,
        msg[1][2], actionPrice.action, this._nextSequence());
    return order;  
}

var makeTrades = function(exchange, productID, msg, sequenceNo) {
	var options = {
		tradeID: msg[0],
		price: pricef(msg[3]),
		qty: Math.abs(msg[2]),
		makerSide: msg[2] > 0 ? 's' : 'b',
		buyOrderID: null,
		sellOrderID: null,
		exchange: exchange,
		productID: productID,
		exchangeTimestamp: msg[1] * 1000,
		timestamp: (new Date().getTime()),
		sequenceNo: sequenceNo,
	};
	return new md.Trade(options);
}

BitfinexDefaultMsgNormalizer.prototype._handleTradeSnapshots = function(msg) {
	var trades = [];
	for (var snap of msg[1]) {
		trades.push(makeTrades(
			this.exchange, this.productID, snap, this._nextSequence()));
	}
	this.trades = true;
	return trades;
}

BitfinexDefaultMsgNormalizer.prototype._handleTradeUpdates = function(msg) {
	var session = msg[1];
	if (session in this.tradeIDs && this.tradeIDs[session] == 1) {
		delete this.tradeIDs[session];
		return null;
	}
	this.tradeIDs[session]++;
	return makeTrades(this.exchange, this.productID, msg[2], this._nextSequence());
}

var makeOrderBook = function(exchange, productID, bids, asks, sequenceNo) {
	var options = {
		exchange: exchange,
		productID: productID,
		exchangeTimestamp: null,
		timestamp: (new Date().getTime()),
		sequenceNo: sequenceNo,
		bids: bids,
		asks: asks
	};
	return new md.OrderBook(options);
}

BitfinexDefaultMsgNormalizer.prototype._handleOrderBookSnapshots = function(msg) {
	var bids = new RBTree(function(a, b) {
		return a.price.cmp(b.price);
	});
	var asks = new RBTree(function(a, b) {
		return a.price.cmp(b.price);
	});
	for (var i = 0; i < msg[1].length; ++i) {
		if (msg[1][i][2] > 0) {
			bids.insert({
				price: pricef(msg[1][i][0]),
				qty: msg[1][i][2],
				cnt: msg[1][i][1],
			});
		} else if (msg[1][i][2] < 0) {
			asks.insert({
				price: pricef(msg[1][i][0]),
				qty: Math.abs(msg[1][i][2]),
				cnt: msg[1][i][1],
			});
		}
	}

	this.orderBook = {
		bids: bids,
		asks: asks,
	}

	return makeOrderBook(this.exchange, this.productID, bids, asks, this._nextSequence());
}

BitfinexDefaultMsgNormalizer.prototype._handleOrderBookUpdates = function(msg) {
	assert(this.orderBook);

	var tree = (msg[1][2] > 0 ? this.orderBook.bids : this.orderBook.asks);
	var price = pricef(msg[1][0]);
	var qty = msg[1][2];
	var cnt = msg[1][1];

	tree.remove({price: price});
	if (!num(cnt).eq(0)) {
		tree.insert({price: price, qty: Math.abs(qty)});
	}

	return makeOrderBook(
		this.exchange, this.productID, this.orderBook.bids, this.orderBook.asks, 
		this._nextSequence());
}

var BitfinexFeedHandler = function(productIDs, channels) {
	FeedHandler.call(this, productIDs, channels);
	this.IdToChannel = {};
	this.IdToProduct = {};
	this.exchange = 'bitfinex';

	this.productToNormalizers = {};
	for (var i = 0; i < productIDs.length; ++i) {
		var p = productIDs[i];
		this.productToNormalizers[p] = new BitfinexDefaultMsgNormalizer(p);
	}
}

BitfinexFeedHandler.prototype = Object.create(FeedHandler.prototype);

BitfinexFeedHandler.prototype._handleBitfinexMsg = function(msg) {
	if ('data' in msg) {
		msg = JSON.parse(msg.data);
	}
	if ('event' in msg) {
		if ('channel' in msg) {
			if (msg.channel == 'book' && msg.prec == 'R0') {
				msg['channel'] = 'orders';
			}
			this.IdToChannel[msg.chanId] = msg.channel;
		}
		if ('symbol' in msg) {
			this.IdToProduct[msg.chanId] = msg.symbol;
		}
	} else if (msg) {
		var product = this.IdToProduct[msg[0]];
		msg[0] = this.IdToChannel[msg[0]];
		msg = this.productToNormalizers[product].process(msg);
		if (msg === null) {
			return;
		} else if (Array.isArray(msg)) {
			for (message of msg) {
				this.eventEmitter.emit('message', message);
			}
		} else {
			this.eventEmitter.emit('message', msg);
		}
	}
}

// Bitfinex uses channel + event to publish messages
BitfinexFeedHandler.prototype.start = function() {
	this.ws = new WebSocket('wss://api.bitfinex.com/ws/2');
	this.ws.onopen = () => {
		for (var chan of this.channels) {
			for (var product of this.productIDs) {
				option = {
					event: 'subscribe',
					channel: chan,
					pair: product,
				};
				if (chan == 'book') {
					option['prec'] = 'P0';
				} else if (chan == 'orders') {
					option['prec'] = 'R0';
					option['channel'] = 'book';
				}
				this.ws.send(JSON.stringify(option));
			}
		}
	}
this.ws.onmessage = this._handleBitfinexMsg.bind(this);
}

module.exports.BitfinexDefaultMsgNormalizer = BitfinexDefaultMsgNormalizer;
module.exports.BitfinexFeedHandler = BitfinexFeedHandler;