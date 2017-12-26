const pusher = require('pusher-js/node');
const num = require('num')
const events = require('events');
const md = require('./market_data.js');
const FeedHandler = require('./feed_handler.js').FeedHandler;
const pricef = require('./utils.js').pricef;
const bitstampPusherKey = 'de504dc5763aeef9ff52';

var BitstampDefaultMsgNormalizer = function(productID, bookDepth) {
    this.exchange = 'bitstamp';
    this.productID = productID;
    this.sequence = 0;
}

BitstampDefaultMsgNormalizer.prototype._nextSequence = function() {
    var result = this.sequence;
    this.sequence += 1;
    return result;
}

BitstampDefaultMsgNormalizer.prototype.process = function(msg) {
    const msg_type = msg['msg_type'];
    switch (msg_type) {
        case 'order_created':
            msg['action'] = 'open';
            return this._handleLiveOrders(msg);
        case 'order_changed':
            msg['action'] = 'change';
            return this._handleLiveOrders(msg);
        case 'order_deleted':
            msg['action'] = 'canceled';
            return this._handleLiveOrders(msg);
        case 'snapshot':
            return this._handleOrderBookSnapshots(msg);
        case 'trade':
            return this._handleTrades(msg);
    }
    return null;
}

var convertSide = function(side) {
    if (side == 0) {
        return 'b';
    } else if (side == 1) {
        return 's';
    }
    throw side;
}

var makeOrder = function(exchange, productID, msg, sequenceNo) {
    var options = {
        exchange: exchange,
        productID: productID,
        exchangeTimestamp: msg.datetime,
        timestamp: (new Date().getTime()),
        sequenceNo: sequenceNo,
        orderID: msg.id,
        side: convertSide(msg.order_type),
        price: pricef(msg.price),
        qty: msg.amount,
        action: msg.action
    };
    return new md.Order(options);
}

BitstampDefaultMsgNormalizer.prototype._handleLiveOrders = function(msg) {
    var order = makeOrder(
        this.exchange, this.productID, msg, this._nextSequence());
    return order;  
}

var makeOrderBook = function(exchange, productID, msg, sequenceNo) {
    var options = {
        exchange: exchange,
        productID: productID,
        exchangeTimestamp: msg.datetime,
        timestamp: (new Date().getTime()),
        sequenceNo: sequenceNo,
        bids: msg.bids,
        asks: msg.asks
    };
    return new md.OrderBook(options);
}

BitstampDefaultMsgNormalizer.prototype._handleOrderBookSnapshots = function(msg) {
    var bids = [];
    var asks = [];
    for (var i = 0; i < msg.bids.length; ++i) {
        bids.push({'price': pricef(msg.bids[i][0]), 'qty': msg.bids[i][1]});
        asks.push({'price': pricef(msg.asks[i][0]), 'qty': msg.asks[i][1]});
    }
    msg['bids'] = bids;
    msg['asks'] = asks;
    return makeOrderBook(this.exchange, this.productID, msg, this._nextSequence());
}

var makeTrade = function(exchange, productID, msg, sequenceNo) {
    var options = {
        tradeID: msg.id,
        price: pricef(msg.price),
        qty: msg.amount,
        makerSide: convertSide(msg.type) == 'b' ? 's' : 'b',
        buyOrderID: msg.buy_order_id,
        sellOrderID: msg.sell_order_id,
        exchange: exchange,
        productID: productID,
        exchangeTimestamp: msg.timestamp,
        timestamp: (new Date().getTime()),
        sequenceNo: sequenceNo,
    };
    return new md.Trade(options);
}

BitstampDefaultMsgNormalizer.prototype._handleTrades = function(msg) {
    msg['side'] = msg.type;
    return makeTrade(this.exchange, this.productID, msg, this._nextSequence());
}

// valid channels: live_orders, order_book, live_trades.
// valid products: btcusd, btceur, eurusd, xrpusd, xrpeur, xrpbtc, ltcusd, ltceur,
// ltcbtc, ethusd, etheur, ethbtc, bchusd, bcheur, bchbtc
var BitstampFeedHandler = function(productIDs, channels) {
    FeedHandler.call(this, productIDs, channels);
    this.channelProducts = [];
    for (var i = 0; i < productIDs.length; ++i) {
        for (var j =0; j < channels.length; ++j) {
            if (productIDs[i] == 'btcusd') {
                this.channelProducts.push(channels[j]);
            } else {
                this.channelProducts.push(channels[j] + '_' + productIDs[i]);
            }
        }
    }
    this.exchange = 'bitstamp';
    this.productToNormalizers = {};

    for (var i = 0; i < productIDs.length; ++i) {
        var p = productIDs[i];
        this.productToNormalizers[p] = new BitstampDefaultMsgNormalizer(p);
    }
}

BitstampFeedHandler.prototype = Object.create(FeedHandler.prototype);

BitstampFeedHandler.prototype._handleOrderCreatedMsg = function(msg) {
    msg['msg_type'] = 'order_created';
    this._handleBitstampMsg(msg);
}

BitstampFeedHandler.prototype._handleOrderChangedMsg = function(msg) {
    msg['msg_type'] = 'order_changed';
    this._handleBitstampMsg(msg);
}

BitstampFeedHandler.prototype._handleOrderDeletedMsg = function(msg) {
    msg['msg_type'] = 'order_deleted';
    this._handleBitstampMsg(msg);
}

BitstampFeedHandler.prototype._handleBookMsg = function(msg) {
    msg['msg_type'] = 'snapshot';
    this._handleBitstampMsg(msg);
}

BitstampFeedHandler.prototype._handleTradeMsg = function(msg) {
    msg['msg_type'] = 'trade';
    this._handleBitstampMsg(msg);
}

BitstampFeedHandler.prototype._handleBitstampMsg = function(msg) {
    // ToDd(Mi): change hardcoded products. find a way to encode
    // products to the message without writing extra callbacks.
    msg = this.productToNormalizers['btcusd'].process(msg);
    if (msg) {
        this.eventEmitter.emit('message', msg);
    }
}

// Bitstamp uses channel + event to publish messages
BitstampFeedHandler.prototype.start = function() {
    this.ws = new pusher(bitstampPusherKey, {
        encrypted: true,
    })
    for (var i = 0; i < this.channelProducts.length; ++i) {
        var name = this.channelProducts[i];
        var channel = this.ws.subscribe(name);
        if (name.indexOf('live_orders') > -1) {
            channel.bind('order_created',
                this._handleOrderCreatedMsg.bind(this));
            channel.bind('order_changed',
                this._handleOrderChangedMsg.bind(this));
            channel.bind('order_deleted',
                this._handleOrderDeletedMsg.bind(this));
        } else if (name.indexOf('order_book') > -1) {
            channel.bind('data', this._handleBookMsg.bind(this));
        } else if (name.indexOf('live_trades') > -1) {
            channel.bind('trade', this._handleTradeMsg.bind(this));
        }
    }
}

module.exports.BitstampDefaultMsgNormalizer = BitstampDefaultMsgNormalizer;
module.exports.BitstampFeedHandler = BitstampFeedHandler;
