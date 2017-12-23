const assert = require('assert');

// 这个模块基本相当于对传入的参数pack, |options|, 套了一个class. 它不进行任何data validation/transformation. 平台相关的validation/transformation逻辑应该包含在各自平台的模块中.

class MDBase {
	constructor(options) {
		assert(options);
		assert('exchange' in options);
		assert('productID' in options);
		assert('exchangeTimestamp' in options);
		assert('timestamp' in options);
		assert('sequenceNo' in options);

		this.exchange = options.exchange;
    this.productID = options.productID;
    this.exchangeTimestamp = options.exchangeTimestamp;
    this.timestamp = options.timestamp;
    this.sequenceNo = options.sequenceNo;
	}
}

class Order extends MDBase {
	constructor(options) {
		super(options);

		assert('orderID' in options);
		assert('side' in options);
		assert('price' in options);
		assert('qty' in options);
		assert('action' in options);

		this.orderID = options.orderID;
    this.side = options.side;
    this.price = options.price;
    this.qty = options.qty;
    this.action = options.action;
	}

	get md_type() {
		return 'Order';
	}
}

class OrderBook extends MDBase {
	constructor(options) {
		super(options);

		assert('bids' in options);
		assert('asks' in options);

		this.bids = options.bids;
		this.asks = options.asks;
	}

	get md_type() {
		return 'OrderBook';
	}
}

class Trade extends MDBase {
	constructor(options) {
		super(options);

		assert('tradeID' in options);
		assert('price' in options);
		assert('qty' in options);
		assert('makerSide' in options);
		// TODO(mike): maybe we should put the following two fields into another subclass of Trade.
		assert('buyOrderID' in options);
		assert('sellOrderID' in options);

	  this.tradeID = options.tradeID;
    this.price = options.price;
    this.qty = options.qty;
    this.makerSide = options.makerSide;
    this.buyOrderID = options.buyOrderID;
    this.sellOrderID = options.sellOrderID;
	}
	
	get md_type() {
		return 'Trade';
	}
}

module.exports.Order = Order;
module.exports.OrderBook = OrderBook;
module.exports.Trade = Trade;
