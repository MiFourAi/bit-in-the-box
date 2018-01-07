const num = require('num');
const BitfinexFeedHandler = require('./bitfinex_mess.js').BitfinexFeedHandler;
const FixDepthOrderBookConverter = require('./utils.js').FixDepthOrderBookConverter;

const bookDepth = 6;
var bfnfh = new BitfinexFeedHandler(['tBTCUSD'], ['orders', 'book', 'trades']);
var obConverter = new FixDepthOrderBookConverter(bookDepth);

bfnfh.on('message', function(msg) {
	if (msg.md_type == 'OrderBook') {
		ob = obConverter.convert(msg);
		msg.bids = ob.bids;
		msg.asks = ob.asks;
		var itemLog = function(item) {
			process.stdout.write('  ' + num(item.qty).set_precision(7).toString() + ', ' + item.price.toString() + '\n');
		};
		process.stdout.write('Asks: \n');
		ob.asks.reverse().forEach(itemLog);
		process.stdout.write('Bids: \n');
		ob.bids.forEach(itemLog);
	}
});

bfnfh.start();