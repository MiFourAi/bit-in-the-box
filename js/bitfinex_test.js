const BitfinexFeedHandler = require('./bitfinex_mess.js').BitfinexFeedHandler;

var bfnfh = new BitfinexFeedHandler(['tBTCUSD'], ['orders', 'book', 'trades']);

bfnfh.on('message', function(msg) {
	console.log(msg)
});

bfnfh.start();