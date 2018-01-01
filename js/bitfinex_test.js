const BitfinexFeedHandler = require('./bitfinex_mess.js').BitfinexFeedHandler;

var bfnfh = new BitfinexFeedHandler(['tBTCUSD'], ['orders']);

bfnfh.on('message', function(msg) {
	console.log(msg)
});

bfnfh.start();