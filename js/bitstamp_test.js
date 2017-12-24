const BitstampFeedHandler = require('./bitstamp_mess.js').BitstampFeedHandler;


var bfh = new BitstampFeedHandler(['btcusd'],
    ['live_trades'])

bfh.on('message', function(msg) {
    console.log(msg);
})

bfh.start();
