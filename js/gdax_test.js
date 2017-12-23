const GdaxFeedHandler = require('./gdax_mess.js').GdaxFeedHandler;


var gfh = new GdaxFeedHandler(['BTC-USD'], ['level2']);

gfh.on('message', function(msg) {
  var book = msg.orderBook;
  var bid1 = book.bids.max();
  var ask1 = book.asks.min();
  console.log('bid1: (', bid1.qty, ' @ ', bid1.price.toString(), '), ask1: (', ask1.qty, ' @ ', ask1.price.toString(), ')');
});

gfh.start();