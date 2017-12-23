const GdaxFeedHandler = require('./gdax_mess.js').GdaxFeedHandler;


var gfh = new GdaxFeedHandler(['BTC-USD'], ['full', 'level2']);

gfh.on('message', function(book) {
	if (book.md_type !== 'OrderBook') {
		return;
	}

  var bid1 = book.bids.max();
  var ask1 = book.asks.min();
  console.log('bid1: ($', bid1.price.toString(), ', ', bid1.qty, '), ask1: ($', ask1.price.toString(), ', ', ask1.qty , ')');
});

// gfh.on('message', function(msg) {
// 	if (msg.md_type === 'Order' || msg.md_type === 'Trade') {
// 		console.log(msg);
// 	}
// });

gfh.start();