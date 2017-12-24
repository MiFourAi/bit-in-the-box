const readline = require('readline');
const num = require('num');
const pricef = require('./utils.js').pricef;
const GdaxFeedHandler = require('./gdax_mess.js').GdaxFeedHandler;
const GdaxFixDepthOrderBookConverter = require('./gdax_mess.js').GdaxFixDepthOrderBookConverter;

const bookDepth = 6;
const numLines = bookDepth * 2 + 2 + 1;  // 2 * (price, qty) + 'Asks: ' + 'Bids: ' + 'Spread: '

var gfh = new GdaxFeedHandler(['BTC-USD'], ['level2']);
var obConverter = new GdaxFixDepthOrderBookConverter(bookDepth);

var clearConsoleLines = function(n) {
  for (var i = 0; i < n; ++i) {
    // 作用:清掉terminal当前这行，然后把cursor移到上一行
    readline.clearLine(process.stdout, 0);
    readline.moveCursor(process.stdout, 0, -1);
  }
};

gfh.on('message', function(book) {
  if (book.md_type !== 'OrderBook') {
    return;
  }
  ob = obConverter.convert(book);
  // a crappy implementation to display OrderBook on the terminal at a fixed position.
  clearConsoleLines(numLines);

  var itemLog = function(item) {
    process.stdout.write('  ' + num(item.qty).set_precision(7).toString() + ', ' + item.price.toString() + '\n');
  };
  process.stdout.write('Asks: \n');
  ob.asks.reverse().forEach(itemLog);
  process.stdout.write('Bids: \n');
  ob.bids.forEach(itemLog);

  process.stdout.write('Spread: ' + pricef(ob.asks[ob.asks.length - 1].price - ob.bids[0].price) + '\n');
});

// gfh.on('message', function(msg) {
//  if (msg.md_type === 'Order' || msg.md_type === 'Trade') {
//    console.log(msg);
//  }
// });

gfh.start();