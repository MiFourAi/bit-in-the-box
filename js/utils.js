const num = require('num');
const moment = require('moment');
const shell = require('shelljs');

var num2f = function(f) {
  return num(f).set_precision(2);
}

var epochToTime = function(epochts) {
  // HH: 24 hour time.
  // hh: 12 hour time.
	return moment(epochts).format('YYYYMMDDTHHmmss');
}

var epochToDate = function(epochts) {
	return moment(epochts).format('YYYYMMDD');
}

var makeDir = function(dir) {
	shell.mkdir('-p', dir);
}

///////////////////////////////////////////////////////////////

// Convert the RBTree based OrderBook to list based.
var FixDepthOrderBookConverter = function(bookDepth) {
  this.bookDepth = bookDepth || 30;
}

FixDepthOrderBookConverter.prototype.convert = function(orderBook) {
  var bidsTree = orderBook.bids, asksTree = orderBook.asks;
  const depth = Math.min(this.bookDepth, bidsTree.size, asksTree.size);
  if (depth !== this.bookDepth) {
    console.log('Warning! depth=', depth, ' cannot match configured bookDepth=', this.bookDepth);
  }

  var bids = [], asks = [];
  var itr = bidsTree.iterator();
  for (var i = 0; i < depth; ++i) {
    var item = itr.prev();
    bids.push(item);
  }

  itr = asksTree.iterator();
  for (var i = 0; i < depth; ++i) {
    var item = itr.next();
    asks.push(item);
  }

  return {bids: bids, asks: asks};
}

module.exports.pricef = num2f;
module.exports.makeDir = makeDir;
module.exports.epochToDate = epochToDate;
module.exports.epochToTime = epochToTime;
module.exports.FixDepthOrderBookConverter = FixDepthOrderBookConverter;
