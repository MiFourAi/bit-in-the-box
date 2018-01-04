const pricef = require('./utils.js').pricef;
const GdaxFeedHandler = require('./gdax_mess.js').GdaxFeedHandler;
const GdaxFixDepthOrderBookConverter = require('./gdax_mess.js').GdaxFixDepthOrderBookConverter;
const FeedCsvAppender = require('./feed_csv_appender').FeedCsvAppender;
const config = require('./config.js').config;

var gdaxConfig = config.gdax;

// var isBookArraySame = function(arr1, arr2) {
//   if (arr1.length !== arr2.length) {
//     return false;
//   }
//   for (var i = 0; i < arr1.length; ++i) {
//     var item1 = arr1[i], item2 = arr2[i];
//     if (item1.price.cmp(item2.price) !== 0) {
//       return false;
//     }
//     if (item1.qty !== item2.qty) {
//       return false;
//     }
//   }
//   return true;
// };


var gfh = new GdaxFeedHandler(gdaxConfig.products, gdaxConfig.channels);
var obConverter = new GdaxFixDepthOrderBookConverter(gdaxConfig.bookDepth);
const mdTypes = ['Order', 'Trade', 'OrderBook'];

var ProductMdCsvAppender = function(productID) {
  this.productID = productID;

  this.csvAppenders = {};
  for (var i = 0; i < mdTypes.length; ++i) {
    const md = mdTypes[i];
    // TODO(mike): Read the storage root path from the config once we can 
    // specify the path in an un-tracked config file.
    this.csvAppenders[md] = new FeedCsvAppender('../..', productID, 'gdax', md, config.blockTime);
  }
  this.lastBookWrittenSeqNo = -gdaxConfig.bookSamplingSeqInterval - 1;
  this.msgCounter = 0;
};

ProductMdCsvAppender.prototype.handle = function(msg) {
  var writeToDisk = true;
  if (msg.md_type == 'OrderBook') {
    var ob = obConverter.convert(msg);
    msg.bids = ob.bids;
    msg.asks = ob.asks;

    if (msg.sequenceNo - this.lastBookWrittenSeqNo < gdaxConfig.bookSamplingSeqInterval) {
      writeToDisk = false;
    } else {
      this.lastBookWrittenSeqNo = msg.sequenceNo;
    }
  }

  if (this.msgCounter > 0 && (this.msgCounter % 1000 == 0)) {
    console.log('Processed ', this.msgCounter, ' messages for product: ', this.productID);
  }
  ++this.msgCounter;
  if (writeToDisk) {
    this.csvAppenders[msg.md_type].append(msg);
  }
};

var productCsvAppenders = {};
for (var i = 0; i < gdaxConfig.products.length; ++i) {
  const p = gdaxConfig.products[i];
  productCsvAppenders[p] = new ProductMdCsvAppender(p);
}

gfh.on('message', function(msg) {
  if (!msg) return;
  productCsvAppenders[msg.productID].handle(msg);
});

gfh.start();
