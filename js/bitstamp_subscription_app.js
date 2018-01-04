const pricef = require('./utils.js').pricef;
const FeedCsvAppender = require('./feed_csv_appender').FeedCsvAppender;
const BitstampFeedHandler = require('./bitstamp_mess.js').BitstampFeedHandler;
const config = require('./config.js').config;
const events = require('events');

var bitstampConfig = config.bitstamp;

var bfh = new BitstampFeedHandler(
	bitstampConfig.products,
	['live_orders', 'order_book', 'live_trades']);

var bitstampCsvAppenders = {};

const mdTypes = ['Order', 'Trade', 'OrderBook'];

for (var product of bitstampConfig.products) {
	bitstampCsvAppenders[product] = {};
	for (var md of mdTypes) {
		bitstampCsvAppenders[product][md] = new FeedCsvAppender(
			config.basePath, product, 'bitstamp', md, config.blockTime);
	}
}

bfh.on('message', function(msg) {
	if (!(msg.productID in bitstampCsvAppenders)) {
		throw msg.productID;
	}
    bitstampCsvAppenders[msg.productID][msg.md_type].append(msg);
})

bfh.start();
