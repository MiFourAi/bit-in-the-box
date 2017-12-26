const FeedCsvAppender = require('./feed_csv_appender').FeedCsvAppender;
const config = require('./config.js').config;
const BitstampFeedHandler = require('./bitstamp_mess.js').BitstampFeedHandler;

var bitstampConfig = config.bitstamp;

var bfh = new BitstampFeedHandler(
	bitstampConfig.products,
	Object.keys(bitstampConfig.channels));

var bitstampCsvAppenders = {};
for (var product of bitstampConfig.products) {
	bitstampCsvAppenders[product] = {};
	for (var channel in bitstampConfig.channels) {
		var normMsgType = bitstampConfig.channels[channel];
		bitstampCsvAppenders[product][normMsgType] = new FeedCsvAppender(
			config.basePath, product, 'bitstamp', channel, config.blockTime);
	}
}

bfh.on('message', function(msg) {
    bitstampCsvAppenders[msg.productID][msg.md_type].append(msg);
})

bfh.start();
