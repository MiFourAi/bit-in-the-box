const FeedCsvAppender = require('./feed_csv_appender').FeedCsvAppender;
const config = require('./config.js').config;

var appender = new FeedCsvAppender(
	config.basePath, 'btcusd', 'bitstamp', 'order_book', 300);
msg = {
	exchange: 'bitstamp',
	productID: 'btcusd',
	exchangeTimestamp: 1318781876,
	timestamp: (new Date().getTime()),
	sequenceNo: 0,
	bids: [{'price': 1, 'qty': 2}, {'price': 2, 'qty': 3}],
	asks: [{'price': 2, 'qty': 3}, {'price': 1, 'qty': 4}],
};
appender.append(msg);