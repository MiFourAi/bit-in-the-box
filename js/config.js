var config = {};
config.basePath = '/Users/wenshuaiye/Dropbox/bitcoin_files/sample_data';
config.blockTime = 300 * 1000;
config.bitstamp = {
	products: ['btcusd'],
	channels: {
		'live_trades': 'Trade',
		'order_book': 'OrderBook',
		'live_orders': 'Order',
	},
}
config.bitfinex = {}

module.exports.config = config;