var config = {};
// TODO(mike): Put this path into a config file, do NOT check in that config file.
config.basePath = '/Users/wenshuaiye/Dropbox/bitcoin_files/sample_data';

config.blockTime = 300 * 1000;
config.unwantedCsvHeaders = ['exchange', 'productID'];

config.bitstamp = {
	products: ['btcusd'],
	channels: ['live_orders', 'order_book', 'live_trades'],
}

config.bitfinex = {
	products: ['tBTCUSD'],
	channels: ['orders', 'book', 'trades'],
	bookDepth: 20,
}

config.gdax = {
  products: ['BTC-USD', 'LTC-USD', 'ETH-USD'],
  channels: ['full', 'level2'],
  bookDepth: 20,
  bookSamplingSeqInterval: 25,
}

module.exports.config = config;