var config = {};
// TODO(mike): Put this path into a config file, do NOT check in that config file.
config.basePath = '/Users/wenshuaiye/Dropbox/bitcoin_files/sample_data';

config.blockTime = 300 * 1000;
config.bitstamp = {
	products: ['btcusd'],
}
config.bitfinex = {}

config.gdax = {
  // TODO(mike): Normalize the product IDs across different exchanges
  products: ['BTC-USD', 'LTC-USD', 'LTC-BTC'],
  channels: ['full', 'level2'],
  bookDepth: 20,
  bookSamplingSeqInterval: 25,
}

module.exports.config = config;