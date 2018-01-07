const pricef = require('./utils.js').pricef;
const BitfinexFeedHandler = require('./bitfinex_mess.js').BitfinexFeedHandler;
const FixDepthOrderBookConverter = require('./utils.js').FixDepthOrderBookConverter;
const bitfinexNormalizeProductID = require('./bitfinex_mess.js').BitfinexDefaultMsgNormalizer;
const FeedCsvAppender = require('./feed_csv_appender').FeedCsvAppender;
const config = require('./config.js').config;

var bitfinexConfig = config.bitfinex;

var bfh = new BitfinexFeedHandler(
    bitfinexConfig.products,
    bitfinexConfig.channels);
var obConverter = new FixDepthOrderBookConverter(bitfinexConfig.bookDepth);
const mdTypes = ['Order', 'Trade', 'OrderBook'];

var bitfinexCsvAppenders = {};

for (var product of bitfinexConfig.products) {
    bitfinexProduct = product.replace('t', '').toLowerCase();
    bitfinexCsvAppenders[bitfinexProduct] = {};
    for (var md of mdTypes) {
        bitfinexCsvAppenders[bitfinexProduct][md] =
            new FeedCsvAppender(
                config.basePath, bitfinexProduct, 'bitfinex', md,
                config.blockTime, config.unwantedCsvHeaders);
    }
}

bfh.on('message', function(msg) {
    if (!(msg.productID in bitfinexCsvAppenders)) {
        throw msg.productID;
    }
    if (msg.md_type == 'OrderBook') {
        var ob = obConverter.convert(msg);
        msg.bids = ob.bids;
        msg.asks = ob.asks;
    }
    bitfinexCsvAppenders[msg.productID][msg.md_type].append(msg);
})

bfh.start();