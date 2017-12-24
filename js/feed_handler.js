const events = require('events');

var FeedHandler = function(productIDs, channels) {
    this.productIDs = productIDs;
    this.channels = channels;

    this.ws = null;
    this.eventEmitter = new events.EventEmitter();
}

FeedHandler.prototype.start = function() {}

FeedHandler.prototype.on = function(eventName, callback) {
    this.eventEmitter.on(eventName, callback);
}

module.exports.FeedHandler = FeedHandler;
