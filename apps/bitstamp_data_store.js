var Pusher = require('pusher-js/node');
var csvWriter = require('csv-write-stream');
var path = require('path');
var fs = require("fs");

var BASE_DIR = "/Users/wenshuaiye/Kaggle/bitcoin/data/bitstamp";
var BITSTAMP_PUSHER_KEY = 'de504dc5763aeef9ff52';
var TIMESPAN = 60 * 1000 * 8;
var BOOK_DEPTH = 10;

var pusher = new Pusher(BITSTAMP_PUSHER_KEY, {
    encrypted: false
});

var addLeadingZeros = function(s, digits) {
    digit = (typeof digit !== "undefined") ?  digit : 2;
    return Array(Math.max(0, digits - String(s).length + 1))
        .join("0") + String(s);
}

var datetimeToStr = function(datetime) {
    return String(datetime.getFullYear()) +
        addLeadingZeros(datetime.getMonth() + 1, 2) +
        addLeadingZeros(datetime.getDate(), 2) + "T" +
        addLeadingZeros(datetime.getHours(), 2) + 
        addLeadingZeros(datetime.getMinutes(), 2) +
        addLeadingZeros(datetime.getSeconds(), 2);
}

var isEndOfBlock = function(curTime, startTime, blockSize) {
    return curTime - startTime > blockSize;
}

var createCsvWriter = function (filename) {
    writer = csvWriter();
    writer.pipe(fs.createWriteStream(filename, "r+"));
    return writer;
}

var makeDir = function(dir) {
    if (!fs.existsSync(dir)){
        fs.mkdirSync(dir);
    }
}

function BitstampChannelListener (channelName, timespan) {
    var date = new Date();
    this.channel = pusher.subscribe(channelName);
    this.channelName = channelName;
    this.timespan = (typeof timespan !== "undefined") ?  timespan : TIMESPAN;
    this.buffer = [];
    this.curTime = date.getTime() - TIMESPAN;
}

BitstampChannelListener.prototype.handleGeneric_ = function (data) {
    var date = new Date();
    data["ts"] = new Date().getTime();
    this.buffer.push(data);
    if (isEndOfBlock(date.getTime(), this.curTime, this.timespan)) {
        var filename = datetimeToStr(date);
        var presetDate = path.join(
            BASE_DIR, this.channelName, filename.split("T")[0])
        var outFile = path.join(presetDate, filename)
        if (this.writer) {
            this.writer.end();
        }
        makeDir(presetDate);
        this.writer = createCsvWriter(outFile);
        this.curTime = date.getTime();
        console.log("Open " + this.channelName + " file at " + outFile);
    }
    this.writer.write(data);
}

BitstampChannelListener.prototype.bind_ = function (event, callback) {
    this.channel.bind(event, function(data) {
        callback(data);
    })
}

BitstampChannelListener.prototype.listen = function() {
    var that = this;
    if (this.channelName.indexOf("live_orders") > -1) {
        var orderMessages = ["order_created",
            "order_changed", "order_deleted"];
        for (var i = 0; i < orderMessages.length; ++i) {
            this.bind_(orderMessages[i], function(data) {
                data["action"] = i;
                that.handleGeneric_(data);
            });
        }
    } else if (this.channelName.indexOf("order_book") > -1) {
        this.bind_("data", function(data) {
            var item = {};
            var bid_ask = ["bid", "ask"];
            for (var j = 0; j < BOOK_DEPTH; ++j) {
                for (var i = 0; i < bid_ask.length; ++i) {
                    var side = bid_ask[i];
                    item[side + "_prc_" + String(j+1)] =
                        data[side + "s"][j][0];
                    item[side + "_qty_" + String(j+1)] =
                        data[side + "s"][j][1];
                }
            }
            that.handleGeneric_(item);
        })
    } else if (this.channelName.indexOf("live_trades") > -1) {
        this.bind_("trade", function(data) {
            that.handleGeneric_(data);
        })
    }
}

channels = ["live_orders", "order_book", "live_trades"];
for (var i = 0; i < channels.length; ++i) {
    listener = new BitstampChannelListener(channels[i]);
    listener.listen();
}
