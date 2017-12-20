
var Pusher = require('pusher-js/node');
var csvWriter = require('csv-write-stream');
var path = require('path');
var fs = require("fs");

var BASE_DIR = "/Users/wenshuaiye/Kaggle/bitcoin/data/bitstamp";
var BITSTAMP_PUSHER_KEY = 'de504dc5763aeef9ff52';
var TIMESPAN = 60 * 1000 * 8;
var BOOK_DEPTH = 10;

var pusher = new Pusher(BITSTAMP_PUSHER_KEY, {
    encrypted: true
});

var addLeadingZeros = function(s, digits) {
    digit = (typeof digit !== 'undefined') ?  digit : 2;
    return Array(Math.max(0, digits - String(s).length + 1))
        .join("0") + String(s);
}

var datetimeToStr = function(datetime) {
    return String(datetime.getFullYear()) + addLeadingZeros(
        datetime.getMonth() + 1, 2) + addLeadingZeros(datetime.getDate(), 
        2) + "T" + addLeadingZeros(datetime.getHours(), 2) + 
        addLeadingZeros(datetime.getMinutes(), 2) + addLeadingZeros(
        datetime.getSeconds(), 2);
}

function Channel (channelName, timespan) {
    this.timespan = (typeof timespan !== 'undefined') ?  timespan : TIMESPAN;
    var date = new Date();
    this.channel = pusher.subscribe(channelName);
    this.channelName = channelName;
    this.buffer = [];
    this.timestamp = date.getTime() - TIMESPAN;
    this.filename = datetimeToStr(date);
    this.writer = csvWriter();
}

Channel.prototype.bind = function (event, callback) {
    this.channel.bind(event, function(data) {
    callback(data);
    })
}

Channel.prototype.handleGeneric = function (data) {
    var date = new Date();
    data['ts'] = new Date().getTime();
    this.buffer.push(data);
    if (this.event == "order_book") {
        console.log(date.getTime() - this.timestamp);
    }
    if (date.getTime() - this.timestamp > this.timespan) {
        var filename = datetimeToStr(date);
        var presetDate = path.join(
            BASE_DIR, this.channelName, filename.split("T")[0])
        var outPath = path.join(presetDate, filename)
        if (!fs.existsSync(presetDate)){
            fs.mkdirSync(presetDate);
        }
        this.timestamp = date.getTime();
        this.writer.end();
        this.writer = csvWriter();
        this.writer.pipe(fs.createWriteStream(outPath, 'r+'));
        console.log("Open " + this.channelName + " file at " + outPath);
    }
    this.writer.write(data);
}

function Bitstamp (channels) {
    this.channels = [];
    var that = this;
    console.log(channels)
    for (var k = 0; k < channels.length; ++k) {
        var x = channels[k];
        this.channels.push(new Channel(x));
        var lastIndex = this.channels.length - 1;
        if (x.indexOf("live_orders") > -1) {
            var orderMessages = ["order_created",
                "order_changed", "order_deleted"];
            for (var i = 0; i < orderMessages.length; ++i) {
                this.channels[lastIndex].bind(orderMessages[i], function(data) {
                    data["action"] = i;
                    that.channels[lastIndex].handleGeneric(data);
                });
            }
        } else if (x.indexOf("order_book") > -1) {
            this.channels[lastIndex].bind("data", function(data) {
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
                that.channels[lastIndex].handleGeneric(item);
            })
        } else if (x.indexOf("live_trades") > -1) {
            this.channels[lastIndex].bind("trade", function(data) {
                that.channels[lastIndex].handleGeneric(data);
            })
        } else {
            continue;
        }
    }
}

var bitstamp = new Bitstamp(["live_orders"]);
//var bitstamp2 = new Bitstamp(["live_trades"]);
//module.exports = Bitstamp;