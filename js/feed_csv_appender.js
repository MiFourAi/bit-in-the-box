const csvWriter = require('csv-write-stream');
const path = require('path');
const fs = require('fs');
const num = require('num');
const moment = require('moment');
const pricef = require('./utils.js').pricef;
const epochToDate = require('./utils.js').epochToDate;
const epochToTime = require('./utils.js').epochToTime;
const makeDir = require('./utils.js').makeDir;

var createCsvWriter = function (dir) {
	writer = csvWriter();
	writer.pipe(fs.createWriteStream(dir + '.csv'));
	return writer;
}

var FeedCsvAppender = function(basePath, productID, exchange, msgType, 
	blockTimeMs, unwantedCsvHeaders) {
	this.basePath = basePath;
	this.productID = productID;
	this.exchange = exchange;
	this.msgType = msgType;
	this.blockTimeMs = blockTimeMs;
	this.unwantedCsvHeaders = unwantedCsvHeaders || [];
	this.writer = null;
}

FeedCsvAppender.prototype.finishWriting = function() {
	if (this.writer) {
		this.writer.end();
		this.writer = null;
	}
}

FeedCsvAppender.prototype._flatten = function(msg, suffix = '') {
	var result = {};
	for (var key in msg) {
		var value = msg[key];
		var uniqKey = key + suffix;
		if (value === null) {
			result[uniqKey] = '';
		} else if (Array.isArray(value)) {
			for (var i = 0; i < value.length; ++i) {
				var tmp = this._flatten(value[i], '_' + String(i+1));
				for (var subkey in tmp) {
					result[key + '_' + subkey] = tmp[subkey];
				}
			}
		} else if (value instanceof num) {
			result[uniqKey] = value.toString();
		} else if (typeof(value) === 'object') {
			var tmp = this._flatten(value[i], suffix);
			for (var subkey in tmp) {
				result[key + '_' + subkey] = tmp[subkey];
			}
		} else {
			result[uniqKey] = value;
		}
	}
	return result;
}

FeedCsvAppender.prototype._removeUnwantedHeaders = function(msg) {
	for (var i = 0; i < this.unwantedCsvHeaders.length; ++i) {
		const header = this.unwantedCsvHeaders[i];
		if (header in msg) {
			delete msg[header];
		}
	}
	return msg;
}

FeedCsvAppender.prototype.append = function(msg) {
	if (!this.writer) {
		this.lastCheckpointTime = msg.timestamp;
		// This is a string, not a number containing the epoch time.
		this.filetime = epochToTime(msg.timestamp);
		var date = epochToDate(msg.timestamp);
		var outdir = path.join(
			this.basePath, this.exchange, this.productID,
			this.msgType, date);
		var outfile = path.join(outdir, this.filetime);
		makeDir(outdir);
		this.writer = createCsvWriter(outfile);
	}
	var flattenMsg = this._flatten(msg);
	flattenMsg = this._removeUnwantedHeaders(flattenMsg);
	this.writer.write(flattenMsg);
	if (msg.timestamp - this.lastCheckpointTime >= this.blockTimeMs) {
		this.finishWriting();
	}
}

module.exports.FeedCsvAppender = FeedCsvAppender;
