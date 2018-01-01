const num = require('num');
const moment = require('moment');
const shell = require('shelljs');

var num2f = function(f) {
  return num(f).set_precision(2);
}

var epochToTime = function(epochts) {
  // HH: 24 hour time.
  // hh: 12 hour time.
	return moment(epochts).format('YYYYMMDDTHHmmss');
}

var epochToDate = function(epochts) {
	return moment(epochts).format('YYYYMMDD');
}

var makeDir = function(dir) {
	shell.mkdir('-p', dir);
}

module.exports.pricef = num2f;
module.exports.makeDir = makeDir;
module.exports.epochToDate = epochToDate;
module.exports.epochToTime = epochToTime;