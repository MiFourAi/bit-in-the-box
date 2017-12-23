const num = require('num');

var num2f = function(f) {
	return num(f).set_precision(2);
}

module.exports.pricef = num2f;