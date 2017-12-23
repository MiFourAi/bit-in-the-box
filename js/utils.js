const num = require('num');

var num2f = function(f) {
	return num(f).set_precision(2);
}

var abbreviateSide = function(side) {
	side = side.toLowerCase();
	if (side === 'buy' || side === 'bid') {
		return 'b';
	} else if (side === 'sell' || side === 'ask') {
		return 's';
	}
	throw side;

}

module.exports.pricef = num2f;
module.exports.abbreviateSide = abbreviateSide;