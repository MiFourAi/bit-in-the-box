const bidsPricePrefix = 'bids_price_';
const bidsQtyPrefix = 'bids_qty_';
const asksPricePrefix = 'asks_price_';
const asksQtyPrefix = 'asks_qty_';

const windowMs = 30 * 1000;
const numWindows = 60;
var tradeWindowInfoArr = [];

const serverUrl = 'http://localhost:5011';

// Load the Visualization API and the corechart package.
google.charts.load('current', {'packages':['corechart']});

var bookChart = null;
var tradePriceChart = null;
var tradeVolChart = null;

// Set a callback to run when the Google Visualization API is loaded.
google.charts.setOnLoadCallback(onChartModuleLoaded);

function onChartModuleLoaded() {
  bookChart = new google.visualization.BarChart(document.getElementById('book_div'));
  tradePriceChart = 
  new google.visualization.CandlestickChart(document.getElementById('price_div'));
  tradeVolChart = new google.visualization.ColumnChart(document.getElementById('vol_div'));
}

function httpGetAsync(theUrl, callback)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function() { 
        if (xmlHttp.readyState == 4 && xmlHttp.status == 200)
            callback(xmlHttp.responseText);
    }
    xmlHttp.open('GET', theUrl, true); // true for asynchronous 
    // xmlHttp.setRequestHeader('Access-Control-Allow-Headers', '*');
    xmlHttp.send(null);
}

function separateBookBidsAndAsks(md) {
  function processSide(pricePrefix, qtyPrefix) {
    var result = [];
    var i = 1;
    while (true) {
      const priceKey = pricePrefix + i;
      const qtyKey = qtyPrefix + i;
      if (!(md.hasOwnProperty(priceKey) && md.hasOwnProperty(qtyKey))) {
        break;
      }
      // leave price as string, but qty as number
      result.push([md[priceKey], parseFloat(md[qtyKey])]);
      i += 1;
    }
    result.sort(function(pa, pb) {
      // |pa| and |pb| are strings
      return parseFloat(pb) - parseFloat(pa);      
    });
    return result;
  }

  var bids = processSide(bidsPricePrefix, bidsQtyPrefix);
  var asks = processSide(asksPricePrefix, asksQtyPrefix);

  return {bids: bids, asks: asks};
}

function toBookChartData(bidsAsks) {
  var bids = bidsAsks.bids;
  var asks = bidsAsks.asks;
  var result = [['Price', 'Qty', { role: 'style' }]];
  asks.forEach(function(e) {
    e.push('#a52714'); // red
    result.push(e);
  });
  result.push(['', 0, 'opacity: 0'])
  bids.forEach(function(e) {
    e.push('#0f9d58'); // green
    result.push(e);
  })
  return result;
}

function drawOrderBook(bookChartData) {
  var data = google.visualization.arrayToDataTable(bookChartData);
  var options = {
        title: "OrderBook",
        width: 500,
        height: bookChartData.length * 15,
        bar: {groupWidth: "90%"},
        legend: { position: "none" },
      };
  bookChart.draw(data, options);
}

function makeTradeWindowInfo(trade) {
  const price = trade.price;
  var buyVol = 0, sellVol = 0;
  if (trade.makerSide === 'b') {
    // maker is buy, meaning that someone sold aggressively.
    sellVol = trade.qty;
  } else {
    // maker is sell, meaning that someone bought aggressively.
    buyVol = trade.qty;
  }
  return {
    initTs: trade.timestamp,
    open: price,
    close: price,
    low: price,
    high: price,
    buyVol: buyVol,
    sellVol: sellVol
  };
}

function makeTradeWindowInfoPlaceholder(timestamp, price) {
  return {
    initTs: timestamp,
    open: price,
    close: price,
    low: price,
    high: price,
    buyVol: 0,
    sellVol: 0
  };
}

function updateTradeWindowInfo(trade) {
  // Sample Trade msg:
  // Trade(exchange='gdax', product='btcusd', exchangeTimestamp='1515571422111', timestamp='1515571422189', sequenceNo='1227482', tradeID='32824839', price='14049.00', qty='0.35679758', makerSide='b', buyOrderID='48469c84-ea90-4ff8-8e73-3faf1429dd9a', sellOrderID='12216ff0-4eb0-45d6-979b-6812ea1e20d1')
  
  var twi = null;
  if (tradeWindowInfoArr.length > 0) {
    twi = tradeWindowInfoArr[tradeWindowInfoArr.length - 1];
  }
  if (!twi) {
    twi = makeTradeWindowInfo(trade);
    tradeWindowInfoArr.push(twi);
  } else if (trade.timestamp - twi.initTs > windowMs) {
    // Deal with the case where we haven't seen a trade during an entire
    // |windowMs| period of time.
    while (trade.timestamp - twi.initTs - windowMs > windowMs) {
      twi = makeTradeWindowInfoPlaceholder(twi.initTs + windowMs, twi.close);
      tradeWindowInfoArr.push(twi);
    }
    twi = makeTradeWindowInfo(trade);
    tradeWindowInfoArr.push(twi);
  }

  while (tradeWindowInfoArr.length > numWindows) {
    tradeWindowInfoArr.shift();
  }

  const price = trade.price;
  twi.close = price;
  twi.high = Math.max(twi.high, price);
  twi.low = Math.min(twi.low, price);
  if (trade.makerSide == 'b') {
    twi.sellVol += trade.qty;
  } else {
    twi.buyVol += trade.qty;
  }
}

function drawTradeWindowInfoArray() {
  if (tradeWindowInfoArr.length === 0) {
    return;
  }
  var priceArr = [];
  var volArr = [['timestamp', 'buy', 'sell']];
  for (var i = 0; i < tradeWindowInfoArr.length; ++i) {
    const twi = tradeWindowInfoArr[i];
    priceArr.push([twi.initTs.toString(), twi.low, twi.open, twi.close, twi.high]);
    volArr.push([twi.initTs.toString(), twi.buyVol, twi.sellVol]);
  }
  // true means treat the first row int the array as data.
  var data = google.visualization.arrayToDataTable(priceArr, true); 
  var options = {
      title: "Price",
      width: numWindows * 15,
      height: 300,
      bar: {groupWidth: "90%"},
        candlestick: {
        fallingColor: { strokeWidth: 0, fill: '#a52714' }, // red
        risingColor: { strokeWidth: 0, fill: '#0f9d58' }   // green
      },
      legend: {position: "none"},
  };
  tradePriceChart.draw(data, options);

  var data = google.visualization.arrayToDataTable(volArr);
  var options = {
      title: "Trade Volume",
      width: numWindows * 15,
      height: 300,
      bar: {
        groupWidth: "90%",
      },
      isStacked: true,
      bars: 'horizontal',
      colors:['#0f9d58', '#a52714'],
  }
  tradeVolChart.draw(data, options);
}

function mdHandler(mdText) {
  md = JSON.parse(mdText);
  // console.log(md);
  const md_type = md.type;
  md.timestamp = parseInt(md.timestamp);
  if (md_type === 'Trade') {
    md.price = parseFloat(md.price);
    md.qty = parseFloat(md.qty);
    updateTradeWindowInfo(md);
    drawTradeWindowInfoArray();
  }
  else if (md_type === 'OrderBook') {
    var bidsAsks = separateBookBidsAndAsks(md);
    var bookChartData = toBookChartData(bidsAsks);
    drawOrderBook(bookChartData);
  }
  else {
    alert('Unknown md type: ' + md_type);
    console.log(md);
  }
  // console.log(bookChartData);
}


function onNextMd() {
  httpGetAsync(serverUrl, mdHandler);
}
