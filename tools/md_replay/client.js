const bidsPricePrefix = 'bids_price_';
const bidsQtyPrefix = 'bids_qty_';
const asksPricePrefix = 'asks_price_';
const asksQtyPrefix = 'asks_qty_';

const serverUrl = 'http://localhost:5011';

// Load the Visualization API and the corechart package.
google.charts.load('current', {'packages':['corechart']});

var bookChart = null;

// Set a callback to run when the Google Visualization API is loaded.
google.charts.setOnLoadCallback(onChartModuleLoaded);

function onChartModuleLoaded() {
  bookChart = new google.visualization.BarChart(document.getElementById('book_div'));
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
    e.push('red');
    result.push(e);
  });
  result.push(['', 0, 'opacity: 0'])
  bids.forEach(function(e) {
    e.push('green');
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
        bar: {groupWidth: "95%"},
        legend: { position: "none" },
      };
  bookChart.draw(data, options);
}

function mdHandler(mdText) {
  md = JSON.parse(mdText);
  var bidsAsks = separateBookBidsAndAsks(md);
  var bookChartData = toBookChartData(bidsAsks);
  drawOrderBook(bookChartData);
  console.log(bookChartData);
}

function nextMdBtnListener() {
  httpGetAsync(serverUrl, mdHandler);
}
