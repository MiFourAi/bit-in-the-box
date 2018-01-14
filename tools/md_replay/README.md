
# OrderBook replay tool

To change the data source and spec (time range, exchange, product...), please change it inside `server.py`.

## How to use

First we need to start the server:

```
python server.py
```

Then open the `index.html` in your browser.

## TODO

- Add trade info, draw the price/candle chart and the traded volume chart (maybe separate by long/short?)
- Integrate with the backtest.