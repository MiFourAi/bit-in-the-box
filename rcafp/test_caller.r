library(rcafp)

# Test caller
root_path <- "~/Downloads/Data"

table <- "orderBook"
table <- "Trade"
exchange <- "GDAX"
product <- "BTC"
date <- "20180117"
table_list <- c("orderBook", "Trade")
exchange_list <- c("GDAX", "ABC")
product_list <- c("BTC", "ETH")
path <- file.path(root_path, table, exchange, product, date)
starttime = "20180117T120000"
endtime = "20180117T130000"

dt_orderBook <- runQuery(
  starttime = "20180117T120000",
  endtime = "20180117T130000",
  exchange_list = c("GDAX", "ABC"),
  product_list = c("BTC", "ETH"),
  table = "orderBook",
  root_path = root_path
)

dt_Trade <- runQuery(
  starttime = "20180117T120000",
  endtime = "20180117T130000",
  exchange_list = c("GDAX", "ABC"),
  product_list = c("BTC", "ETH"),
  table = "Trade",
  root_path = root_path
)

options(digits = 15)

queue <- initialSubscriber(starttime, endtime, exchange_list, product_list, table_list, root_path)

dt_Stream <- pblapply(1:1000, function(i) {
  stream <- runSubscriber(queue)
  if (is.null(stream)) break
  return(stream)
})

showConnections(all = T)
closeAllConnections()

test <- run_backtest(strategy = NULL, starttime, endtime, exchange, product, table_list, root_path)
