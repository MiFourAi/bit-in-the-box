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
  table = "orderBook"
)

dt_Trade <- runQuery(
  starttime = "20180117T120000",
  endtime = "20180117T130000",
  exchange_list = c("GDAX", "ABC"),
  product_list = c("BTC", "ETH"),
  table = "Trade"
)

options(digits = 15)
library(pbapply)

queue <- initialSubscriber(starttime, endtime, exchange_list, product_list, table_list)

dt_Stream <- pblapply(1:1000, function(i) {
  stream <- runSubscriber(queue)
  # spec <- attr(stream, "spec")
  # cat(stream$timestamp, spec[3], "\n")
  if (is.null(stream)) break
  return(stream)
})

showConnections(all = T)
closeAllConnections()

process <- function(callback) {
  for (i in c(1,2,3)) {
    callback(i)
  }
}

tmpdata <<- c()
foo <- function(i) {
  tmpdata <<- c(tmpdata, i)
}
process(foo)
print(tmpdata)

test1 <- backtest$new()
test1$init(1, 2, "GDAX", "BTC", 100000, 10)
test1$load_stream(dt_Stream[[1]])
test1$load_stream(dt_Stream[[2]])
test1$place_order(timestamp = 123456, 10494.54, 5, "ask")
test1$place_order(timestamp = 123456, 10490, 1, "bid")
test1$place_order(timestamp = 123456, 10500, 1, "ask")

tmp <- pblapply(3:1000, function(i) {
  test1$load_stream(dt_Stream[[i]])
  # spec <- attr(stream, "spec")
  # cat(stream$timestamp, spec[3], "\n")
  return(NULL)
})

test <- run_backtest(strategy = NULL, starttime, endtime, exchange, product, table_list)
