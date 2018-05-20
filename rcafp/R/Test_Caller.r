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

dt_orderBook <- runQuery(
  starttime = "20180117T120000",
  endtime = "20180119T000000",
  exchange_list = c("GDAX", "ABC"),
  product_list = c("BTC", "ETH"),
  table = "orderBook"
)

dt_Trade <- runQuery(
  starttime = "20180117T120000",
  endtime = "20180119T000000",
  exchange_list = c("GDAX", "ABC"),
  product_list = c("BTC", "ETH"),
  table = "Trade"
)

initialSubscriber(starttime, endtime, exchange_list, product_list, table_list)

options(digits = 15)
dt_Stream <- pblapply(1:100, function(i) {
  stream <- runSubscriber()
  spec <- attr(stream, "spec")
  cat(stream$timestamp, spec[3], "\n")
  return(stream)
})

showConnections(all = T)
closeAllConnections()

process <- function(callback) {
  for (i in c(1,2,3)) {
    callback(i)
  }
}

data <<- c()
foo <- function(i) {
  data <<- c(data, i)
}
process(foo)
print(data)
