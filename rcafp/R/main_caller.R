#' main caller
#'
#' @export
run_backtest <- function(strategy = NULL, starttime, endtime, exchange, product, table_list, root_path) {
  #input: tables and a trading strategy class
  #output: orderbook and transaction record
  queue <- initialSubscriber(starttime, endtime, exchange, product, table_list, root_path)
  test <- backtest$new()
  test$init(starttime, endtime, exchange, product, 100000, 100)
  strat <- strategy_ma$new()
  strat$init(test)
  last_order_stream <<- NULL
  dummy <- pblapply(1:5000, function(i) {
    stream <- runSubscriber(queue)
    if (is.null(stream)) break
    test$load_stream(stream)
    ### run stragegy
    strat$load_stream(stream, test)
    if (attr(stream, "spec")[3] == "orderBook") last_order_stream <<- stream
    if (i %% 100 == 0) {
      cat("\n", last_order_stream$asks_price_1, strat$ask_price, last_order_stream$bids_price_1, strat$bid_price, "\n")
    }
    # if (i == 100) {
    #   test$place_order(timestamp = stream$timestamp, 10300, 1, "bid")
    #   test$place_order(timestamp = stream$timestamp, 10700, 1, "ask")
    # }
    return(invisible(NULL))
  })
  return(invisible(NULL))
}
