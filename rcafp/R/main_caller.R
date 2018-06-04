#' main caller
#'
#' @export
run_backtest <- function(strategy = NULL, starttime, endtime, exchange, product, table_list) {
  #input: tables and a trading strategy class
  #output: orderbook and transaction record
  queue <- initialSubscriber(starttime, endtime, exchange, product, table_list)
  test <- backtest$new()
  test$init(starttime, endtime, exchange, product, 100000, 100)
  strat <- strategy_ma$new()
  strat$init(test)

  dummy <- pblapply(1:10000, function(i) {
    stream <- runSubscriber(queue)
    if (is.null(stream)) break
    test$load_stream(stream)
    ### run stragegy
    strat$load_stream(stream, test)
    if (i %% 100 == 0) cat("\n", stream$asks_price_1, strat$ask_price, stream$bids_price_1, strat$bid_price, "\n")
    # if (i == 100) {
    #   test$place_order(timestamp = stream$timestamp, 10300, 1, "bid")
    #   test$place_order(timestamp = stream$timestamp, 10700, 1, "ask")
    # }
    return(invisible(NULL))
  })
  return(invisible(NULL))
}
