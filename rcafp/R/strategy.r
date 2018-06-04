#' strategy class
#'
#' create a class for trading strategy (moving average)
#' @export
strategy_ma <- setRefClass(
  "strategy_ma",
  fields = c("exchange_order_book", "burn_in", "starttime",
             "bid_sum", "bid_sum_2", "bid_n", "bid_price",
             "ask_sum", "ask_sum_2", "ask_n", "ask_price"),
  methods = list(
    init = function(test) {
      exchange_order_book <<- data.table()
      burn_in <<- TRUE
      starttime <<- epoch_to_timestamp(test$starttime)
      bid_sum <<- 0
      bid_sum_2 <<- 0
      bid_n <<- 0
      bid_price <<- 0
      ask_sum <<- 0
      ask_sum_2 <<- 0
      ask_n <<- 0
      ask_price <<- Inf
    },
    load_stream = function(stream, test) {
      if (attr(stream, "spec")[3] == "Trade") {
        # do nothing
      } else if (attr(stream, "spec")[3] == "orderBook") {
        # run strategy
        start_ts <- stream$timestamp - window
        get_price(stream, start_ts)
        if (burn_in) {
          check_burn_in(start_ts)
        }
        if (!burn_in) {
          # cat(stream$asks_price_1, ask_price, stream$bids_price_1, bid_price, "\n")
          if (stream$asks_price_1 > (1 + thres_pct) * ask_price) {
            # cat(stream$asks_price_1, ask_price, stream$bids_price_1, bid_price, "\n")
            test$place_order(
              timestamp = stream$timestamp + 1,
              price = stream$asks_price_1,
              qty = 1,
              type = "bid")
          }
          if (stream$bids_price_1 < (1 - thres_pct) * bid_price) {
            # cat(stream$asks_price_1, ask_price, stream$bids_price_1, bid_price, "\n")
            test$place_order(
              timestamp = stream$timestamp + 1,
              price = stream$bids_price_1,
              qty = 1,
              type = "bid")
          }
        }
      }
    },
    get_price = function(exchange_order, start_ts) {
      if (nrow(exchange_order_book) == 0) {
        exchange_order_book <<- rbind(exchange_order_book, exchange_order)
      } else {
        exchange_order_book <<- rbind(exchange_order_book[timestamp >= start_ts], exchange_order)
      }
      past_orders <- exchange_order_book[timestamp < start_ts]
      bid_sum <<- bid_sum + exchange_order$bids_price_1 - sum(past_orders$bids_price_1)
      bid_sum_2 <<- bid_sum_2 + (exchange_order$bids_price_1)^2 - sum((past_orders$bids_price_1)^2)
      bid_n <<- bid_n + 1 - nrow(past_orders)
      bid_price <<- bid_sum/bid_n
      ask_sum <<- ask_sum + exchange_order$asks_price_1 - sum(past_orders$asks_price_1)
      ask_sum_2 <<- ask_sum_2 + (exchange_order$asks_price_1)^2 - sum((past_orders$asks_price_1)^2)
      ask_n <<- ask_n + 1 - nrow(past_orders)
      ask_price <<- ask_sum/ask_n
      return(invisible(NULL))
    },
    check_burn_in = function(start_ts) {
      if (starttime < start_ts) burn_in <<- FALSE
    }
  )
)
