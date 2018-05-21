#' strategy back tester
#'
#' create functions/class to test different trading stragety
#' @export
run_backtest <- function(strategy = NULL, starttime, endtime, exchange, product, table_list) {
  #input: tables and a trading strategy class
  #output: orderbook and transaction record

  queue <- initialSubscriber(starttime, endtime, exchange, product, table_list)
  test <- backtest$new()
  test$init(starttime, endtime, exchange, product)

  dummy <- pblapply(1:10000, function(i) {
    stream <- runSubscriber(queue)
    if (is.null(stream)) break
    test$load_stream(stream)
    ### run stragegy
    if (i == 100) {
      test$make_order(timestamp = stream$timestamp, 10300, 1, "bid")
      test$make_order(timestamp = stream$timestamp, 10700, 1, "ask")
    }
    return(stream)
  })
  return(test)
}

# call:
# backtest(strategy, starttime, endtime)
# stragegy class
eps <<- 1e-8
verbose <<- TRUE

orderbook <- setRefClass(
  "orderbook",
  fields = c("book", "history", "exchange", "product", "type"),
  methods = list(
    init = function(tp, ex, pd) {
      type <<- tp
      exchange <<- ex
      product <<- pd
      book <<- history <<- data.table(
        orderID = numeric(),
        timestamp = numeric(),
        price = numeric(),
        qty = numeric(),
        fill_qty = numeric(),
        ahead_qty = numeric()
        )
    },
    add_order = function(orderID, timestamp, price, qty, fill_qty, ahead_qty) {
      book <<- rbind(
        book,
        data.table(orderID, timestamp, price, qty, fill_qty, ahead_qty)
        )
      if (type == "bid") book <<- book[order(-price)]
      if (type == "ask") book <<- book[order(price)]
      return(invisible(NULL))
    },
    update_ahead_qty = function(ID, val) {
      book[orderID == ID, "ahead_qty"] <<- val
      return(invisible(NULL))
    },
    add_fill_qty = function(ID, val) {
      book[orderID == ID, "fill_qty"] <<- book[orderID == ID, fill_qty] + val
      if (abs(book[orderID == ID, fill_qty] - book[orderID == ID, qty]) < eps) {
        deactive_order(ID)
      }
      return(invisible(NULL))
    },
    deactive_order = function(ID) {
      if (ID %in% book$orderID) {
        history <<- rbind(
          history,
          book[orderID == ID]
        )
        book <<- book[orderID != ID]
      }
      return(invisible(NULL))
    },
    get_top_active_order = function() {
      if (nrow(book) > 0) return(book[1])
      return(invisible(NULL))
    },
    get_order = function(ID) {
      return(book[orderID == ID])
    },
    get_order_info = function(ID, name) {
      return(book[orderID == ID, name, with = F])
    },
    is_empty = function() {
      return(nrow(book) == 0)
    }
  )
)

backtest <- setRefClass(
  "backtest",
  fields = c("starttime", "endtime", "exchange", "product",
            "exchangebook", "bid_book", "ask_book", "transaction",
            "orderID"),
  methods = list(
    init = function(starttime, endtime, exchange, product) {
      starttime <<- starttime
      endtime <<- endtime
      exchange <<- exchange
      product <<- product
      exchangebook <<- data.table()
      bid_book <<- orderbook$new()
      bid_book$init("bid", exchange, product)
      ask_book <<- orderbook$new()
      ask_book$init("ask", exchange, product)
      transaction <<- data.table()
      orderID <<- 0
    },
    load_stream = function(stream) {
      if (attr(stream, "spec")[3] == "Trade" & nrow(exchangebook) > 0) fill_order(data.table(stream))
      if (attr(stream, "spec")[3] == "orderBook") load_exchangebook(data.table(stream))
    },
    load_exchangebook = function(stream) {
      exchangebook <<- data.table(stream)
    },
    generate_orderID = function() {
      orderID <<- orderID + 1
      return(orderID)
    },
    make_order = function(timestamp, price, qty, type) {
      # check market orderbook to see if it is a taker
      generate_orderID()
      book <- eval(parse(text = paste0(type, "_book")))
      reverse_type <- ifelse(type == "ask", "bid", "ask")
      book$add_order(orderID, timestamp, price, qty, 0, NA)
      for (i in 1:20) {
        exchange_price <- exchangebook[,get(paste0(reverse_type, "s_price_", i))]
        exchange_qty <- exchangebook[,get(paste0(reverse_type, "s_qty_", i))]
        if ((type == "ask") * (exchange_price >= price) +
          (type == "bid") * (exchange_price <= price)) {
          # triggle a transaction as taker
          trade_qty <- min(qty, exchange_qty)
          transaction <<- rbind(
            transaction,
            data.table(
              order_ID = orderID,
              timestamp = timestamp,
              price = exchange_price,
              qty = trade_qty,
              type = type,
              makerSide = substr(reverse_type, 1, 1)
              )
            )

          cat("Order", order$orderID, "Trade", trade_qty, "\n")
          book$add_fill_qty(orderID, trade_qty)
          exchangebook[,paste0(reverse_type, "s_qty_", i)] <<- exchange_qty - trade_qty
        }
        if (nrow(book$get_order(orderID)) == 0) {
          # all qty filled
          break
        }
      }
      remain_qty <- qty - book$get_order_info(orderID, "fill_qty")
      if (length(remain_qty) > 0) {
        # not all qty filled
        # check market orderbook to see how many qty of same price before us
        for (i in 1:20) {
          exchange_price <- exchangebook[,get(paste0(type, "s_price_", i))]
          exchange_qty <- exchangebook[,get(paste0(type, "s_qty_", i))]
          if (exchange_price == price) {
            book$update_ahead_qty(orderID, exchange_qty)
            break
          }
        }
      }
      return(invisible(NULL))
    },
    cancel_order = function(ID) {
      bid_book$deactivate(ID)
      ask_book$deactivate(ID)
    },
    fill_order = function(stream) {
      for (i in seq_len(nrow(stream))) {
        # look through trades
        trade <- stream[i]
        to_trade_qty <- trade$qty
        # check if there is possible transaction
        possible_transaction <- FALSE
        if (!bid_book$is_empty()) {
          if (trade$price <= bid_book$get_top_active_order()$price) {
            possible_transaction <- TRUE
            type <- "bid"
            book <- bid_book
          }
        }
        if (!ask_book$is_empty()) {
          if (trade$price >= ask_book$get_top_active_order()$price) {
            possible_transaction <- TRUE
            type <- "ask"
            book <- ask_book
          }
        }
        if (possible_transaction) {
          # there might be transaction
          order <- book$get_top_active_order()
          for (i in 1:20) {
            exchange_price <- exchangebook[,get(paste0(type, "s_price_", i))]
            exchange_qty <- exchangebook[,get(paste0(type, "s_qty_", i))]
            # compare order, exchange and trade to re-execute trade

            if ((type == "bid") * (exchange_price < order$price) +
                (type == "ask") * (exchange_price > order$price)) {
              # trigger transaction at order price, maker side
              trade_qty <- min(to_trade_qty, order$qty - order$fill_qty)
              cat("Order", order$orderID, "Trade", trade_qty, "\n")
              book$add_fill_qty(order$orderID, trade_qty)
              book$update_ahead_qty(order$orderID, 0)
              transaction <<-
                rbind(transaction,
                      data.table(order_ID = order$orderID,
                                 timestamp = trade$timestamp,
                                 price = order$price,
                                 qty = trade_qty,
                                 type = type,
                                 makerSide = substr(type, 1, 1)))
              to_trade_qty <- to_trade_qty - trade_qty
              break
            } else if ((type == "bid") * (exchange_price > order$price) +
                (type == "ask") * (exchange_price < order$price)) {
              # fill exchange first
              trade_qty <- min(to_trade_qty, exchange_qty)
              to_trade_qty <- to_trade_qty - trade_qty
              exchangebook[,paste0(type, "s_qty_", i)] <<- exchange_qty - trade_qty
              if (to_trade_qty < eps) {
                # trade over, nothing happen
                break
              }
            } else if (exchange_price == order$price) {
              # top price but need to wait in queue
              trade_qty <- min(to_trade_qty, exchange_qty, order$ahead_qty, na.rm = T)
              to_trade_qty <- to_trade_qty - trade_qty
              exchangebook[,paste0(type, "s_qty_", i)] <<- exchange_qty - trade_qty
              if (to_trade_qty < eps) {
                # trade over, but a little bit ahead in queue
                book$update_ahead_qty(order$orderID, order$ahead_qty - trade_qty)
                break
              } else {
                # trade triggled, maker side
                trade_qty <- min(to_trade_qty, order$qty - order$fill_qty)

                cat("Order", order$orderID, "Trade", trade_qty, "\n")
                book$add_fill_qty(order$orderID, trade_qty)
                book$update_ahead_qty(order$orderID, 0)
                transaction <<-
                  rbind(transaction,
                  data.table(order_ID = order$orderID,
                             timestamp = trade$timestamp,
                             price = order$price,
                             qty = trade_qty,
                             type = type,
                             makerSide = substr(type, 1, 1)))
                to_trade_qty <- to_trade_qty - trade_qty
                if (to_trade_qty < eps) {
                  break
                }
              }
            }
          } #loop through 20
          #???
          #if (type == "bid") bid_book <<- book
          #if (type == "ask") ask_book <<- book
        } else {
          # there cannot be transaction
          # just update exchangebook
          type <- ifelse(trade$price <= exchangebook$bids_price_1, "bid", "ask")
          for (i in 1:20) {
            exchange_price <- exchangebook[,get(paste0(type, "s_price_", i))]
            exchange_qty <- exchangebook[,get(paste0(type, "s_qty_", i))]
            if (exchange_price == trade$price) {
              exchangebook[, paste0(type, "s_qty_", i)] <<- exchange_qty - trade$qty
              break
            }
          }
        }
      } # loop through trades
    }
  )
)

