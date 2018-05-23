#' strategy back tester
#'
#' create functions/class to test different trading stragety
#' @export
run_backtest <- function(strategy = NULL, starttime, endtime, exchange, product, table_list) {
  #input: tables and a trading strategy class
  #output: orderbook and transaction record

  queue <- initialSubscriber(starttime, endtime, exchange, product, table_list)
  test <- backtest$new()
  test$init(starttime, endtime, exchange, product, 100000, 100)

  dummy <- pblapply(1:10000, function(i) {
    stream <- runSubscriber(queue)
    if (is.null(stream)) break
    test$load_stream(stream)
    #print(i)
    #print(stream)
    ### run stragegy
    if (i == 100) {
      test$place_order(timestamp = stream$timestamp, 10300, 1, "bid")
      test$place_order(timestamp = stream$timestamp, 10700, 1, "ask")
    }
    return(stream)
  })
  return(test)
}

###
eps <<- 1e-8
verbose <<- TRUE
fee_rate <<- 0.003

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
    update_qty = function(ID, val) {
      # only call this when place_order
      book[orderID == ID, "qty"] <<- book[orderID == ID, fill_qty] + val
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
    },
    is_active = function(ID) {
      return(nrow(book[orderID == ID]) > 0)
    }
  )
)

backtest <- setRefClass(
  "backtest",
  fields = c("starttime", "endtime", "exchange", "product",
            "exchangebook", "bid_book", "ask_book", "transaction",
            "orderID", "capital", "hold_capital", "position", "hold_position"),
  methods = list(
    init = function(starttime, endtime, exchange, product, capital = 0, position = 0) {
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
      capital <<- capital
      hold_capital <<- 0
      position <<- position
      hold_position <<- 0
    },
    load_stream = function(stream) {
      if (attr(stream, "spec")[3] == "Trade" & nrow(exchangebook) > 0) fill_order(data.table(stream))
      if (attr(stream, "spec")[3] == "orderBook") load_exchangebook(data.table(stream))
    },
    load_exchangebook = function(stream) {
      exchangebook <<- data.table(stream)
    },
    add_capital = function(amt) {
      capital <<- capital + amt
    },
    add_position = function(qty) {
      position <<- position + qty
    },
    generate_orderID = function() {
      orderID <<- orderID + 1
      return(orderID)
    },
    place_order = function(timestamp, price, qty, type) {
      generate_orderID()
      book <- eval(parse(text = paste0(type, "_book")))
      reverse_type <- ifelse(type == "ask", "bid", "ask")
      book$add_order(orderID, timestamp, price, qty, 0, 0)
      # check market orderbook to see if it is a taker
      for (i in 1:20) {
        exchange_price <- exchangebook[,get(paste0(reverse_type, "s_price_", i))]
        exchange_qty <- exchangebook[,get(paste0(reverse_type, "s_qty_", i))]
        if ((type == "ask") * (exchange_price >= price) +
            (type == "bid") * (exchange_price <= price)) {
          # triggle a transaction as taker
          trade_qty <- min(qty, exchange_qty)
          if (type == "bid") {
            if (capital == 0) {
              cat("No capital to place a bid order.")
              return(invisible(NULL))
            }
            max_qty <- capital / exchange_price / (1 + fee_rate)
            if (max_qty < trade_qty) {
              cat("Insufficient capital. A smaller bid order is placed.")
              trade_qty <- min(trade_qty, max_qty)
            }
          } else if (type == "ask") {
            if (position == 0) {
              cat("No position to place an ask order.")
              return(invisible(NULL))
            }
            if (position < trade_qty) {
              cat("Insufficient position. A smaller ask order is placed.")
              trade_qty <- min(trade_qty, position)
            }
          }
          amt <- exchange_price * trade_qty
          fee <- exchange_price * trade_qty * fee_rate
          if (type == "bid") {
            capital <<- capital - amt - fee
            position <<- position + trade_qty
          } else if (type == "ask") {
            capital <<- capital + amt - fee
            position <<- position - trade_qty
          }
          book$add_fill_qty(orderID, trade_qty)
          trans <-
            data.table(
              order_ID = orderID,
              timestamp = timestamp,
              price = exchange_price,
              qty = trade_qty,
              type = type,
              makerSide = ifelse(reverse_type == "bid", "b", "s"),
              fee = fee
            )
          transaction <<- rbind(transaction, trans)
          exchangebook[,paste0(reverse_type, "s_qty_", i)] <<- exchange_qty - trade_qty
          if (verbose) print(trans)
        }
        if ((type == "bid") * (abs(capital) < eps) +
            (type == "ask") * (abs(position) < eps)) {
          # cancel remaining part of order
          book$deactive_order(orderID)
          return(invisible(NULL))
        }
        if (!book$is_active(orderID)) {
          # all qty filled
          return(invisible(NULL))
        }
      }
      # still have remaining qty
      # now add orderbook
      remain_qty <- qty - as.numeric(book$get_order_info(orderID, "fill_qty"))
      # check sufficient capitals
      if (type == "bid") {
        max_qty <- capital / price
        if (max_qty < remain_qty) {
          cat("Insufficient capital. A smaller bid order is placed.")
          remain_qty <- min(remain_qty, max_qty)
        }
      } else if (type == "ask") {
        if (position < remain_qty) {
          cat("Insufficient position. A smaller ask order is placed.")
          remain_qty <- min(remain_qty, position)
        }
      }
      book$update_qty(orderID, remain_qty)
      if (type == "bid") {
        hold_capital <<- hold_capital + price * remain_qty
        capital <<- capital - price * remain_qty
      } else {
        hold_position <<- hold_position + remain_qty
        position <<- position - remain_qty
      }
      # check market orderbook to see how many qty of same price ahead of this order
      for (i in 1:20) {
        exchange_price <- exchangebook[,get(paste0(type, "s_price_", i))]
        exchange_qty <- exchangebook[,get(paste0(type, "s_qty_", i))]
        if (exchange_price == price) {
          book$update_ahead_qty(orderID, exchange_qty)
          break
        }
      }
      return(invisible(NULL))
    },
    cancel_order = function(ID) {
      if (bid_book$is_active(ID)) {
        type <- "bid"
        bid_book$deactivate(ID)
        order <- bid_book$get_order(ID)
        capital <<- capital + as.numeric((order$qty - order$fill_qty) * order$price)
        hold_capital <<- hold_capital - as.numeric((order$qty - order$fill_qty) * order$price)
      } else if (ask_book$is_active(ID)) {
        type <- "ask"
        ask_book$deactivate(ID)
        order <- ask_book$get_order(ID)
        position <<- position + as.numeric((order$qty - order$fill_qty))
        hold_position <<- hold_position - as.numeric((order$qty - order$fill_qty))
      } else {
        # do nothing
      }
      return(invisible(NULL))
    },
    trigger_order = function(orderID, timestamp, type, book, price, qty) {
      if (type == "bid") {
        hold_capital <<- hold_capital - price * qty
        position <<- position + qty
      } else if (type == "ask") {
        capital <<- capital + price * qty
        hold_position <<- hold_position - qty
      }
      book$add_fill_qty(orderID, qty)
      book$update_ahead_qty(orderID, 0)
      trans <-
        data.table(order_ID = orderID,
                   timestamp = timestamp,
                   price = price,
                   qty = qty,
                   type = type,
                   makerSide = ifelse(type == "bid", "b", "s"),
                   fee = 0)
      transaction <<- rbind(transaction, trans)
      if (verbose) print(trans)
      return(invisible(NULL))
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
          # order might be filled
          order <- book$get_top_active_order()
          if ((trade$price > order$price) || (trade$price == order$price && order$ahead_qty < eps)) {
            # all exchange orders in between are filled or cancelled
            # trigger maker side transaction at order price
            trade_qty <- min(to_trade_qty, order$qty - order$fill_qty)
            trigger_order(order$orderID, trade$timestamp, type, book, order$price, trade_qty)
            to_trade_qty <- to_trade_qty - trade_qty
          } else {
            # trade price == order price and
            # need to check exchange book
            for (i in 1:20) {
              exchange_price <- exchangebook[,get(paste0(type, "s_price_", i))]
              exchange_qty <- exchangebook[,get(paste0(type, "s_qty_", i))]
              if ((type == "bid") * (exchange_price > order$price) +
                  (type == "ask") * (exchange_price < order$price)) {
                # this exchange order must be filled or cancelled
                # only update exchange book
                exchangebook[,paste0(type, "s_qty_", i)] <<- 0
              } else if (
                (type == "bid") * (exchange_price < order$price) +
                (type == "ask") * (exchange_price > order$price)
              ) {
                # this exchange order is not close to book order
                # trigger maker side transaction at order price
                trade_qty <- min(to_trade_qty, order$qty - order$fill_qty)
                trigger_order(order$orderID, trade$timestamp, type, book, order$price, trade_qty)
                to_trade_qty <- to_trade_qty - trade_qty
              } else if (exchange_price == order$price) {
                # fill exchange order ahead of book order first
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
                  trigger_order(order$orderID, trade$timestamp, type, book, order$price, trade_qty)
                  to_trade_qty <- to_trade_qty - trade_qty
                }
              }
              if (to_trade_qty < eps || !book$is_active(order$orderID)) {
                break
              }
            } # loop through exchange orders
          }
        } else {
          # impossible to fill
          # just update exchangebook
          type <- ifelse(trade$price <= exchangebook$bids_price_1, "bid", "ask")
          for (i in 1:20) {
            exchange_price <- exchangebook[,get(paste0(type, "s_price_", i))]
            exchange_qty <- exchangebook[,get(paste0(type, "s_qty_", i))]
            if (exchange_price == trade$price) {
              exchangebook[, paste0(type, "s_qty_", i)] <<- max(0, exchange_qty - trade$qty)
              break
            }
          }
        }
      } # loop through trades
    }
  )
)
