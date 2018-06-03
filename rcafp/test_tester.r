### unit test for tester
root_path <- "~/Downloads/Data"
exchange <- "GDAX"
product <- "BTC"
table_list <- c("orderBook", "Trade")
starttime = "20180117T120000"
endtime = "20180117T123000"

# initialization
queue <- initialSubscriber(starttime, endtime, exchange, product, table_list)
test <- backtest$new()
test$init(starttime, endtime, exchange, product, 1000000, 2)

# loading first rec
stream <- runSubscriber(queue)
print(stream)
test$load_stream(stream)

test
# 1.make a bid order @ 10494.54
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10494.54,
  qty = 1,
  type = "bid")

test

# 2.make an ask order @ 10500
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10500,
  qty = 1,
  type = "ask")

test

# 3.make a bid order @ 10494.55 (market order)
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10494.55,
  qty = 1,
  type = "bid")

test

# 4.make an ask order @ 10494 (market order)
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10494,
  qty = 1,
  type = "ask")

test

# 5.make a bid order @ 10494.55 qty 10 (market order)
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10494.55,
  qty = 10,
  type = "bid")

test

# 6. make an ask order @ 10470 qty 2.5 (market order)
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10470,
  qty = 2.5,
  type = "ask")

test

# 7. make an ask order @ 10440 qty 9 (market order)
test$place_order(
  timestamp = test$exchangebook$timestamp,
  price = 10440,
  qty = 9,
  type = "ask")

test

# cancel orders
test$cancel_order(1)
test$cancel_order(5)
test


