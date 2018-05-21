#' data loader
#'
#' This package allows you to load Cryptocurrency data
#' @export
root_path <- "~/Downloads/Data"

### list all files in one folder
#' @export
fileLister <- function(starttime, endtime, exchange, product, path, table) {
  all_files <- list.files(path, ".csv$")
  if (length(all_files) == 0) {
    # cat("No Data\n")
    return(NULL)
  }
  timestamps <- gsub(".csv$", "", all_files)
  first_ind <- min(which(timestamps >= starttime))
  last_ind <- max(which(timestamps <= endtime))
  # to read one more file from both ends for completeness
  first_ind <- ifelse(first_ind > 1, first_ind - 1, first_ind)
  last_ind <- ifelse(last_ind < length(timestamps), last_ind + 1, last_ind)
  timestamps <- timestamps[first_ind:last_ind]
  all_files <- paste0(timestamps, ".csv")
  return(all_files)
}

### load and stack all files in one folder
#' @export
csvDriver <- function(starttime, endtime, exchange, product, path, table) {
  require(data.table, quietly = T, warn.conflicts = F)
  require(pbapply, quietly = T, warn.conflicts = F)
  all_files <- fileLister(starttime, endtime, exchange, product, path, table)
  if (!is.null(all_files)) {
    cat("Loading", path, "\n")
    data_list <- pblapply(all_files, function(f) fread(file.path(path, f)))
    dt <- do.call(rbind, data_list)
    return(dt[timestamp >= epoch_to_timestamp(starttime) & timestamp <= epoch_to_timestamp(endtime)])
  } else {
    return(invisible(NULL))
  }
}

### load and stack all files for one type of table
#' @export
runQuery <- function(starttime, endtime, exchange_list, product_list, table) {
  # query data
  startdate <- as.Date(substr(starttime, 1, 8), "%Y%m%d")
  enddate <- as.Date(substr(endtime, 1, 8), "%Y%m%d")
  date_list <- format(as.Date(startdate:enddate, origin = '1970-01-01'), "%Y%m%d")
  specs <- expand.grid(exchange = exchange_list, product = product_list, date = date_list)
  data_list <- apply(specs, 1, function(tuple) {
    path <- file.path(root_path, table, tuple[1], tuple[2], tuple[3])
    # print(path)
    csvDriver(starttime, endtime, exchange, product, path, table)
  })
  if (length(data_list) == 0) {
    cat("No Data!!\n")
    return(invisible(NULL))
  } else {
    data <- do.call(rbind, data_list)
    cat("runQuery finished\n")
    return(data)
  }
}

### initiate Subscriber runs for a list of specs
#' @export
initialSubscriber <- function(starttime, endtime, exchange_list, product_list, table_list) {
  require(liqueueR, quietly = T, warn.conflicts = F)
  require(iterators, quietly = T, warn.conflicts = F)
  require(itertools, quietly = T, warn.conflicts = F)
  startdate <- as.Date(substr(starttime, 1, 8), "%Y%m%d")
  enddate <- as.Date(substr(endtime, 1, 8), "%Y%m%d")
  date_list <- format(as.Date(startdate:enddate, origin = '1970-01-01'), "%Y%m%d")
  specs <- expand.grid(table = table_list, exchange = exchange_list, product = product_list, date = date_list, stringsAsFactors = F)
  iter_obj_initial <- lapply(seq_len(nrow(specs)), function(i) {
    table <- specs$table[i]
    exchange <- specs$exchange[i]
    product <- specs$product[i]
    date <- specs$date[i]
    path <- file.path(root_path, table, exchange, product, date)
    # print(path)
    iter_obj <- csvStreamerGenerator(starttime, endtime, exchange, product, path, table)
    return(iter_obj)
  })
  iter_obj_initial[sapply(iter_obj_initial, is.null)] <- NULL
  iter_obj_queue <- PriorityQueue$new()
  cat("Initializing Queue...\n")
  for (i in seq_along(iter_obj_initial)) {
    # load first line
    iter_obj <- iter_obj_initial[[i]]
    iter_obj <- csvStreamer(iter_obj)
    # make sure to load data within range
    # if too late then exit
    if (iter_obj$data$timestamp > epoch_to_timestamp(endtime)) break
    # if too early then iterate to first timestamp within range
    while (iter_obj$data$timestamp < epoch_to_timestamp(starttime)) {
      iter_obj <- csvStreamer(iter_obj)
      if (is.null(iter_obj$data)) break
    }
    # if valid then push
    if (!is.null(iter_obj$data)) iter_obj_queue$push(iter_obj, -iter_obj$data$timestamp)
    # if reach to end of file then skip
  }
  return(iter_obj_queue)
}

### run Subscriber to get stream of data by time
#' @export
runSubscriber <- function(iter_obj_queue) {
  require(data.table)
  iter_obj <- iter_obj_queue$poll()
  if (is.null(iter_obj)) {
    cat("End of File\n")
    return(invisible(NULL))
  }
  dt <- data.table(iter_obj$data)
  current_time <- dt$timestamp
  spec <- attr(iter_obj, "spec")
  attr(dt, "spec") <- spec
  iter_obj <- csvStreamer(iter_obj)
  if (!is.null(iter_obj$data)) {
    while(iter_obj$data$timestamp == current_time) {
      dt <- rbind(dt, data.table(iter_obj$data))
      attr(dt, "spec") <- spec
      iter_obj <- csvStreamer(iter_obj)
      if (is.null(iter_obj$data)) {
        return(dt)
      }
    }
    # until next timestamp
    # push to queue
    iter_obj_queue$push(iter_obj, -iter_obj$data$timestamp)
  }
  return(dt)
}

### helper functions for subscriber/streamer
#' @export
csvStreamerGenerator <- function(starttime, endtime, exchange, product, path, table) {
  all_files <- fileLister(starttime, endtime, exchange, product, path, table)
  if (is.null(all_files)) return(NULL)
  iter_obj <- list(data = NULL, file_it = ihasNext(all_files), reader_it = NULL)
  attr(iter_obj, "spec") <- c(exchange, product, table, path)
  return(iter_obj)
}

csvStreamer <- function(iter_obj) {
  reader_it <- iter_obj$reader_it
  file_it <- iter_obj$file_it
  spec <- attr(iter_obj, "spec")
  if (is.null(reader_it) || !hasNext(reader_it)) {
    if (hasNext(file_it)) {
      file <- nextElem(file_it)
      full_path <- file.path(spec[4], file)
      reader_it <- ihasNext(iread.table(full_path, header = T, row.names = NULL, sep = ","))
    } else {
      iter_obj <- list(data = NULL, file_it = file_it, reader_it = reader_it)
      attr(iter_obj, "spec") <- spec
      return(iter_obj)
    }
  }
  data <- nextElem(reader_it)
  # print(data)
  iter_obj <- list(data = data, file_it = file_it, reader_it = reader_it)
  attr(iter_obj, "spec") <- spec
  return(iter_obj)
}

# utility function
epoch_to_timestamp <- function(epoch) {
  return(as.numeric(strptime(epoch, "%Y%m%dT%H%M%S"))*1000)
}

timestamp_to_epoch <- function(timestamp) {
  return(strftime(as.POSIXct(timestamp/1000, origin = "1970-01-01 00:00:00"), "%Y%m%dT%H%M%S"))
}
