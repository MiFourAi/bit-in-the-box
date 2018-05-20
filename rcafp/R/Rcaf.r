### data loader ###
root_path <- "~/Downloads/Data"

### list all files in one folder
fileLister <- function(starttime, endtime, exchange, product, path, table) {
  all_files <- list.files(path, ".csv$")
  if (length(all_files) == 0) {
    # cat("No Data\n")
    return(NULL)
  }
  timestamps <- gsub(".csv$", "", all_files)
  timestamps <- timestamps[timestamps <= endtime & timestamps >= starttime]
  all_files <- paste0(timestamps, ".csv")
  return(all_files)
}

### load and stack all files in one folder
csvDriver <- function(starttime, endtime, exchange, product, path, table) {
  require(data.table, quietly = T, warn.conflicts = F)
  require(pbapply, quietly = T, warn.conflicts = F)
  all_files <- fileLister(starttime, endtime, exchange, product, path, table)
  if (!is.null(all_files)) {
    cat("Loading", path, "\n")
    data_list <- pblapply(all_files, function(f) fread(file.path(path, f)))
    return(do.call(rbind, data_list))
  } else {
    return(invisible(NULL))
  }
}

### load and stack all files for one type of table
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
initialSubscriber <- function(starttime, endtime, exchange_list, product_list, table_list) {
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
  # first run
  iter_obj_list <<- lapply(seq_along(iter_obj_initial), function(i) {
    csvStreamer(iter_obj_initial[[i]])
  })
  return(invisible(NULL))
}

### run Subscriber to get stream of data by time
runSubscriber <- function() {
  if (exists("iter_obj_list", envir = .GlobalEnv)) {
    first_ind <- which.min(sapply(iter_obj_list, function(iter_obj) {
      ifelse(is.null(iter_obj$data$timestamp), Inf, iter_obj$data$timestamp)
    }))
    if (is.infinite(first_ind)) break
    dt <- iter_obj_list[[first_ind]]$data
    spec <- attr(iter_obj_list[[first_ind]], "spec")
    tmp_iter_obj_list <- iter_obj_list
    tmp_iter_obj_list[[first_ind]] <- csvStreamer(iter_obj_list[[first_ind]])
    iter_obj_list <<- tmp_iter_obj_list
    attr(dt, "spec") <- spec
    return(dt)
  } else {
    cat("Need to Initiate Subscriber.\n")
    return(invisible(NULL))
  }
}

### helper functions for subscriber
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
      reader_it <- ihasNext(iread.table(file.path(spec[4], file), header = T, row.names = NULL, sep = ","))
      # read header line
      header <- nextElem(reader_it)
    } else {
      iter_obj <- list(data = NULL, file_it = file_it, reader_it = reader_it)
      attr(iter_obj, "attr") <- spec
      return(iter_obj)
    }
  }
  data <- nextElem(reader_it)
  # print(data)
  iter_obj <- list(data = data, file_it = file_it, reader_it = reader_it)
  attr(iter_obj, "spec") <- spec
  return(iter_obj)
}
