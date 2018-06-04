#' initialize environment and define constants
#'
#' initialize environment and define constants
.onLoad <- function(libname, pkgname) {
  eps <<- 1e-8
  verbose <<- TRUE
  fee_rate <<- 0.003
  max_exchange_order_ind <<- 5
  root_path <<- "~/Downloads/Data"
  window <<- 5 * 60 * 1000
  thres_pct <<- 0.01
  invisible()
}
