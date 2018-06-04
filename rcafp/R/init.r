#' initialize environment and define constants
#'
#' initialize environment and define constants
.onLoad <- function(libname, pkgname) {
  eps <<- 1e-8
  verbose <<- TRUE
  fee_rate <<- 0.003
  root_path <<- "~/Downloads/Data"
  invisible()
}
