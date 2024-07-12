# Internal utility function for NULL coalescing
`%||%` <- function(a, b) if (!is.null(a)) a else b
