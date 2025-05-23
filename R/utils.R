# Internal utility function for NULL coalescing
`%||%` <- function(a, b) if (!is.null(a)) a else b

utils::globalVariables(c(
  "ValidFrom", "ValidTo", "current", "typesR2DBMS", "odbcGetInfo",
  "capture.output", "r"
))

custom_sqlSave <-
  function (channel, dat, tablename = NULL, append = FALSE, rownames = TRUE,
            colnames = FALSE, verbose = FALSE, safer = TRUE, addPK = FALSE,
            typeInfo, varTypes, fast = TRUE, test = FALSE, nastring = NULL) {
    if (!RODBC:::odbcValidChannel(channel))
      stop("first argument is not an open RODBC channel")
    if (missing(dat))
      stop("missing parameter")
    if (!is.data.frame(dat))
      stop("should be a data frame")
    if (is.null(tablename))
      tablename <- if (length(substitute(dat)) == 1)
        as.character(substitute(dat))
    else as.character(substitute(dat)[[2L]])
    if (length(tablename) != 1L)
      stop(sQuote(tablename), " should be a name")
    switch(attr(channel, "case"), nochange = {
    }, toupper = {
      tablename <- toupper(tablename)
      colnames(dat) <- toupper(colnames(dat))
    }, tolower = {
      tablename <- tolower(tablename)
      colnames(dat) <- tolower(colnames(dat))
    })
    keys <- -1
    if (is.logical(rownames) && rownames)
      rownames <- "rownames"
    if (is.character(rownames)) {
      dat <- cbind(row.names(dat), dat)
      names(dat)[1L] <- rownames
      if (addPK) {
        keys <- vector("list", 4L)
        keys[[4L]] <- rownames
      }
    }
    if (is.logical(colnames) && colnames) {
      dat <- as.data.frame(rbind(colnames(dat), as.matrix(dat)))
    }
    dbname <- RODBC:::odbcTableExists(channel, tablename, abort = FALSE)
    if (length(dbname)) {
      if (!append) {
        if (safer)
          stop("table ", sQuote(tablename), " already exists")
        query <- paste("DELETE FROM", dbname)
        if (verbose)
          cat("Query: ", query, "\n", sep = "")
        res <- RODBC::sqlQuery(channel, query, errors = FALSE)
        if (is.numeric(res) && res == -1L)
          stop(paste(RODBC::odbcGetErrMsg(channel), collapse = "\n"))
      }
      if (custom_sqlwrite(channel, tablename, dat, verbose = verbose,
                          fast = fast, test = test, nastring = nastring) ==
          -1) {
        query <- paste("DROP TABLE", dbname)
        if (verbose) {
          cat("sqlwrite returned ", RODBC::odbcGetErrMsg(channel),
              "\n", sep = "\n")
          cat("Query: ", query, "\n", sep = "")
        }
        if (safer)
          stop("unable to append to table ", sQuote(tablename))
        res <- RODBC::sqlQuery(channel, query, errors = FALSE)
        if (is.numeric(res) && res == -1L)
          stop(paste(RODBC::odbcGetErrMsg(channel), collapse = "\n"))
      }
      else {
        return(invisible(1L))
      }
    }
    types <- sapply(dat, typeof)
    facs <- sapply(dat, is.factor)
    isreal <- (types == "double")
    isint <- (types == "integer") & !facs
    islogi <- (types == "logical")
    colspecs <- rep("varchar(255)", length(dat))
    if (!missing(typeInfo) || !is.null(typeInfo <- typesR2DBMS[[odbcGetInfo(channel)[1L]]])) {
      colspecs <- rep(typeInfo$character[1L], length(dat))
      colspecs[isreal] <- typeInfo$double[1L]
      colspecs[isint] <- typeInfo$integer[1L]
      colspecs[islogi] <- typeInfo$logical[1L]
    }
    else {
      typeinfo <- RODBC::sqlTypeInfo(channel, "all", errors = FALSE)
      if (is.data.frame(typeinfo)) {
        if (any(isreal)) {
          realinfo <- RODBC::sqlTypeInfo(channel, "double")[,
                                                             1L]
          if (length(realinfo) > 0L) {
            if (length(realinfo) > 1L) {
              nm <- match("double", tolower(realinfo))
              if (!is.na(nm))
                realinfo <- realinfo[nm]
            }
            colspecs[isreal] <- realinfo[1L]
          }
          else {
            realinfo <- RODBC::sqlTypeInfo(channel, "float")[,
                                                              1L]
            if (length(realinfo) > 0L) {
              if (length(realinfo) > 1L) {
                nm <- match("float", tolower(realinfo))
                if (!is.na(nm))
                  realinfo <- realinfo[nm]
              }
              colspecs[isreal] <- realinfo[1L]
            }
          }
        }
        if (any(isint)) {
          intinfo <- RODBC::sqlTypeInfo(channel, "integer")[,
                                                             1L]
          if (length(intinfo) > 0L) {
            if (length(intinfo) > 1) {
              nm <- match("integer", tolower(intinfo))
              if (!is.na(nm))
                intinfo <- intinfo[nm]
            }
            colspecs[isint] <- intinfo[1L]
          }
        }
      }
    }
    names(colspecs) <- names(dat)
    if (!missing(varTypes)) {
      if (!length(nm <- names(varTypes)))
        warning("argument 'varTypes' has no names and will be ignored")
      OK <- names(colspecs) %in% nm
      colspecs[OK] <- varTypes[names(colspecs)[OK]]
      notOK <- !(nm %in% names(colspecs))
      if (any(notOK))
        warning("column(s) ", paste(nm[notOK], collapse = ", "),
                " 'dat' are not in the names of 'varTypes'")
    }
    query <- RODBC:::sqltablecreate(channel, tablename, colspecs = colspecs,
                                    keys = keys)
    if (verbose)
      cat("Query: ", query, "\n", sep = "")
    res <- RODBC::sqlQuery(channel, query, errors = FALSE)
    if (is.numeric(res) && res == -1)
      stop(paste(RODBC::odbcGetErrMsg(channel), collapse = "\n"))
    if (custom_sqlwrite(channel, tablename, dat, verbose = verbose,
                        fast = fast, test = test, nastring = nastring) < 0) {
      err <- RODBC::odbcGetErrMsg(channel)
      msg <- paste(err, collapse = "\n")
      if ("missing column name" %in% err)
        msg <- paste(msg, "Check case conversion parameter in odbcConnect",
                     sep = "\n")
      stop(msg)
    }
    invisible(1L)
  }


custom_sqlwrite <- function (channel, tablename, mydata, test = FALSE, fast = TRUE,
                             nastring = NULL, verbose = FALSE){
  if (!RODBC:::odbcValidChannel(channel))
    stop("first argument is not an open RODBC channel")
  colnames <- as.character(RODBC::sqlColumns(channel, tablename)[4L][,1L])

  #CUSTOM CHANGE START: REMOVE AUTO-GENERATED COLUMNS!
  auto_generated_colindices <- which(!is.na(RODBC::sqlColumns(channel, tablename)[13L][,1L]))
  if (length(auto_generated_colindices)>0) {
    colnames <- colnames[-auto_generated_colindices]
  }
  #CUSTOM CHANGE END

  colnames <- RODBC:::mangleColNames(colnames)
  cnames <- paste(RODBC:::quoteColNames(channel, colnames), collapse = ", ")
  dbname <- RODBC:::quoteTabNames(channel, tablename)
  if (!fast) {
    for (i in seq_along(mydata)) if (is.logical(mydata[[i]]))
      mydata[[i]] <- as.character(mydata[[i]])
    data <- as.matrix(mydata)
    if (nchar(enc <- attr(channel, "encoding")) && is.character(data))
      data <- iconv(data, to = enc)
    colnames(data) <- colnames
    cdata <- sub("\\([[:digit:]]*\\)", "", RODBC::sqlColumns(channel,
                                                              tablename)[, "DATA_TYPE"])
    tdata <- RODBC::sqlTypeInfo(channel)
    nr <- match(cdata, tdata[, 2L])
    tdata <- as.matrix(tdata[nr, 4:5])
    tdata[is.na(nr), ] <- "'"
    for (cn in seq_along(cdata)) {
      td <- as.vector(tdata[cn, ])
      if (is.na(td[1L]))
        next
      if (identical(td, rep("'", 2L)))
        data[, cn] <- gsub("'", "''", data[, cn])
      data[, cn] <- paste(td[1L], data[, cn], td[2L],
                          sep = "")
    }
    data[is.na(mydata)] <- if (is.null(nastring))
      "NULL"
    else nastring[1L]
    for (i in 1L:nrow(data)) {
      query <- paste("INSERT INTO", dbname, "(", cnames,
                     ") VALUES (", paste(data[i, colnames], collapse = ", "),
                     ")")
      if (verbose)
        cat("Query: ", query, "\n", sep = "")
      if (RODBC::odbcQuery(channel, query) < 0)
        return(-1L)
    }
  }
  else {
    query <- paste("INSERT INTO", dbname, "(", cnames, ") VALUES (",
                   paste(rep("?", ncol(mydata)), collapse = ","), ")")
    if (verbose)
      cat("Query: ", query, "\n", sep = "")
    coldata <- RODBC::sqlColumns(channel, tablename)[c(4L, 5L,
                                                        7L, 9L)]
    if (any(is.na(m <- match(colnames, coldata[, 1]))))
      return(-1L)
    if (any(notOK <- (coldata[, 3L] == 0L))) {
      types <- coldata[notOK, 2]
      tdata <- RODBC::sqlTypeInfo(channel)
      coldata[notOK, 3L] <- tdata[match(types, tdata[,
                                                     2L]), 3L]
    }
    if (RODBC::odbcUpdate(channel, query, mydata, coldata[m, ],
                           test = test, verbose = verbose, nastring = nastring) <
        0)
      return(-1L)
  }
  return(invisible(1L))
}



