#' Generic time series manipulation functions in R

#' packages
library(xts)
library(pryr)
library(dplyr)
library(zoo)
library(forecast)

#' functions
xts_range <- function(start, end) {
    #' Create an xts range from start and end dates
    return(paste(start, end, sep = "::"))
}


generate_xts <- function(len) {
    #' Generate a random xts series of length len
    return(xts(runif(len), seq(from = Sys.Date(), by = 1, length.out = len)))
}


make_xts <- function(data) {
    #' Attempt to convert the data into xts data
    if (is.list(data)) {
        return(lapply(data, make_xts))
    } else {
        return(as.xts(ts(data)))
    }
}


convert_xts <- function(serie) {
    if (is(serie, "data.frame")) {
        serie <- as.xts(serie[[2]], order.by = as.POSIXct(serie[[1]], tz = tz(serie[[1]][[1]])))
    }

    return(serie)
}

convert_xts_weekly <- function(serie) {
    if (is(serie, "data.frame")) {
        serie <- as.xts(serie[[2]], order.by = as.Date(serie[[1]], tz = tz(serie[[1]][[1]])))
    }

    res <- xts::apply.weekly(serie, sum)
    attr(res, 'frequency') <- 52

    ## Hard setting endpoints to be a full week
    ## myTs <- clus_tel_day$del$orig[[5]]
    ## ep <- endpoints(myTs,'weeks')
    ## ts.weeks <- apply.monthly(myTs, sum)
    ## attributes(ts.weeks)$index[length(ts.weeks)] <- as.POSIXct(last(index(myTs)) + 7-last(floor(diff(ep))))

    return(res)
}


convert_xts_daily <- function(serie, FUN = sum) {
    if (is(serie, "data.frame")) {
        serie <- as.xts(serie[[2]], order.by = as.Date(serie[[1]], tz = tz(serie[[1]][[1]])))
    }

    res <- xts::apply.daily(serie, FUN)
    attr(res, 'frequency') <- 7
    return(res)
}


convert_xts_monthly <- function(serie) {
    if (is(serie, "data.frame")) {
        serie <- as.xts(serie[[2]], order.by = as.Date(serie[[1]], tz = tz(serie[[1]][[1]])))
    }

    res <- xts::apply.monthly(serie, sum)
    attr(res, 'frequency') <- 12
    return(res)
}


merge_xts_list <- function(l) {
    #' Merge multiple xts series contianed in a list
    return(Reduce(merge, l[2:length(l)], l[[1]]))
}


trim_ts <- function(serie, n, how = "both") {
    #' Remove n values in a time series object.
    #' Series can be trimed from the start, the end and both.

    how  <- match.arg(how, c("both", "start", "end"))

    ## Don't trim if there's 0 length
    if (n == 0) {
        return(serie)
    }
    if (how %in% c("start", "both")) {
        serie <- tail(serie, -n)
    }
    if (how %in% c("end", "both")) {
        serie <- head(serie, -n)
    }
    return(serie)
}


match_ends <- function(a, b, how) {
    #' Takes two xts and returns them so that both ends match
    #' It cuts off the extra in the larger time series.
    how <- match.arg(how, c("start", "end"))

    if (how == "end") {
        min_end <- min(end(a), end(b))
        a <- a[xts_range(start(a), min_end)]
        b <- b[xts_range(start(b), min_end)]
    } else if (how == "start") {
        max_start <- max(start(a), start(b))
        a <- a[xts_range(max_start, end(a))]
        b <- b[xts_range(max_start, end(b))]
    }

    return(list(a,b))
}

filter_year <- function(serie, year) {
    #' Return a time serie with the only the specified year time stamp
    #' year can be a string or list to
    #' if year is list will return time serie with time stamps which match to
    #' any in the list
    if (is.null(serie)) {
        return(NULL)
    }

    if (length(year) > 1) {
        filt <- serie[format(index(serie), "%Y") %in% year]
    } else {
        filt <- serie[format(index(serie), "%Y") == as.character(year)]
    }

    return(filt)
}


filter_quarter <- function(serie, quarters) {
    if (is.null(serie)) {
        return(NULL)
    }

    filt <- serie[format(as.yearqtr(index(serie)), "%q") %in% quarters]

    return(filt)
}


days_in_quarter <- function(quarter, year) {
    switch(quarter,
           "1" = ifelse(leap_year(year), 91, 90),
           "2" = 91,
           "3" = 92,
           "4" = 92)
}


filter_full_quarter <- function(serie, quarter, year) {
    if (is.null(serie)) {
        return(NULL)
    }

    filt <- filter_quarter(filter_year(serie, year), quarter)
    n_periods <- days_in_quarter(quarter, year)

    if (nrow(filt) == n_periods) {
        return(filt)
    } else {
        return(NULL)
    }
}


days_in_year <- function(x) {
    if (leap_year(as.numeric(x))) {
        n_days <- 366
    } else {
        n_days <- 365
    }
    return(n_days)
}


filter_full_year <- function(serie, time_scale, year = "2016") {
    #' Return a series with only the specified year of data if the year has
    #' full observations of data
    if (is.null(serie)) {
        return(NULL)
    }

    time_scale <- match.arg(time_scale, c("weeks", "days", "months"))

    #' Filter time series for a specific year
    filt <- filter_year(serie, year)
    n_periods <- switch(time_scale,
                        weeks = 52, days = days_in_year(year), months = 12)

    if (nrow(filt) >= n_periods) {
        return(filt)
    } else {
        return(NULL)
    }
}


number_of_days_between_dates <- function(start, end) {
    start <- as.Date(start)
    end <- as.Date(end)

    if (start > end) {
        stop("Start date cannot be larger than end date.")
    }

    count <- 0

    while (start <= end) {
        count <- count + 1
        start <- start + 1
    }

    return(count)
}


## number_of_months_for_days <- function(h, start) {
##     ## Determine the number of months to forecast to achieve the an horizon of h in days
##     ## Excluding the start month
##     ## TODO leap years
##     start <- as.yearmon(date(start))

##     format(start,
##            a <- h
##            cur_month <- month(start)
##            cur_year <- year(start)
##            c <- 0
##            while (a > 0) {
##                c <- c + 1
##                cur_month <- mod(cur_month + 1, 12)
##                days <- as.numeric(days_in_month(cur_month))
##                a  <- a - days
##            }
##            return(c)
##     }


filter_full_xts_observations <- function(serie, start, end) {
    #' Return the time series between the specified start and end dates
    #' but only if all observations are present
    if (is.null(serie)) {
        return(NULL)
    }

    tryCatch({
        filt <- serie[xts_range(start, end)]
    },
    ## If the function doesn't even exist in those ranges
    error = function(cond) {
        return(NULL)
    })

    n_periods <- number_of_days_between_dates(start, end)

    if (nrow(filt) >= n_periods) {
        return(filt)
    } else {
        return(NULL)
    }
}


best_subsequence_x <- function(serie, consec) {
    #' Find longuest continuous subset that does not have more than x consecutive
    #' missing values
    #' Input data is a time series in 2 column df (date and value)

    ## Change NA values with a number we know isn't in the data
    ## Replacing Nas with a unique value is necessary since NA are treated as
    ## unique values in the rle.
    unique_value <- max(serie[[2]], na.rm = TRUE) + 1
    serie[[2]][is.na(serie[[2]])] <- unique_value

    ## Convert the serie to a run lenght encoding (rle).
    ## We will use the properties of rle to more easily find the longuest subsequence.
    ## Also using rle directly avoid us having to use slower looping algorithms.
    ## (slower in R that is).
    final_rle <- rle(serie[[2]])

    ## Find the "NA" values in the rle encoding which are longuer than our consecutive limit
    foo <- which(final_rle$lengths > consec & final_rle$values == unique_value)

    ## Convert our unique_values back to NA
    serie[[2]][serie[[2]] == unique_value] <- NA

    if (length(foo) == 0L) {
        ## we didn't find any gaps longer than our limit, we return the original serie
        return(serie)
    } else {
        ## For each of the points we found we iterate to find the length of that subset
        ## Add beginning and end to the iteration
        split_points <- c(0, foo, length(final_rle$lengths)+1)
        split_res <- vector("list", (length(split_points)-1))
        for (i in seq(1, (length(split_points)-1))) {
            a <- split_points[i]+1
            b <- split_points[i+1]-1
            res <- sum(final_rle$lengths[a:b])
            split_res[[i]] <- list(a=a, b=b, length=res)
        }
        c <- which.max(lapply(split_res, function(x) x$length))

        ## Converting the rle indexes into our original series
        start <- sum(final_rle$lengths[1:split_res[[c]]$a])
        end   <- sum(final_rle$lengths[1:split_res[[c]]$b])
        final_serie <- serie[start:end,]

        return(final_serie)
    }
}


drop_non_rounded_hours <- function(serie) {
    #' Remove non rounded hours
    res <- serie[,datetime:=as.POSIXct(round(datetime, units = "hours"))] %>%
        dplyr::distinct(.data = serie, datetime, .keep_all = TRUE)
    return(res)
}


#' Event matching between time series
match_events_time_series <- function(a, b, time_window=10) {
    #' Match two time series with close events timed together
    #' Input - (a,b): two dataframes containing the time series
    #'         time_window: the window of time to search around for a match
    #' Output - A dataframe containing both initial time series with matches beeing
    #'          next to each other.


    ## Check if the input data is properly formatted data.frame should have two
    ## columns. First column should be convertible to dates or POSIXct.
    if (!all(sapply(a[[1]], is_Date)) & !all(sapply(a[[1]], is_POSIXct))) {
        stop("Incorrect date format in the first column of dataframe a. Expected date or POSIXct")
    } else if (!all(sapply(b[[1]], is_Date)) & !all(sapply(b[[1]], is_POSIXct))) {
        stop("Incorrect date format in the first column of dataframe b. Expected date or POSIXct")
    }


    a %<>% dplyr::arrange(.[[1]])
    b %<>% dplyr::arrange(.[[1]])

    ## To improve the speed as well as reduce the
    flipped <- FALSE
    if (nrow(b) < nrow(a)) {
        new_a <- b
        new_b <- a
        a <- new_a
        b <- new_b
        flipped <- TRUE
    }

    matches <- vector(mode = "logical", length = nrow(b))

    get_a_match <- function(x, time_list, time_window) {
        #' Determines if x (a time stamp) can be matched to any of dates in time_list
        #' If a match is found return the closest match.
        #' Also update our list of found values.

        updated_time_list <- time_list[matches==FALSE]

        ## Don't continue the function if we have completed every possible match
        if (length(updated_time_list) == 0) {
            return(list(as.POSIXct(NA), NA, NA))
        }

        f <- pryr::partial(difftime, time2 = x, units = "hours")

        res <- sapply(updated_time_list, abs %c% as.double %c% f)

        small_ind <- which.min(res)

        if (res[small_ind] < time_window) {
            matches[matches==FALSE][small_ind] <<- TRUE
            date <- updated_time_list[small_ind]
            b_value <- b[[2]][which(b[[1]]==date)]
            return(list(date, b_value, res[small_ind]))
        } else {
            return(list(as.POSIXct(NA), NA, NA))
        }
    }


    f <- pryr::partial(get_a_match, time_list = b[[1]], time_window = time_window)
    res <- lapply(a[[1]], f)

    matching_dates <- do.call(c, lapply(res, function(x) x[[1]]))
    matching_values <- do.call(c, lapply(res, function(x) x[[2]]))
    time_diffs <- do.call(c, lapply(res, function(x) x[[3]]))

    #' Merging both dataframes and lining up the matches
    #' Start by merging the data with the matches.
    #' Then add the missing dates from b
    res_df <- data.frame(a[[1]], matching_dates, a[[2]], matching_values, time_diffs) %>%
        merge(.,
              b[matches==FALSE,],
              by.x = c("matching_dates", "matching_values"),
              by.y = names(b),
              all = TRUE)

    colnames(res_df) <- c(names(b), names(a), "time_diff")

    ## Order the df with both dates column merged together
    merged_time <- res_df[,1]
    merged_time[is.na(merged_time)] <- res_df[,3][is.na(res_df[,1])]
    res_df <- res_df[order(merged_time),]
    rownames(res_df) <- NULL

    if (flipped) {
        res_df <- res_df[,c(3,4,1,2,5)]
    }

    ##class(res_df) <- c("MatchedTimeSeries", class(res_df))

    return(res_df)
}

cor_matched_time_series <- function(df) {
    return(cor(df[[2]], df[[4]], use = "pairwise.complete.obs", method = "pearson"))
}


matching_ratio <- function(df) {
    return(sum(complete.cases(df))/nrow(df))
}


best_start_matching <- function(df) {
    #' Return the best start row index of the matches dataframe.
    #' i.e. return the index of first row that does not have any NA values
    #' starting from the top of dataframe

    na_list <- complete.cases(df)
    for (i in seq_along(na_list)) {
        if (na_list[i]) {
            return(i)
        }
    }
}


best_end_matching <- function(df) {
    #' Return the best end row index of the matches dataframe.
    #' i.e. return the index of the first row that does not have nay NA values
    #' starting from the end of the df.

    na_list <- complete.cases(df)
    for (i in rev(seq_along(na_list))) {
        if (na_list[i]) {
            return(i)
        }
    }
}

#' Temporal aggregation and disaggregation of time series
aggregate_temp <- function(data, binsize, FUN = sum) {
    #' Temporal aggreqation of time series
    #' The series are temporaly agregated starting from the end as such some starting can be loss

    ## Cut down the length so that we capture all of the end
    rem <- length(data) %% binsize
    if (rem != 0) {
        data <- trim_ts(data, rem, "start")
    }

    agg <- zoo::rollapply(data, binsize, FUN, by=binsize)
    agg <- agg[!is.na(agg)]
    return(agg)
}


disaggregate_weighted <-  function(data, weights) {
    #' Implementation of weighted disaggregation of time series

    ## cur_agg_lvl <- unique(diff(index(data)))

    ## if (length(cur_agg_lvl) != 1) {
    ##     stop("Aggregation level is inconsistent accross the serie.")
    ## } else if (cur_agg_lvl = 1) {
    ##     warning("Cannot disagregate timeseries of 1 aggregation. Returning input serie.")
    ##     return(data)
    ## }

    res  <- as.vector(t(data %*% weights))
    return(res)
}


disaggregate_sma <- function(data, binsize) {
    weights <- simpleweights(binsize)
    res <- disaggregate_weighted(data, weights)
    return(res)
}


disaggregate_ema <- function(data, binsize) {
    weights <- emaweights(binsize)
    res <- disaggregate_weighted(data, weights)
    return(res)
}


disaggregate_identity <- function(data, binsize) {
    weights <- rep(1, times = binsize)
    res <- disaggregate_weighted(data, weights)
    return(res)
}


emaweights <- function(m) {
    alpha <- 2/(m+1)
    i <- 1:m
    sm <- sum((alpha*(1-alpha)^(1-i)))
    weights <- (alpha*(1-alpha)^(1-i))/sm
    return(rev(weights))
}


wmaweights <- function(m) {
    weights <- (1:m)/sum(1:m)
    return(rev(weights))
}


simpleweights <- function(m) {
    return(rep(1/m, m))
}


series_agg <- function(series, func = mean) {
    #' Aggregate the series based on the function.
    #' For time series each time period will be have the func applied to each
    #' Also works for multivariate series where each variable will be aggregated seperately
    if (is_multivariate(series)) {
        L <- sapply(series, NCOL)[[1]]
        res <- vector("list", L)

        for (i in seq_len(L)) {
            series_sub <- lapply(series, function(x) x[,i])
            df <- as.data.frame(series_sub, col.names = 1:length(series_sub))
            res[[i]] <- apply(df, 1, func)
        }

        res <- Reduce(cbind, res)
    } else {
        #' Univariate series
        df <- as.data.frame(series, col.names = 1:length(series))
        res <- apply(df, 1, func)
    }

    ## If the series as xts, we reformat the output to be xts
    if (is.xts(series[[1]])) {
        res <- as.xts(res, order.by = index(series[[1]]))
        colnames(res) <- colnames(series[[1]])
    }

    return(res)
}


mean_series <- function(series) {
    return(series_agg(series, func = mean))
}

#' Time series error measurments
#' packages
library(greybox)

#' imports

#' functions
my_MASE <- function(forecast, train, test, period = 1) {
    #' Mean Absolute Scaled Error
    ## forecast - forecasted values
    ## train - data used for forecasting .. used to find scaling factor
    ## test - actual data used for finding MASE.. same length as forecast
    ## period - in case of seasonal data.. if not, use 1

    forecast <- as.vector(forecast)
    train <- as.vector(train)
    test <- as.vector(test)

    n <- length(train)
    scalingFactor <- sum(abs(train[(period+1):n] - train[1:(n-period)])) / (n-period)

    et <- abs(test-forecast)
    qt <- et/scalingFactor
    MASE <- mean(qt)
    return(MASE)
}


my_sME <- function(x, f, scale) {
    #' scaled Mean Error
    res <- mean(x - f, na.rm = TRUE)/scale
    return(res)
}


my_sMAE <- function(x, f, scale) {
    #' scaled Mean Absolute Error
    return(greybox::MAE(x, f)/scale)
}


my_sMSE <- function(x, f, scale) {
    #' scaled Mean Squared Error
    res <- mean(((x - f)/scale)**2)
    return(res)
}


my_PIS <- function(x, f) {
    #' Period In Stock
    return(sum(cumsum(f - x)))
}


my_sPIS <- function(x, f, scale) {
    #' scaled Period In Stock
    return(my_PIS(x,f)/scale)
}


my_sAPIS <- function(x, f, scale) {
    #' scaled Absolute Period In Stock
    return(abs(my_PIS(x,f))/scale)
}


my_MAAPE <- function(x, f) {
    #' Mean Arctangent Absolute Percentage Error
    #' Kim, S., & Kim, H. (2016). A new metric of absolute percentage error for intermittent demand forecasts. International Journal of Forecasting, 32(3), 669-679.
    return(mean(atan(abs((x - f)/x)), na.rm = TRUE))
}


my_mMAPE <- function(x, f) {
    #'  Mean-based error measures for intermittent demand forecasting
    #' Prestwich, S., Rossi, R., Armagan Tarim, S., Hnich, B., 2014. Mean-based error measures for intermittent demand forecasting. International Journal of Production Research 52, 6782-6791.
    x_mean <- mean(x)
    return(mean(abs((x_mean - f)/x_mean), na.rm = TRUE))
}

#' Normalization functions
z_score <- function(ts) {
    if (is.null(ts)) {
        warning("Null value passed returning NULL.")
        return(NULL)
    } else {
        return((ts - mean(ts))/sd(ts))
    }
}


un_z_score <- function(ts, mean, sd) {
    return(ts*sd + mean)
}


scale <- function(ts) {
    #' Scale the data between 0 and 1
    if (is.null(ts)) {
        warning("Null value passed returning NULL.")
        return(NULL)
    } else {
        return((ts-min(ts))/(max(ts)-min(ts)))
    }
}


unscale <- function(ts, max, min) {
    return(ts * (max - min) + min)
}


mean_normalization <- function(ts) {
    return((ts - mean(ts))/max(ts) - min(ts))
}


un_mean_normalization <- function(ts, mean, max, min) {
    return(ts * (max - min) + mean)
}


scale_ab <- function(ts, a, b) {
    if (is.null(ts)) {
        warning("Null value passed returning NULL.")
        return(NULL)
    } else {
        return(a + (ts - min(ts)) * (b - a) / (max(ts) - min(ts)))
    }
}


unscale_ab <- function(ts, a, b, max, min) {
    return((ts - a) * (max - min) / (b - a) + min)
}


normalize_relative <- function(x, y) {
    #' Normalize serie x relative to serie y
    return(x * sum(y)/sum(x))
}


scale_mult <- function(...) {
    #' Scale multiple time series based on the global max and min
    my_max <- max(c(...))
    my_min <- min(c(...))
    return(lapply(list(...), function(x) (x - my_min) / (my_max - my_min)))
}

#' Plots
plot_mult_xts <- function(..., main = NULL, xlab = NULL, ylab = NULL, legend.names = NULL, col = NULL) {
    #' Plots multiple xts series together
    if (length(list(...)) == 1 && is.list(...)) {
        data <- Reduce(merge, ...)
    } else {
        data <- merge(...)
    }

    plot(data, main = main, xlab = xlab, ylab = ylab)
    addLegend(legend.names = legend.names, col = col, lty = 1, bty = "o")
}

#' Smoothing
ksmooth_xts <- function(data, ...) {
    #' Smooth using ksmooth and return an xts
    res <- ksmooth(index(data), data, ...)$y

    res <- xts(res, order.by = index(data))
    attr(res, 'frequency') <- frequency(data)

    return(res)
}


silverman_bandwidth <- function(serie) {
    return(sd(serie)*(4/3/length(serie))^(1/5))
}


ets_smooth_xts <- function(data, ...) {
    #' automatic Exponential Smoothing
    if (nrow(data) < 1) {
        warning("ets requires a minimum of 1 value in the time series. Returning NULL.")
        return(NULL)
    }

    ## Convert data to numeric since the method don't like to take xts input
    smooth <- fitted(forecast::ets(as.numeric(data), ...))

    if (is.xts(data)) {
        res <- xts(smooth, order.by = index(data))
        attr(res, 'frequency') <- frequency(data)
    } else {
        res <- smooth
    }

    return(res)
}
