package com.gotocompany.dagger.core.source.parquet;

import com.google.gson.annotations.SerializedName;

import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_PARQUET_READ_ORDER_STRATEGY_EARLIEST_INDEX_FIRST;
import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_PARQUET_READ_ORDER_STRATEGY_EARLIEST_TIME_URL_FIRST;

public enum SourceParquetReadOrderStrategy {
    @SerializedName(STREAM_SOURCE_PARQUET_READ_ORDER_STRATEGY_EARLIEST_TIME_URL_FIRST)
    EARLIEST_TIME_URL_FIRST,
    @SerializedName(STREAM_SOURCE_PARQUET_READ_ORDER_STRATEGY_EARLIEST_INDEX_FIRST)
    EARLIEST_INDEX_FIRST
}
