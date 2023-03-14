package com.gotocompany.dagger.core.source.parquet;

import com.google.gson.annotations.SerializedName;

import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_PARQUET_BACKWARD_COMPATIBLE_SCHEMA_MATCH_STRATEGY;
import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_PARQUET_SAME_SCHEMA_MATCH_STRATEGY;

public enum SourceParquetSchemaMatchStrategy {
    @SerializedName(STREAM_SOURCE_PARQUET_SAME_SCHEMA_MATCH_STRATEGY)
    SAME_SCHEMA_WITH_FAIL_ON_MISMATCH,
    @SerializedName(STREAM_SOURCE_PARQUET_BACKWARD_COMPATIBLE_SCHEMA_MATCH_STRATEGY)
    BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH
}
