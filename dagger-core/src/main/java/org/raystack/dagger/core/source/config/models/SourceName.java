package org.raystack.dagger.core.source.config.models;

import com.google.gson.annotations.SerializedName;

import static org.raystack.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA_CONSUMER;
import static org.raystack.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA;
import static org.raystack.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_NAME_PARQUET;

public enum SourceName {
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA)
    KAFKA_SOURCE,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_PARQUET)
    PARQUET_SOURCE,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA_CONSUMER)
    KAFKA_CONSUMER
}
