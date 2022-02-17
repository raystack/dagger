package io.odpf.dagger.core.source;

import com.google.gson.annotations.SerializedName;

import static io.odpf.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_TYPE_BOUNDED;
import static io.odpf.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_TYPE_UNBOUNDED;

public enum SourceType {
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_TYPE_BOUNDED)
    BOUNDED,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_TYPE_UNBOUNDED)
    UNBOUNDED
}