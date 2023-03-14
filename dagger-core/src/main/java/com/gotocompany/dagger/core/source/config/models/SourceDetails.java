package com.gotocompany.dagger.core.source.config.models;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;

import java.io.Serializable;

import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_NAME_KEY;
import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_TYPE_KEY;

public class SourceDetails implements Serializable {
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_KEY)
    @Getter
    private SourceName sourceName;

    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_TYPE_KEY)
    @Getter
    private SourceType sourceType;

    public SourceDetails(SourceName sourceName, SourceType sourceType) {
        this.sourceName = sourceName;
        this.sourceType = sourceType;
    }
}
