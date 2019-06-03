package com.gojek.daggers.longbow.metric;

import com.gojek.daggers.utils.stats.AspectType;
import com.gojek.daggers.utils.stats.Aspects;

import static com.gojek.daggers.utils.stats.AspectType.Histogram;
import static com.gojek.daggers.utils.stats.AspectType.Metric;

public enum LongbowAspects implements Aspects {
    SUCCESS_ON_CREATE_BIGTABLE("successOnCreateBigTable", Metric),
    SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME("successOnCreateBigTableResponseTime", Histogram),
    FAILURES_ON_CREATE_BIGTABLE("failedOnCreateBigTable", Metric),
    FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME("failedOnCreateBigTableResponseTime", Histogram),
    TIMEOUTS_ON_WRITER("timeoutsOnWriter", Metric),
    CLOSE_CONNECTION_ON_WRITER("closeConnectionOnWriter", Metric),
    SUCCESS_ON_WRITE_DOCUMENT("successOnWriteDocument", Metric),
    SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME("successOnWriteDocumentResponseTime", Histogram),
    FAILURES_ON_WRITE_DOCUMENT("failedOnWriteDocument", Metric),
    FAILURES_ON_WRITE_DOCUMENT_RESPONSE_TIME("failedOnWriteDocumentResponseTime", Histogram),
    TIMEOUTS_ON_READER("timeoutsOnReader", Metric),
    CLOSE_CONNECTION_ON_READER("closeConnectionOnReader", Metric),
    SUCCESS_ON_READ_DOCUMENT("successOnReadDocument", Metric),
    SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME("successOnReadDocumentResponseTime", Histogram),
    FAILURES_ON_READ_DOCUMENT("failedOnReadDocument", Metric),
    FAILURES_ON_READ_DOCUMENT_RESPONSE_TIME("failedOnReadDocumentResponseTime", Histogram);

    private String value;
    private AspectType aspectType;

    LongbowAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
