package com.gojek.daggers.metrics;

import static com.gojek.daggers.metrics.AspectType.Histogram;
import static com.gojek.daggers.metrics.AspectType.Metric;

public enum LongbowWriterAspects implements Aspects {
    SUCCESS_ON_CREATE_BIGTABLE("successOnCreateBigTable", Metric),
    SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME("successOnCreateBigTableResponseTime", Histogram),
    FAILURES_ON_CREATE_BIGTABLE("failedOnCreateBigTable", Metric),
    FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME("failedOnCreateBigTableResponseTime", Histogram),
    TIMEOUTS_ON_WRITER("timeoutsOnWriter", Metric),
    CLOSE_CONNECTION_ON_WRITER("closeConnectionOnWriter", Metric),
    SUCCESS_ON_WRITE_DOCUMENT("successOnWriteDocument", Metric),
    SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME("successOnWriteDocumentResponseTime", Histogram),
    FAILED_ON_WRITE_DOCUMENT("failedOnWriteDocument", Metric),
    FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME("failedOnWriteDocumentResponseTime", Histogram);

    private String value;
    private AspectType aspectType;

    LongbowWriterAspects(String value, AspectType aspectType) {
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
