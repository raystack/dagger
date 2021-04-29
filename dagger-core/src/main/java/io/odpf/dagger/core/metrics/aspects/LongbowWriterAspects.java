package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

public enum LongbowWriterAspects implements Aspects {
    SUCCESS_ON_CREATE_BIGTABLE("successOnCreateBigTable", AspectType.Metric),
    SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME("successOnCreateBigTableResponseTime", AspectType.Histogram),
    FAILURES_ON_CREATE_BIGTABLE("failedOnCreateBigTable", AspectType.Metric),
    FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME("failedOnCreateBigTableResponseTime", AspectType.Histogram),
    TIMEOUTS_ON_WRITER("timeoutsOnWriter", AspectType.Metric),
    CLOSE_CONNECTION_ON_WRITER("closeConnectionOnWriter", AspectType.Metric),
    SUCCESS_ON_WRITE_DOCUMENT("successOnWriteDocument", AspectType.Metric),
    SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME("successOnWriteDocumentResponseTime", AspectType.Histogram),
    FAILED_ON_WRITE_DOCUMENT("failedOnWriteDocument", AspectType.Metric),
    FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME("failedOnWriteDocumentResponseTime", AspectType.Histogram);

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
