package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

/**
 * The enum Longbow writer aspects.
 */
public enum LongbowWriterAspects implements Aspects {
    SUCCESS_ON_CREATE_BIGTABLE("success_on_create_bigtable", AspectType.Metric),
    SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME("success_on_create_bigtable_response_time", AspectType.Histogram),
    FAILURES_ON_CREATE_BIGTABLE("failed_on_create_bigtable", AspectType.Metric),
    FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME("failed_on_create_bigtable_response_time", AspectType.Histogram),
    TIMEOUTS_ON_WRITER("timeouts_on_writer", AspectType.Metric),
    CLOSE_CONNECTION_ON_WRITER("close_connection_on_writer", AspectType.Metric),
    SUCCESS_ON_WRITE_DOCUMENT("success_on_write_document", AspectType.Metric),
    SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME("success_on_write_document_response_time", AspectType.Histogram),
    FAILED_ON_WRITE_DOCUMENT("failed_on_write_document", AspectType.Metric),
    FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME("failed_on_write_document_response_time", AspectType.Histogram);

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
