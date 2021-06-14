package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

/**
 * The enum Longbow reader aspects.
 */
public enum LongbowReaderAspects implements Aspects {
    TIMEOUTS_ON_READER("timeouts_on_reader", AspectType.Metric),
    CLOSE_CONNECTION_ON_READER("close_connection_on_reader", AspectType.Metric),
    SUCCESS_ON_READ_DOCUMENT("success_on_read_document", AspectType.Metric),
    SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME("success_on_read_document_response_time", AspectType.Histogram),
    DOCUMENTS_READ_PER_SCAN("documents_read_per_scan", AspectType.Histogram),
    FAILED_ON_READ_DOCUMENT("failed_on_read_document", AspectType.Metric),
    FAILED_ON_READ_DOCUMENT_RESPONSE_TIME("failed_on_read_document_response_time", AspectType.Histogram),
    FAILED_TO_READ_LAST_RECORD("failed_to_read_last_record", AspectType.Metric);

    private String value;
    private AspectType aspectType;

    LongbowReaderAspects(String value, AspectType aspectType) {
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
