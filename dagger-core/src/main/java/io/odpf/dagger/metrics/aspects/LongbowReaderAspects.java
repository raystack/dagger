package io.odpf.dagger.metrics.aspects;

public enum LongbowReaderAspects implements Aspects {
    TIMEOUTS_ON_READER("timeoutsOnReader", AspectType.Metric),
    CLOSE_CONNECTION_ON_READER("closeConnectionOnReader", AspectType.Metric),
    SUCCESS_ON_READ_DOCUMENT("successOnReadDocument", AspectType.Metric),
    SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME("successOnReadDocumentResponseTime", AspectType.Histogram),
    DOCUMENTS_READ_PER_SCAN("documentsReadPerScan", AspectType.Histogram),
    FAILED_ON_READ_DOCUMENT("failedOnReadDocument", AspectType.Metric),
    FAILED_ON_READ_DOCUMENT_RESPONSE_TIME("failedOnReadDocumentResponseTime", AspectType.Histogram),
    FAILED_TO_READ_LAST_RECORD("failedToReadLastRecord", AspectType.Metric);

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
