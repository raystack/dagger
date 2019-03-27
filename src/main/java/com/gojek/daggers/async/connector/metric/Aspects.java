package com.gojek.daggers.async.connector.metric;

public enum Aspects {
    SUCCESS_RESPONSE("successCount"),
    EXCEPTION("failedCount"),
    SUCCESS_RESPONSE_TIME("successResponseTime"),
    FAILED_RESPONSE_TIME("failedResponseTime"),
    FOUR_XX_RESPONSE("4XXFailedCount"),
    FIVE_XX_RESPONSE("5XXFailedCount"),
    CALL_COUNT("callCount");

    private String value;

    Aspects(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
