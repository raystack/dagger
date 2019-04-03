package com.gojek.daggers.async.connector.metric;

public enum Aspects {
    SUCCESSES("successes"),
    FAILURES("failures"),
    SUCCESS_RESPONSE_TIME("successResponseTime"),
    FAILED_RESPONSE_TIME("failedResponseTime"),
    FOUR_XX_FAILURES("4XXFailures"),
    FIVE_XX_FAILURES("5XXFailures"),
    TOTAL_CALLS("totalCalls");

    private String value;

    Aspects(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
