package com.gojek.daggers.metrics;

public enum TelemetryTypes {
    INPUT_TOPIC("input_topic"),
    INPUT_PROTO("input_proto"),
    INPUT_STREAM("input_stream");

    public String getValue() {
        return value;
    }

    private String value;

    TelemetryTypes(String value) {
        this.value = value;
    }
}
