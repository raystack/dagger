package com.gojek.daggers.metrics;

public enum TelemetryTypes {
    INPUT_TOPIC("input_topic"),
    INPUT_PROTO("input_proto"),
    INPUT_STREAM("input_stream"),
    SINK_TYPE("sink_type"),
    OUTPUT_TOPIC("output_topic"),
    OUTPUT_PROTO("output_proto"),
    OUTPUT_STREAM("output_stream"),
    POST_PROCESSOR_TYPE("post_processor_type");

    public String getValue() {
        return value;
    }

    private String value;

    TelemetryTypes(String value) {
        this.value = value;
    }
}
