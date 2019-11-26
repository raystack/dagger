package com.gojek.daggers.metrics;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
