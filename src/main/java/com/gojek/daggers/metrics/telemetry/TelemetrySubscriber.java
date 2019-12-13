package com.gojek.daggers.metrics.telemetry;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
