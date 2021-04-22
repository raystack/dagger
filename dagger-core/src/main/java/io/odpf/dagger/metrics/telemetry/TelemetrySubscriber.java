package io.odpf.dagger.metrics.telemetry;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
