package io.odpf.dagger.core.metrics.telemetry;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
