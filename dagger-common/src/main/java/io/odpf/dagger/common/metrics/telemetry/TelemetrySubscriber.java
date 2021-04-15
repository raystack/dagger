package io.odpf.dagger.common.metrics.telemetry;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
