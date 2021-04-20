package io.odpf.dagger.common.contracts;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
