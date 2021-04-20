package io.odpf.dagger.common.contracts;

import io.odpf.dagger.common.contracts.TelemetryPublisher;

public interface TelemetrySubscriber {
    void updated(TelemetryPublisher publisher);
}
