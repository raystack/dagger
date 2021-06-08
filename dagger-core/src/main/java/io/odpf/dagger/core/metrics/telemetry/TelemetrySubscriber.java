package io.odpf.dagger.core.metrics.telemetry;

/**
 * The interface Telemetry subscriber.
 */
public interface TelemetrySubscriber {
    /**
     * Updated telemetry publisher.
     *
     * @param publisher the publisher
     */
    void updated(TelemetryPublisher publisher);
}
