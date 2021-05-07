package io.odpf.dagger.core.metrics.telemetry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public interface TelemetryPublisher {
    @SuppressWarnings("checkstyle")
    List<TelemetrySubscriber> TELEMETRY_SUBSCRIBERS = new ArrayList<>();

    default void addSubscriber(TelemetrySubscriber subscriber) {
        TELEMETRY_SUBSCRIBERS.add(subscriber);
    }

    default void notifySubscriber(TelemetrySubscriber subscriber) {
        TELEMETRY_SUBSCRIBERS.add(subscriber);
        notifySubscriber();
    }

    default void notifySubscriber() {
        preProcessBeforeNotifyingSubscriber();
        TELEMETRY_SUBSCRIBERS.forEach(telemetrySubscriber -> telemetrySubscriber.updated(this));
    }

    Map<String, List<String>> getTelemetry();

    default void preProcessBeforeNotifyingSubscriber() {
    }
}
