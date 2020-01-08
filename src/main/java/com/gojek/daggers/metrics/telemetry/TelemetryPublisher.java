package com.gojek.daggers.metrics.telemetry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface TelemetryPublisher {
    List<TelemetrySubscriber> subscribers = new ArrayList<>();

    default void addSubscriber(TelemetrySubscriber subscriber) {
        subscribers.add(subscriber);
    }

    default void notifySubscriber(TelemetrySubscriber subscriber) {
        subscribers.add(subscriber);
        notifySubscriber();
    }

    default void notifySubscriber() {
        preProcessBeforeNotifyingSubscriber();
        subscribers.forEach(telemetrySubscriber -> telemetrySubscriber.updated(this));
    }

    Map<String, List<String>> getTelemetry();

    default void preProcessBeforeNotifyingSubscriber() {
    }
}
