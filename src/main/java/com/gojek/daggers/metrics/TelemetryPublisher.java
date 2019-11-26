package com.gojek.daggers.metrics;

import java.util.List;
import java.util.Map;

public interface TelemetryPublisher {
    void addSubscriber(TelemetrySubscriber subscriber);

    void notifySubscriber();

    Map<String, List<String>> getTelemetry();
}
