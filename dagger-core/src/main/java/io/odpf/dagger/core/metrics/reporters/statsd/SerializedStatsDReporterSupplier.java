package io.odpf.dagger.core.metrics.reporters.statsd;

import io.odpf.depot.metrics.StatsDReporter;

import java.io.Serializable;

@FunctionalInterface
public interface SerializedStatsDReporterSupplier extends Serializable {
    StatsDReporter buildStatsDReporter();
}
