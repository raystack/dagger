package com.gotocompany.dagger.core.metrics.reporters.statsd;

import com.gotocompany.depot.metrics.StatsDReporter;

import java.io.Serializable;

/* Flink requires that all objects which are needed to prepare the Job Graph should be serializable along with their
properties/fields. StatsDReporter and its fields are not serializable. Hence, in order to mitigate job graph creation
failure, we create a serializable interface around the reporter as below. This is a common idiom to make un-serializable
fields serializable in Java 8: https://stackoverflow.com/a/22808112 */

@FunctionalInterface
public interface SerializedStatsDReporterSupplier extends Serializable {
    StatsDReporter buildStatsDReporter();
}
