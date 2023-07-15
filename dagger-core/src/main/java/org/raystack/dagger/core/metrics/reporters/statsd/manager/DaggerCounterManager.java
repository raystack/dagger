package org.raystack.dagger.core.metrics.reporters.statsd.manager;

import org.raystack.dagger.common.metrics.aspects.Aspects;
import org.raystack.dagger.core.metrics.reporters.statsd.measurement.Counter;
import org.raystack.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import org.raystack.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import org.raystack.depot.metrics.StatsDReporter;

import java.util.ArrayList;

public class DaggerCounterManager implements MeasurementManager, Counter {
    private final StatsDReporter statsDReporter;
    private String[] formattedTags;

    public DaggerCounterManager(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.statsDReporter = statsDReporterSupplier.buildStatsDReporter();
    }

    @Override
    public void register(StatsDTag[] tags) {
        ArrayList<String> tagList = new ArrayList<>();
        for (StatsDTag measurementTag : tags) {
            tagList.add(measurementTag.getFormattedTag());
        }
        this.formattedTags = tagList.toArray(new String[0]);
    }

    @Override
    public void increment(Aspects aspect) {
        increment(aspect, 1L);
    }

    @Override
    public void increment(Aspects aspect, long positiveCount) {
        statsDReporter.captureCount(aspect.getValue(), positiveCount, formattedTags);
    }

    @Override
    public void decrement(Aspects aspect) {
        decrement(aspect, -1L);
    }

    @Override
    public void decrement(Aspects aspect, long negativeCount) {
        statsDReporter.captureCount(aspect.getValue(), negativeCount, formattedTags);
    }
}
