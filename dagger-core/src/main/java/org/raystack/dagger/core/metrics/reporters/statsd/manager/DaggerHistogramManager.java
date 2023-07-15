package org.raystack.dagger.core.metrics.reporters.statsd.manager;

import org.raystack.dagger.common.metrics.aspects.Aspects;
import org.raystack.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import org.raystack.dagger.core.metrics.reporters.statsd.measurement.Histogram;
import org.raystack.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import org.raystack.depot.metrics.StatsDReporter;

import java.util.ArrayList;

public class DaggerHistogramManager implements MeasurementManager, Histogram {
    private final StatsDReporter statsDReporter;
    private String[] formattedTags;

    public DaggerHistogramManager(SerializedStatsDReporterSupplier statsDReporterSupplier) {
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
    public void recordValue(Aspects aspect, long value) {
        statsDReporter.captureHistogram(aspect.getValue(), value, formattedTags);
    }
}
