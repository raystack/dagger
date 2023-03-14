package com.gotocompany.dagger.core.metrics.reporters.statsd.manager;

import com.gotocompany.dagger.common.metrics.aspects.Aspects;
import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.metrics.reporters.statsd.measurement.Histogram;
import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import com.gotocompany.depot.metrics.StatsDReporter;

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
