package io.odpf.dagger.core.metrics.reporters.statsd.manager;

import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.measurement.Gauge;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;

import java.util.ArrayList;

public class DaggerGaugeManager implements MeasurementManager, Gauge {
    private final StatsDReporter statsDReporter;
    private String[] formattedTags;

    public DaggerGaugeManager(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.statsDReporter = statsDReporterSupplier.getStatsDReporter();
    }

    @Override
    public void register(Aspects[] aspect, StatsDTag[] tags) {
        register(tags);
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
    public void registerInteger(Aspects aspect, int gaugeValue) {
        statsDReporter.gauge(aspect.getValue(), gaugeValue, formattedTags);
    }
}
