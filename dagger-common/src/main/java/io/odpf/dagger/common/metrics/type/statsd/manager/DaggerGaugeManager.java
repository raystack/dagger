package io.odpf.dagger.common.metrics.type.statsd.manager;

import com.timgroup.statsd.StatsDClient;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.statsd.SerializedStatsDClientSupplier;
import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;
import io.odpf.dagger.common.metrics.type.Gauge;
import io.odpf.dagger.common.metrics.type.MeasurementManager;

import java.util.ArrayList;

public class DaggerGaugeManager implements MeasurementManager, Gauge {
    private final StatsDClient statsDClient;
    private String[] formattedTags;

    public DaggerGaugeManager(SerializedStatsDClientSupplier statsDClientSupplier) {
        this.statsDClient = statsDClientSupplier.getClient();
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
    public void registerLong(Aspects aspect, long gaugeValue) {
        statsDClient.gauge(aspect.getValue(), gaugeValue, formattedTags);
    }

    @Override
    public void registerString(Aspects aspect, String gaugeValue) {
        throw new UnsupportedOperationException("DogStatsD library doesn't support reporting non-numeric values as gauge values.");
    }

    @Override
    public void registerDouble(Aspects aspect, double gaugeValue) {
        statsDClient.gauge(aspect.getValue(), gaugeValue, formattedTags);
    }
}
