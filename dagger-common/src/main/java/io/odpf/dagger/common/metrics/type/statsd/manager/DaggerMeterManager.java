package io.odpf.dagger.common.metrics.type.statsd.manager;

import com.timgroup.statsd.StatsDClient;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.statsd.SerializedStatsDClientSupplier;
import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;
import io.odpf.dagger.common.metrics.type.MeasurementManager;
import io.odpf.dagger.common.metrics.type.Meter;

import java.util.ArrayList;
import java.util.function.Supplier;

public class DaggerMeterManager implements MeasurementManager, Meter {
    private final StatsDClient statsDClient;
    private String[] formattedTags;

    public DaggerMeterManager(SerializedStatsDClientSupplier statsDClientSupplier) {
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
    public void markEvent(Aspects aspect) {
        statsDClient.increment(aspect.getValue(), formattedTags);
    }

    @Override
    public void markEvent(Aspects aspect, long num) {
        statsDClient.count(aspect.getValue(), num, formattedTags);
    }
}
