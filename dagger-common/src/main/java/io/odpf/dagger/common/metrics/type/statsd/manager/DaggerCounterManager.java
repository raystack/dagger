package io.odpf.dagger.common.metrics.type.statsd.manager;

import com.timgroup.statsd.StatsDClient;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.statsd.SerializedStatsDClientSupplier;
import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;
import io.odpf.dagger.common.metrics.type.Counter;
import io.odpf.dagger.common.metrics.type.MeasurementManager;

import java.util.ArrayList;

public class DaggerCounterManager implements MeasurementManager, Counter {
    private final StatsDClient statsDClient;
    private String[] formattedTags;

    public DaggerCounterManager(SerializedStatsDClientSupplier statsDClientSupplier) {
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
    public void increment(Aspects aspect) {
        statsDClient.increment(aspect.getValue(), formattedTags);
    }

    @Override
    public void decrement(Aspects aspect) {
        statsDClient.decrement(aspect.getValue(), formattedTags);
    }
}
