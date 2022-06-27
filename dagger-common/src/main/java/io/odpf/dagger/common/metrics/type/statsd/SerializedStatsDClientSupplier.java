package io.odpf.dagger.common.metrics.type.statsd;

import com.timgroup.statsd.StatsDClient;

import java.io.Serializable;

@FunctionalInterface
public interface SerializedStatsDClientSupplier extends Serializable {
    StatsDClient getClient();
}
