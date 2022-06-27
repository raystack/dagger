package io.odpf.dagger.core.metrics.telemetry.statsd;

import io.odpf.dagger.common.metrics.type.statsd.DaggerStatsDClient;
import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import static io.odpf.dagger.core.utils.Constants.FLINK_JOB_ID_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_JOB_ID_KEY;

public class DaggerStatsDClientProvider {

    private static final String FLINK_STATSD_HOST_CONFIG_KEY = "metrics.reporter.stsd.host";
    private static final String DEFAULT_STATSD_HOST_VALUE = "localhost";
    private static final String FLINK_STATSD_PORT_CONFIG_KEY = "metrics.reporter.stsd.port";
    private static final int DEFAULT_STATSD_PORT_VALUE = 8125;

    public static DaggerStatsDClient provide(Configuration flinkConfiguration, io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
        ConfigOption<String> hostConfigOption = ConfigOptions
                .key(FLINK_STATSD_HOST_CONFIG_KEY)
                .stringType()
                .defaultValue(DEFAULT_STATSD_HOST_VALUE);
        ConfigOption<Integer> portConfigOption = ConfigOptions
                .key(FLINK_STATSD_PORT_CONFIG_KEY)
                .intType()
                .defaultValue(DEFAULT_STATSD_PORT_VALUE);

        String hostname = flinkConfiguration.getString(hostConfigOption);
        int port = flinkConfiguration.getInteger(portConfigOption);
        StatsDTag[] globalTags = generateGlobalTags(daggerConfiguration);

        return new DaggerStatsDClient(hostname, port, globalTags);
    }

    public static StatsDTag[] generateGlobalTags(io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
        return new StatsDTag[]{
                new StatsDTag(GlobalTags.JOB_ID, daggerConfiguration.getString(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT))
        };
    }
}
