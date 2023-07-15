package org.raystack.dagger.core.metrics.reporters.statsd;

import org.raystack.depot.config.MetricsConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public class DaggerMetricsConfig implements MetricsConfig {
    private static final String FLINK_STATSD_HOST_CONFIG_KEY = "metrics.reporter.stsd.host";
    private static final String DEFAULT_STATSD_HOST_VALUE = "localhost";
    private static final String FLINK_STATSD_PORT_CONFIG_KEY = "metrics.reporter.stsd.port";
    private static final int DEFAULT_STATSD_PORT_VALUE = 8125;
    private final String hostName;
    private final int port;

    public DaggerMetricsConfig(Configuration flinkConfiguration) {
        ConfigOption<String> hostConfigOption = ConfigOptions
                .key(FLINK_STATSD_HOST_CONFIG_KEY)
                .stringType()
                .defaultValue(DEFAULT_STATSD_HOST_VALUE);
        ConfigOption<Integer> portConfigOption = ConfigOptions
                .key(FLINK_STATSD_PORT_CONFIG_KEY)
                .intType()
                .defaultValue(DEFAULT_STATSD_PORT_VALUE);
        this.hostName = flinkConfiguration.getString(hostConfigOption);
        this.port = flinkConfiguration.getInteger(portConfigOption);
    }

    @Override
    public String getMetricStatsDHost() {
        return hostName;
    }

    @Override
    public Integer getMetricStatsDPort() {
        return port;
    }

    @Override
    public String getMetricStatsDTags() {
        return "";
    }
}
