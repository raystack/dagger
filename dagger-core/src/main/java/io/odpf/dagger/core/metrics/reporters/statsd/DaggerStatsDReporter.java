package io.odpf.dagger.core.metrics.reporters.statsd;

import io.odpf.dagger.core.metrics.reporters.statsd.tags.GlobalTags;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.metrics.StatsDReporterBuilder;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;

import static io.odpf.dagger.core.utils.Constants.FLINK_JOB_ID_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_JOB_ID_KEY;

public class DaggerStatsDReporter implements SerializedStatsDReporterSupplier {
    private static StatsDReporter statsDReporter;
    private final Configuration flinkConfiguration;
    private final io.odpf.dagger.common.configuration.Configuration daggerConfiguration;

    private DaggerStatsDReporter(Configuration flinkConfiguration, io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
        this.flinkConfiguration = flinkConfiguration;
        this.daggerConfiguration = daggerConfiguration;
    }

    private String[] generateGlobalTags(io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
        StatsDTag[] globalTags = new StatsDTag[]{
                new StatsDTag(GlobalTags.JOB_ID, daggerConfiguration.getString(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT))};
        return Arrays.stream(globalTags)
                .map(StatsDTag::getFormattedTag)
                .toArray(String[]::new);
    }

    @Override
    public StatsDReporter getStatsDReporter() {
        if (statsDReporter == null) {
            DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(flinkConfiguration);
            String[] globalTags = generateGlobalTags(daggerConfiguration);
            statsDReporter = StatsDReporterBuilder
                    .builder()
                    .withMetricConfig(daggerMetricsConfig)
                    .withExtraTags(globalTags)
                    .build();
        }
        return statsDReporter;
    }

    public static class Provider {
        public static DaggerStatsDReporter provide(Configuration flinkConfiguration, io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
            return new DaggerStatsDReporter(flinkConfiguration, daggerConfiguration);
        }
    }
}
