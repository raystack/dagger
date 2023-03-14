package com.gotocompany.dagger.core.metrics.reporters.statsd;

import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.GlobalTags;
import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.metrics.StatsDReporterBuilder;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Arrays;

import static com.gotocompany.dagger.core.utils.Constants.FLINK_JOB_ID_DEFAULT;
import static com.gotocompany.dagger.core.utils.Constants.FLINK_JOB_ID_KEY;

public class DaggerStatsDReporter implements SerializedStatsDReporterSupplier {
    private static StatsDReporter statsDReporter;
    private final Configuration flinkConfiguration;
    private final com.gotocompany.dagger.common.configuration.Configuration daggerConfiguration;

    private DaggerStatsDReporter(Configuration flinkConfiguration, com.gotocompany.dagger.common.configuration.Configuration daggerConfiguration) {
        this.flinkConfiguration = flinkConfiguration;
        this.daggerConfiguration = daggerConfiguration;
    }

    private String[] generateGlobalTags() {
        StatsDTag[] globalTags = new StatsDTag[]{
                new StatsDTag(GlobalTags.JOB_ID, daggerConfiguration.getString(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT))};
        return Arrays.stream(globalTags)
                .map(StatsDTag::getFormattedTag)
                .toArray(String[]::new);
    }

    @Override
    public StatsDReporter buildStatsDReporter() {
        if (statsDReporter == null) {
            DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(flinkConfiguration);
            String[] globalTags = generateGlobalTags();
            statsDReporter = StatsDReporterBuilder
                    .builder()
                    .withMetricConfig(daggerMetricsConfig)
                    .withExtraTags(globalTags)
                    .build();
        }
        return statsDReporter;
    }

    protected static void close() throws IOException {
        if (statsDReporter != null) {
            statsDReporter.close();
            statsDReporter = null;
        }
    }

    public static class Provider {
        public static DaggerStatsDReporter provide(Configuration flinkConfiguration, com.gotocompany.dagger.common.configuration.Configuration daggerConfiguration) {
            return new DaggerStatsDReporter(flinkConfiguration, daggerConfiguration);
        }
    }
}
