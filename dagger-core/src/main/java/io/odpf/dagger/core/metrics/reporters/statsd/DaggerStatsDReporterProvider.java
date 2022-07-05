package io.odpf.dagger.core.metrics.reporters.statsd;

import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.GlobalTags;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.metrics.StatsDReporterBuilder;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static io.odpf.dagger.core.utils.Constants.FLINK_JOB_ID_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_JOB_ID_KEY;

public class DaggerStatsDReporterProvider {
    private static StatsDReporter statsDReporter;
    private static final Logger LOGGER = LoggerFactory.getLogger(DaggerStatsDReporterProvider.class.getName());

    public static SerializedStatsDReporterSupplier provide(Configuration flinkConfiguration, io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
        if (statsDReporter == null) {
            DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(flinkConfiguration);
            String[] globalTags = generateGlobalTags(daggerConfiguration);
            statsDReporter = StatsDReporterBuilder
                    .builder()
                    .withMetricConfig(daggerMetricsConfig)
                    .withExtraTags(globalTags)
                    .build();
            LOGGER.info("Instantiated new StatsDReporter");
        }
        return () -> statsDReporter;
    }

    private static String[] generateGlobalTags(io.odpf.dagger.common.configuration.Configuration daggerConfiguration) {
        StatsDTag[] globalTags = new StatsDTag[]{
                new StatsDTag(GlobalTags.JOB_ID, daggerConfiguration.getString(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT))};
        return Arrays.stream(globalTags)
                .map(StatsDTag::getFormattedTag)
                .toArray(String[]::new);
    }
}
