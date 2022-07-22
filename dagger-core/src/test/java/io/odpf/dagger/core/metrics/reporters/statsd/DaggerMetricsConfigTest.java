package io.odpf.dagger.core.metrics.reporters.statsd;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class DaggerMetricsConfigTest {
    @Mock
    private Configuration flinkConfiguration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldSetHostNameAsDefinedInFlinkConfiguration() {
        ConfigOption<String> hostConfigOption = ConfigOptions
                .key("metrics.reporter.stsd.host")
                .stringType()
                .defaultValue("localhost");
        when(flinkConfiguration.getString(hostConfigOption)).thenReturn("my-host");

        DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(flinkConfiguration);
        assertEquals("my-host", daggerMetricsConfig.getMetricStatsDHost());
    }

    @Test
    public void shouldUseDefaultHostNameWhenNotDefinedInFlinkConfiguration() {
        DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(new Configuration());
        assertEquals("localhost", daggerMetricsConfig.getMetricStatsDHost());
    }

    @Test
    public void shouldSetPortNumberAsDefinedInFlinkConfiguration() {
        ConfigOption<Integer> portConfigOption = ConfigOptions
                .key("metrics.reporter.stsd.port")
                .intType()
                .defaultValue(8125);
        when(flinkConfiguration.getInteger(portConfigOption)).thenReturn(9010);

        DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(flinkConfiguration);
        assertEquals(9010, daggerMetricsConfig.getMetricStatsDPort().intValue());
    }

    @Test
    public void shouldUseDefaultPortWhenNotDefinedInFlinkConfiguration() {
        DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(new Configuration());
        assertEquals(8125, daggerMetricsConfig.getMetricStatsDPort().intValue());
    }

    @Test
    public void shouldReturnEmptyStringAsTags() {
        DaggerMetricsConfig daggerMetricsConfig = new DaggerMetricsConfig(new Configuration());
        assertEquals("", daggerMetricsConfig.getMetricStatsDTags());
    }
}
