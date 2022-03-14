package io.odpf.dagger.core.source;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static io.odpf.dagger.core.source.SourceName.*;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SourceName.class)
public class StreamMetricsTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private SourceName invalidSourceName;

    @Before
    public void setup() {
        initMocks(this);
        invalidSourceName = mock(SourceName.class);
        given(invalidSourceName.name()).willReturn("INVALID_SOURCE_NAME");
        given(invalidSourceName.ordinal()).willReturn(2);
        PowerMockito.mockStatic(SourceName.class);
        PowerMockito.when(SourceName.values()).thenReturn(new SourceName[]{KAFKA, PARQUET, invalidSourceName});
    }

    @Test
    public void shouldAddDefaultMetricsForKafkaSource() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getKafkaTopicNames()).thenReturn("order-log|shipping-log|payment-log");
        when(streamConfig.getKafkaName()).thenReturn("kafka_table");

        Map<String, List<String>> metrics = new HashMap<>();

        StreamMetrics.addDefaultMetrics(streamConfig, metrics);

        assertEquals(2, metrics.size());
        assertEquals(Arrays.asList("order-log", "shipping-log", "payment-log"), metrics.get("input_topic"));
        assertEquals(Collections.singletonList("kafka_table"), metrics.get("input_stream"));
    }

    @Test
    public void shouldAddNoMetricsForParquetSource() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, BOUNDED)});

        Map<String, List<String>> metrics = new HashMap<>();

        StreamMetrics.addDefaultMetrics(streamConfig, metrics);

        assertEquals(0, metrics.size());
    }

    @Test
    public void shouldThrowExceptionIfSourceNotFound() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(invalidSourceName, BOUNDED)});

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> StreamMetrics.addDefaultMetrics(streamConfig, new HashMap<>()));

        assertEquals("Error: Cannot add default metrics for unknown source INVALID_SOURCE_NAME", exception.getMessage());
    }
}