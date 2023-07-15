package org.raystack.dagger.core.metrics.reporters.statsd.manager;

import org.raystack.dagger.core.metrics.aspects.ParquetReaderAspects;
import org.raystack.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import org.raystack.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import org.raystack.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerHistogramManagerTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    private final SerializedStatsDReporterSupplier statsDReporterSupplier = () -> statsDReporter;

    @Test
    public void shouldRecordHistogramValue() {
        DaggerHistogramManager daggerHistogramManager = new DaggerHistogramManager(statsDReporterSupplier);

        daggerHistogramManager.recordValue(ParquetReaderAspects.READER_ROW_READ_TIME, 5L);

        verify(statsDReporter, times(1)).captureHistogram(ParquetReaderAspects.READER_ROW_READ_TIME.getValue(), 5L, (String[]) null);
    }

    @Test
    public void shouldRecordHistogramValueWithRegisteredTags() {
        DaggerHistogramManager daggerHistogramManager = new DaggerHistogramManager(statsDReporterSupplier);
        daggerHistogramManager.register(new StatsDTag[]{new StatsDTag("tag1", "value1"), new StatsDTag("tag2", "value2")});

        daggerHistogramManager.recordValue(ParquetReaderAspects.READER_ROW_READ_TIME, 6L);

        verify(statsDReporter, times(1)).captureHistogram(ParquetReaderAspects.READER_ROW_READ_TIME.getValue(), 6L, "tag1=value1", "tag2=value2");
    }
}
