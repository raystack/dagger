package io.odpf.dagger.core.metrics.reporters.statsd.manager;

import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_ROW_READ_TIME;
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

        daggerHistogramManager.recordValue(READER_ROW_READ_TIME, 5L);

        verify(statsDReporter, times(1)).captureHistogram(READER_ROW_READ_TIME.getValue(), 5L, (String[]) null);
    }

    @Test
    public void shouldRecordHistogramValueWithRegisteredTags() {
        DaggerHistogramManager daggerHistogramManager = new DaggerHistogramManager(statsDReporterSupplier);
        daggerHistogramManager.register(new StatsDTag[]{new StatsDTag("tag1", "value1"), new StatsDTag("tag2", "value2")});

        daggerHistogramManager.recordValue(READER_ROW_READ_TIME, 6L);

        verify(statsDReporter, times(1)).captureHistogram(READER_ROW_READ_TIME.getValue(), 6L, "tag1=value1", "tag2=value2");
    }
}
