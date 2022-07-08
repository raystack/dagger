package io.odpf.dagger.core.metrics.reporters.statsd.manager;

import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.core.metrics.aspects.ParquetReaderAspects.READER_CLOSED;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerCounterManagerTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    private SerializedStatsDReporterSupplier statsDReporterSupplier = () -> statsDReporter;

    @Test
    public void shouldIncrementCounterMeasurement() {
        DaggerCounterManager daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);

        daggerCounterManager.increment(READER_CLOSED);

        verify(statsDReporter, times(1)).captureCount(READER_CLOSED.getValue(), 1L, (String[]) null);
    }

    @Test
    public void shouldIncrementCounterMeasurementWithDelta() {
        DaggerCounterManager daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);

        daggerCounterManager.increment(READER_CLOSED, 5L);

        verify(statsDReporter, times(1)).captureCount(READER_CLOSED.getValue(), 5L, (String[]) null);
    }

    @Test
    public void shouldIncrementCounterMeasurementWithRegisteredTags() {
        DaggerCounterManager daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);
        daggerCounterManager.register(new StatsDTag[]{new StatsDTag("tag1", "value1"), new StatsDTag("tag2", "value2")});

        daggerCounterManager.increment(READER_CLOSED);

        verify(statsDReporter, times(1)).captureCount(READER_CLOSED.getValue(), 1L, "tag1=value1", "tag2=value2");
    }

    @Test
    public void shouldDecrementCounterMeasurement() {
        DaggerCounterManager daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);

        daggerCounterManager.decrement(READER_CLOSED);

        verify(statsDReporter, times(1)).captureCount(READER_CLOSED.getValue(), -1L, (String[]) null);
    }

    @Test
    public void shouldDecrementCounterMeasurementWithDelta() {
        DaggerCounterManager daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);

        daggerCounterManager.decrement(READER_CLOSED, -5L);

        verify(statsDReporter, times(1)).captureCount(READER_CLOSED.getValue(), -5L, (String[]) null);
    }

    @Test
    public void shouldDecrementCounterMeasurementWithRegisteredTags() {
        DaggerCounterManager daggerCounterManager = new DaggerCounterManager(statsDReporterSupplier);
        daggerCounterManager.register(new StatsDTag[]{new StatsDTag("tag1", "value1"), new StatsDTag("tag2", "value2")});

        daggerCounterManager.decrement(READER_CLOSED);

        verify(statsDReporter, times(1)).captureCount(READER_CLOSED.getValue(), -1L, "tag1=value1", "tag2=value2");
    }
}