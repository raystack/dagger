package io.odpf.dagger.core.metrics.reporters.statsd.manager;

import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.core.metrics.aspects.ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerGaugeManagerTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    private SerializedStatsDReporterSupplier statsDReporterSupplier = () -> statsDReporter;

    @Test
    public void shouldMarkGaugeValue() {
        DaggerGaugeManager daggerGaugeManager = new DaggerGaugeManager(statsDReporterSupplier);

        daggerGaugeManager.markValue(TOTAL_SPLITS_DISCOVERED, 1000);

        verify(statsDReporter, times(1)).gauge(TOTAL_SPLITS_DISCOVERED.getValue(), 1000, (String[]) null);
    }

    @Test
    public void shouldMarkGaugeValueWithRegisteredTags() {
        DaggerGaugeManager daggerGaugeManager = new DaggerGaugeManager(statsDReporterSupplier);
        daggerGaugeManager.register(new StatsDTag[]{new StatsDTag("tag1", "value1"), new StatsDTag("tag2", "value2")});

        daggerGaugeManager.markValue(TOTAL_SPLITS_DISCOVERED, 1000);

        verify(statsDReporter, times(1)).gauge(TOTAL_SPLITS_DISCOVERED.getValue(), 1000, "tag1=value1", "tag2=value2");
    }
}
