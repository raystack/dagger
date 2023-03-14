package com.gotocompany.dagger.core.metrics.reporters.statsd.manager;

import com.gotocompany.dagger.core.metrics.aspects.ChronologyOrderedSplitAssignerAspects;
import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

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

    private final SerializedStatsDReporterSupplier statsDReporterSupplier = () -> statsDReporter;

    @Test
    public void shouldMarkGaugeValue() {
        DaggerGaugeManager daggerGaugeManager = new DaggerGaugeManager(statsDReporterSupplier);

        daggerGaugeManager.markValue(ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED, 1000);

        verify(statsDReporter, times(1)).gauge(ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED.getValue(), 1000, (String[]) null);
    }

    @Test
    public void shouldMarkGaugeValueWithRegisteredTags() {
        DaggerGaugeManager daggerGaugeManager = new DaggerGaugeManager(statsDReporterSupplier);
        daggerGaugeManager.register(new StatsDTag[]{new StatsDTag("tag1", "value1"), new StatsDTag("tag2", "value2")});

        daggerGaugeManager.markValue(ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED, 1000);

        verify(statsDReporter, times(1)).gauge(ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED.getValue(), 1000, "tag1=value1", "tag2=value2");
    }
}
