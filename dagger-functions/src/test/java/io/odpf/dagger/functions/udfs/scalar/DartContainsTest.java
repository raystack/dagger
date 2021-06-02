package io.odpf.dagger.functions.udfs.scalar;


import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.functions.exceptions.TagDoesNotExistException;
import io.odpf.dagger.functions.udfs.scalar.dart.store.gcs.GcsDataStore;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DartContainsTest {
    private GcsDataStore dataStore;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Before
    public void setUp() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "DartContains")).thenReturn(metricGroup);
        when(metricGroup.addGroup("DartContains")).thenReturn(metricGroup);
        this.dataStore = mock(GcsDataStore.class);
        when(dataStore.getMeterStatsManager()).thenReturn(meterStatsManager);
    }

    @Test
    public void shouldReturnTrueWhenFieldContainsTheValue() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(true, dartContains.eval("someList", "someField", 0));
    }

    @Test
    public void shouldReturnTrueWhenFieldContainsTheValueFromDifferentPaths() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));
        when(dataStore.getSet("otherList")).thenReturn(new SetCache(singleton("otherField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(true, dartContains.eval("someList", "someField", 0));
        assertEquals(true, dartContains.eval("otherList", "otherField", 0));
    }

    @Test
    public void shouldReturnFalseWhenFieldDoesNotContainsTheValue() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(false, dartContains.eval("someList", "otherField", 0));
    }

    @Test(expected = TagDoesNotExistException.class)
    public void shouldThrowErrorWhenFieldIsNotExist() {
        when(dataStore.getSet("nonExistingList")).thenThrow(TagDoesNotExistException.class);

        DartContains dartContains = new DartContains(dataStore);

        dartContains.eval("nonExistingList", "someField", 0);
    }

    @Test
    public void shouldNotInvokeDataSourceWhenInvokedAgainWithinRefreshRate() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);
        dartContains.eval("someList", "someField", 1);
        dartContains.eval("someList", "otherField", 1);

        verify(dataStore, times(1)).getSet("someList");
    }

    @Test
    public void shouldInvokeDataSourceWhenExceededRefreshRate() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);
        dartContains.eval("someList", "someField", -1);
        dartContains.eval("someList", "otherField", -1);

        verify(dataStore, times(2)).getSet("someList");
    }

    @Test
    public void shouldReturnTrueWhenFieldContainsTheValueInMiddleWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("prefixsomeField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(true, dartContains.eval("someList", "a sentence with prefixsomeField and an end", ".*%s.*"));
    }

    @Test
    public void shouldReturnFalseWhenTagContainsSpaceAndFieldDoesNotWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("prefixsomeField ")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(false, dartContains.eval("someList", "a sentence with prefixsomeFieldsuffix and an end", ".*%s.*"));
    }

    @Test
    public void shouldReturnTrueWhenFieldContainsTheValueAtEndWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("prefixsomeField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(true, dartContains.eval("someList", "a sentence that ends with prefixsomeField", ".*%s"));
    }

    @Test
    public void shouldReturnTrueWhenFieldContainsTheValueAtBeginningWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("prefixsomeField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(true, dartContains.eval("someList", "prefixsomeField is the start of this sentence", "%s.*"));
    }

    @Test
    public void shouldReturnTrueWhenFieldContainsEntireValueWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("prefixsomeField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(true, dartContains.eval("someList", "prefixsomeField", "%s"));
    }

    @Test
    public void shouldReturnFalseWhenFieldContainsValueNotInSameCaseWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("prefixsomeField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(false, dartContains.eval("someList", "preFixSomEfield", ".*%s.*"));
    }

    @Test
    public void shouldReturnFalseWhenFieldDoesNotContainsTheValueWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);

        assertEquals(false, dartContains.eval("someList", "other", ".*%s.*"));
    }

    @Test(expected = TagDoesNotExistException.class)
    public void shouldThrowErrorWhenFieldIsNotExistWithARegex() {
        when(dataStore.getSet("nonExistingList")).thenThrow(TagDoesNotExistException.class);

        DartContains dartContains = new DartContains(dataStore);

        dartContains.eval("nonExistingList", "someField", ".*%s.*");
    }

    @Test
    public void shouldNotInvokeDataSourceWhenInvokedAgainWithinRefreshRateWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);
        dartContains.eval("someList", "someField", ".*%s.*", 1);
        dartContains.eval("someList", "otherField", ".*%s.*", 1);

        verify(dataStore, times(1)).getSet("someList");
    }

    @Test
    public void shouldInvokeDataSourceWhenExceededRefreshRateWithARegex() {
        when(dataStore.getSet("someList")).thenReturn(new SetCache(singleton("someField")));

        DartContains dartContains = new DartContains(dataStore);
        dartContains.eval("someList", "someField", ".*%s.*", -1);
        dartContains.eval("someList", "otherField", ".*%s.*", -1);

        verify(dataStore, times(2)).getSet("someList");
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        DartContains dartContains = new DartContains(dataStore);
        dartContains.open(functionContext);
        verify(metricGroup, Mockito.times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
