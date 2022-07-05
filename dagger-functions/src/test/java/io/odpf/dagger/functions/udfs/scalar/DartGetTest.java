package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.functions.exceptions.KeyDoesNotExistException;
import io.odpf.dagger.functions.exceptions.TagDoesNotExistException;
import io.odpf.dagger.functions.udfs.scalar.dart.store.gcs.GcsDataStore;
import io.odpf.dagger.functions.udfs.scalar.dart.types.MapCache;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class DartGetTest {
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
        when(metricGroup.addGroup("udf", "DartGet")).thenReturn(metricGroup);
        when(metricGroup.addGroup("DartGet")).thenReturn(metricGroup);
        this.dataStore = mock(GcsDataStore.class);
        when(dataStore.getMeterStatsManager()).thenReturn(meterStatsManager);
    }

    @Test
    public void shouldReturnValueWhenMapAndKeyExist() {
        String key = "some-key";
        String value = "expected-value";
        when(dataStore.getMap("someMap")).thenReturn(new MapCache(singletonMap(key, value)));

        DartGet dartGet = new DartGet(dataStore);

        assertEquals(value, dartGet.eval("someMap", "some-key", 1));
    }

    @Test
    public void shouldReturnDifferentValueWhenMapAndKeyExistForAllOfThem() {
        String key = "some-key";
        String key2 = "other-key";
        String value = "expected-value";
        String value2 = "other-expected-value";
        when(dataStore.getMap("someMap")).thenReturn(new MapCache(singletonMap(key, value)));
        when(dataStore.getMap("otherMap")).thenReturn(new MapCache(singletonMap(key2, value2)));

        DartGet dartGet = new DartGet(dataStore);

        assertEquals(value, dartGet.eval("someMap", "some-key", 1));
        assertEquals(value2, dartGet.eval("otherMap", "other-key", 1));
    }

    @Test(expected = TagDoesNotExistException.class)
    public void shouldThrowErrorWhenMapDoesNotExist() {
        when(dataStore.getMap("nonExistingMap")).thenThrow(TagDoesNotExistException.class);

        DartGet dartGet = new DartGet(dataStore);

        dartGet.eval("nonExistingMap", "some-key", 1);
    }

    @Test(expected = KeyDoesNotExistException.class)
    public void shouldThrowErrorWhenKeyDoesNotExistAndDefaultValueNotGiven() {
        MapCache mapCache = mock(MapCache.class);
        when(dataStore.getMap("someMap")).thenReturn(mapCache);
        when(mapCache.get("nonExistingKey")).thenThrow(KeyDoesNotExistException.class);

        DartGet dartGet = new DartGet(dataStore);

        dartGet.eval("someMap", "nonExistingKey", 1);
    }

    @Test
    public void shouldReturnDefaultValueWhenKeyIsNotFoundAndDefaultValueGiven() {
        MapCache mapCache = mock(MapCache.class);
        when(dataStore.getMap("someMap")).thenReturn(mapCache);
        when(mapCache.get("nonExistingKey")).thenThrow(KeyDoesNotExistException.class);
        String defaultValue = "some value";

        DartGet dartGet = new DartGet(dataStore);

        assertEquals(defaultValue, dartGet.eval("someMap", "nonExistingKey", 1, defaultValue));
    }

    @Test
    public void shouldNotInvokeDataSourceWhenNotExceededRefreshRate() {
        MapCache mapCache = mock(MapCache.class);
        when(dataStore.getMap("someMap")).thenReturn(mapCache);
        when(mapCache.hasExpired(1)).thenReturn(false);

        DartGet dartGet = new DartGet(dataStore);
        dartGet.eval("someMap", "some-key", 1);
        dartGet.eval("someMap", "some-key", 1);

        verify(dataStore, times(1)).getMap("someMap");
    }


    @Test
    public void shouldInvokeDataSourceWhenExceededRefreshRate() {
        MapCache mapCache = mock(MapCache.class);
        when(dataStore.getMap("someMap")).thenReturn(mapCache);
        when(mapCache.hasExpired(-1)).thenReturn(true);

        DartGet dartGet = new DartGet(dataStore);
        dartGet.eval("someMap", "some-key", -1);

        verify(dataStore, times(1)).getMap("someMap");
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        DartGet dartGet = new DartGet(dataStore);
        dartGet.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
