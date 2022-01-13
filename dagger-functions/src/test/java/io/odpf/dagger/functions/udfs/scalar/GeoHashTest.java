package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GeoHashTest {
    private static final int GEOHASH_LENGTH = 6;
    private static final double CENTRAL_MONUMENT_JAKARTA_LATITUDE = -6.170024;
    private static final double CENTRAL_MONUMENT_JAKARTA_LONGITUDE = 106.8243203;
    private static final String CENTRAL_MONUMENT_JAKARTA_GEOHASH_LENGTH_6 = "qqguyu";

    private static final String INVALID_CENTRAL_MONUMENT_JAKARTA_LONGITUDE = "{106.8243203}";


    private GeoHash converter;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "GeoHash")).thenReturn(metricGroup);
        converter = new GeoHash();
    }

    @Test
    public void shouldConvertLocationToS2Id() {
        String actualS2Id = converter.eval(CENTRAL_MONUMENT_JAKARTA_LATITUDE, CENTRAL_MONUMENT_JAKARTA_LONGITUDE, GEOHASH_LENGTH);

        assertEquals(actualS2Id, CENTRAL_MONUMENT_JAKARTA_GEOHASH_LENGTH_6);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEForInvalidInput() {
        String actualS2Id = converter.eval(null, CENTRAL_MONUMENT_JAKARTA_LONGITUDE, GEOHASH_LENGTH);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        converter.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
