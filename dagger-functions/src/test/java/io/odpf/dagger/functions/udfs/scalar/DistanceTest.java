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

public class DistanceTest {

    private Distance distance;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "Distance")).thenReturn(metricGroup);
        distance = new Distance();
    }

    @Test
    public void shouldReturnHaversineDistanceBetweenParisAndLondon() {
        double parisLat = 48.8566;
        double parisLon = 2.3522;

        double londonLat = 51.5074;
        double londonLon = 0.1278;

        double distanceInKm = distance.eval(parisLat, parisLon, londonLat, londonLon);

        assertEquals(334.5761379805, distanceInKm, 0.0000000001);
    }

    @Test
    public void shouldReturnHaversineDistanceBetweenBangaloreAndJakarta() {
        double bangaloreLat = 12.9716;
        double bangaloreLon = 77.5946;

        double jakartaLat = 6.1751;
        double jakartaLon = 106.8650;

        double distanceInKm = distance.eval(bangaloreLat, bangaloreLon, jakartaLat, jakartaLon);

        assertEquals(3294.208197583382, distanceInKm, 0.0000000001);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        distance.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
