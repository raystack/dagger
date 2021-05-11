package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class S2AreaInKm2Test {
    private static final String TEST_S2ID = "3267730965564227584";
    private static final double TEST_AREA = 909.97058;

    private S2AreaInKm2 converter;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() throws Exception {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "S2AreaInKm2")).thenReturn(metricGroup);
        converter = new S2AreaInKm2();
        converter.open(functionContext);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldComputAreaForS2Id() {
        double actualArea = converter.eval(TEST_S2ID);

        assertEquals(actualArea, TEST_AREA, 0.0001);
    }
}
