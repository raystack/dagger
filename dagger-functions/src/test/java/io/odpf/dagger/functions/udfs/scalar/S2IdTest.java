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

public class S2IdTest {
    private static final int S2LEVEL = 13;
    private static final double CENTRAL_MONUMENT_JAKARTA_LATITUDE = -6.170024;
    private static final double CENTRAL_MONUMENT_JAKARTA_LONGITUDE = 106.8243203;
    private static final String CENTRAL_MONUMENT_JAKARTA_S2ID_LEVEL_13 = "3344474489181175808";

    private static final String INVALID_CENTRAL_MONUMENT_JAKARTA_LONGITUDE = "{106.8243203}";
    private S2Id converter;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;


    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "S2Id")).thenReturn(metricGroup);
        converter = new S2Id();
    }

    @Test
    public void shouldConvertLocationToS2Id() {
        String actualS2Id = converter.eval(CENTRAL_MONUMENT_JAKARTA_LATITUDE, CENTRAL_MONUMENT_JAKARTA_LONGITUDE, S2LEVEL);

        assertEquals(actualS2Id, CENTRAL_MONUMENT_JAKARTA_S2ID_LEVEL_13);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEForInvalidInput() {
        String actualS2Id = converter.eval(null, CENTRAL_MONUMENT_JAKARTA_LONGITUDE, S2LEVEL);

        assertEquals(actualS2Id, CENTRAL_MONUMENT_JAKARTA_S2ID_LEVEL_13);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        converter.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
