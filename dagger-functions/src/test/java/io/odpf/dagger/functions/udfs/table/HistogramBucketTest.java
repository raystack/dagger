package io.odpf.dagger.functions.udfs.table;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HistogramBucketTest {

    @Mock
    private FunctionContext functionContext;

    @Mock
    private MetricGroup metricGroup;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "HistogramBucket")).thenReturn(metricGroup);
    }

    @Test
    public void testCollectCumulativeBucketsForAValue() {
        HistogramBucket bucketUDF = new HistogramBucket();
        Collector<Tuple1<String>> collector = mock(Collector.class);
        bucketUDF.setCollector(collector);
        bucketUDF.eval(10.0, "1,2,5,10,20");
        verify(collector, times(1)).collect(new Tuple1<>("+Inf"));
        verify(collector, times(1)).collect(new Tuple1<>("10"));
        verify(collector, times(1)).collect(new Tuple1<>("20"));
    }

    @Test
    public void testCollectInfBucketForAValue() {
        HistogramBucket bucketUDF = new HistogramBucket();
        Collector<Tuple1<String>> collector = mock(Collector.class);
        bucketUDF.setCollector(collector);
        bucketUDF.eval(30.0, "1,2,5,10,20");
        verify(collector, times(1)).collect(new Tuple1<>("+Inf"));
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        HistogramBucket bucketUDF = new HistogramBucket();
        bucketUDF.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
