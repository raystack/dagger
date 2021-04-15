package io.odpf.dagger.functions.udfs.table;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HistogramBucketTest {

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
}
