package io.odpf.dagger.functions.udfs.table;

import io.odpf.dagger.common.udfs.TableUdf;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.Arrays;


/**
 * The class responsible for Histogram bucket udf.
 */
public class HistogramBucket extends TableUdf<Tuple1<String>> {

    /**
     * This UDF returns buckets for given value to calculate histograms.
     * see https://github.com/influxdata/telegraf/tree/master/plugins/aggregators/histogram#tags
     *
     * @param dValue  Value to be compared
     * @param buckets Buckets for Cumulative Histograms
     * @author lavkesh.lahngir
     */
    public void eval(Double dValue, String buckets) {
        // Always emit the bucket for '+Inf'
        collect(new Tuple1<>("+Inf"));
        Arrays.stream(buckets.split(",")).
                filter(bucket -> dValue <= Double.parseDouble(bucket)).
                map(Tuple1::new).
                forEach(this::collect);
    }
}
