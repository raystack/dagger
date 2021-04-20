package io.odpf.dagger.functions.udfs.table;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import io.odpf.dagger.functions.udfs.telemetry.UDFTypes;
import io.odpf.dagger.functions.udfs.telemetry.UdfMetricsManager;

import java.util.Arrays;


public class HistogramBucket extends TableFunction<Tuple1<String>> {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        UdfMetricsManager udfMetricsManager = new UdfMetricsManager(context);
        udfMetricsManager.registerGauge(UDFTypes.HISTOGRAM_BUCKET.getValue());
    }

    /**
     * This UDTF returns buckets for given value to calculate histograms.
     * see https://github.com/influxdata/telegraf/tree/master/plugins/aggregators/histogram#tags
     *
     * @param dValue  Value to be compared
     * @param buckets Buckets for Cumulative Histograms
     * @author lavkesh.lahngir
     */
    public void eval(double dValue, String buckets) {
        // Always emit the bucket for '+Inf'
        collect(new Tuple1<>("+Inf"));
        Arrays.stream(buckets.split(",")).
                filter(bucket -> dValue <= Double.parseDouble(bucket)).
                map(Tuple1::new).
                forEach(this::collect);
    }
}
