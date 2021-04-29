package io.odpf.dagger.common.udfs;

import io.odpf.dagger.common.metrics.managers.UdfMetricsManager;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

/**
 * This class will not publish the UDF telemetry because
 * for AggregatedFunction it can not be done due to this bug in flink
 * ISSUE : https://issues.apache.org/jira/browse/FLINK-15040
 */
public abstract class AggregateUdf<T, ACC> extends AggregateFunction<T, ACC> {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        UdfMetricsManager udfMetricsManager = new UdfMetricsManager(context.getMetricGroup());
        udfMetricsManager.registerGauge(getName());
    }

    public String getName() {
        return this.getClass().getSimpleName();
    }
}
