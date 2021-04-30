package io.odpf.dagger.common.udfs;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

import io.odpf.dagger.common.metrics.managers.GaugeStatsManager;

import static io.odpf.dagger.common.udfs.Constants.GAUGE_ASPECT_NAME;
import static io.odpf.dagger.common.udfs.Constants.UDF_TELEMETRY_GROUP_KEY;

/**
 * This class will not publish the UDF telemetry because
 * for AggregatedFunction it can not be done due to this bug in flink
 * ISSUE : https://issues.apache.org/jira/browse/FLINK-15040
 */
public abstract class AggregateUdf<T, ACC> extends AggregateFunction<T, ACC> {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        GaugeStatsManager metricsManager = new GaugeStatsManager(context.getMetricGroup(), true);
        metricsManager.register(UDF_TELEMETRY_GROUP_KEY, getName(), GAUGE_ASPECT_NAME, 1);
    }

    public String getName() {
        return this.getClass().getSimpleName();
    }
}
