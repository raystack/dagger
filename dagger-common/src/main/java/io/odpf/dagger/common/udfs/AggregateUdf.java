package io.odpf.dagger.common.udfs;

import io.odpf.dagger.common.metrics.managers.GaugeStatsManager;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

import static io.odpf.dagger.common.core.Constants.GAUGE_ASPECT_NAME;
import static io.odpf.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

/**
 * This class will not publish the UDF telemetry.
 * Because for AggregatedFunction it can not be done due to this bug in flink
 * ISSUE : https://issues.apache.org/jira/browse/FLINK-15040
 *
 * @param <T>   the type parameter
 * @param <ACC> the type parameter
 */
public abstract class AggregateUdf<T, ACC> extends AggregateFunction<T, ACC> {

    private GaugeStatsManager gaugeStatsManager;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        gaugeStatsManager = new GaugeStatsManager(context.getMetricGroup(), true);
        gaugeStatsManager.registerInteger(UDF_TELEMETRY_GROUP_KEY, getName(), GAUGE_ASPECT_NAME, 1);
    }

    /**
     * Gets aggregate udf name.
     *
     * @return the name
     */
    public String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Gets gauge stats manager.
     *
     * @return the gauge stats manager
     */
    public GaugeStatsManager getGaugeStatsManager() {
        return gaugeStatsManager;
    }
}
