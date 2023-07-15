package org.raystack.dagger.common.udfs;

import org.raystack.dagger.common.metrics.managers.GaugeStatsManager;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import static org.raystack.dagger.common.core.Constants.GAUGE_ASPECT_NAME;
import static org.raystack.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

/**
 * Abstarct class for Table udf.
 *
 * @param <T> the type parameter
 */
public abstract class TableUdf<T> extends TableFunction<T> {

    private GaugeStatsManager gaugeStatsManager;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        gaugeStatsManager = new GaugeStatsManager(context.getMetricGroup(), true);
        gaugeStatsManager.registerInteger(UDF_TELEMETRY_GROUP_KEY, getName(), GAUGE_ASPECT_NAME, 1);
    }

    /**
     * Gets table udf name.
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
