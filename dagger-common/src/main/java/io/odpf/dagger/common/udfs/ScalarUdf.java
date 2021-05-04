package io.odpf.dagger.common.udfs;

import io.odpf.dagger.common.metrics.managers.GaugeStatsManager;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import static io.odpf.dagger.common.core.Constants.GAUGE_ASPECT_NAME;
import static io.odpf.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

public abstract class ScalarUdf extends ScalarFunction {

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
