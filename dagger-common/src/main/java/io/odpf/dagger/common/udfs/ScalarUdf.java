package io.odpf.dagger.common.udfs;

import io.odpf.dagger.common.metrics.managers.UdfMetricsManager;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public abstract class ScalarUdf extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        UdfMetricsManager udfMetricsManager = new UdfMetricsManager(context);
        udfMetricsManager.registerGauge(getName());
    }

    public String getName() {
        return this.getClass().getSimpleName();
    }
}
