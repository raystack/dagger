package io.odpf.dagger.common.udfs;

import io.odpf.dagger.common.udfs.telemetry.UdfMetricsManager;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public abstract class TableUdf<T> extends TableFunction<T> {
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
