package io.odpf.dagger.common.udfs;

import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.HashSet;

public abstract class UdfFactory {
    private final StreamTableEnvironment streamTableEnvironment;

    public UdfFactory(StreamTableEnvironment streamTableEnvironment) {
        this.streamTableEnvironment = streamTableEnvironment;
    }

    final public void registerFunctions() {
        HashSet<ScalarUdf> scalarFunctions = getScalarUdfs();
        HashSet<TableUdf> tableFunctions = getTableUdfs();
        HashSet<AggregateUdf> aggregateFunctions = getAggregateUdfs();
        scalarFunctions.forEach((function) -> streamTableEnvironment.registerFunction(function.getName(), function));
        tableFunctions.forEach((function) -> streamTableEnvironment.registerFunction(function.getName(), function));
        aggregateFunctions.forEach((function) -> streamTableEnvironment.registerFunction(function.getName(), function));
    }

    public abstract HashSet<ScalarUdf> getScalarUdfs();

    public abstract HashSet<TableUdf> getTableUdfs();

    public abstract HashSet<AggregateUdf> getAggregateUdfs();
}
