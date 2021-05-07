package io.odpf.dagger.common.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.HashSet;

public abstract class UdfFactory {
    private final StreamTableEnvironment streamTableEnvironment;
    private final Configuration configuration;

    public UdfFactory(StreamTableEnvironment streamTableEnvironment, Configuration configuration) {
        this.streamTableEnvironment = streamTableEnvironment;
        this.configuration = configuration;
    }

    public final void registerFunctions() {
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

    public Configuration getConfiguration() {
        return configuration;
    }
}
