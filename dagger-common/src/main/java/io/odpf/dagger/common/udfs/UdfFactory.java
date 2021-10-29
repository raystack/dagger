package io.odpf.dagger.common.udfs;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.odpf.dagger.common.configuration.Configuration;

import java.util.HashSet;

/**
 * The Udf factory for scalar functions, table functions, and aggregate functions.
 */
public abstract class UdfFactory {
    private final StreamTableEnvironment streamTableEnvironment;
    private final Configuration configuration;

    /**
     * Instantiates a new Udf factory.
     *
     * @param streamTableEnvironment the stream table environment
     * @param configuration          the configuration
     */
    public UdfFactory(StreamTableEnvironment streamTableEnvironment, Configuration configuration) {
        this.streamTableEnvironment = streamTableEnvironment;
        this.configuration = configuration;
    }

    /**
     * Register functions.
     */
    public final void registerFunctions() {
        HashSet<ScalarUdf> scalarFunctions = getScalarUdfs();
        HashSet<TableUdf> tableFunctions = getTableUdfs();
        HashSet<AggregateUdf> aggregateFunctions = getAggregateUdfs();
        scalarFunctions.forEach((function) -> streamTableEnvironment.createTemporaryFunction(function.getName(), function));
        tableFunctions.forEach((function) -> streamTableEnvironment.createTemporaryFunction(function.getName(), function));
        aggregateFunctions.forEach((function) -> streamTableEnvironment.createTemporaryFunction(function.getName(), function));
    }

    /**
     * Gets scalar udfs.
     *
     * @return the scalar udfs
     */
    public abstract HashSet<ScalarUdf> getScalarUdfs();

    /**
     * Gets table udfs.
     *
     * @return the table udfs
     */
    public abstract HashSet<TableUdf> getTableUdfs();

    /**
     * Gets aggregate udfs.
     *
     * @return the aggregate udfs
     */
    public abstract HashSet<AggregateUdf> getAggregateUdfs();

    /**
     * Gets configuration.
     *
     * @return the configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }
}
