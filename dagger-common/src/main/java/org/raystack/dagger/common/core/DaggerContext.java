package org.raystack.dagger.common.core;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.exceptions.DaggerContextException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DaggerContext singleton object.
 * It initializes with StreamExecutionEnvironment, StreamTableEnvironment and Configuration.
 */
public class DaggerContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(DaggerContext.class.getName());
    private static volatile DaggerContext daggerContext = null;
    private final StreamExecutionEnvironment executionEnvironment;
    private final StreamTableEnvironment tableEnvironment;
    private final Configuration configuration;

    /**
     * Instantiates a new DaggerContext.
     *
     * @param configuration the Configuration
     */
    private DaggerContext(Configuration configuration) {
        this.executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);
        this.configuration = configuration;
    }

    /**
     * Get the instance of DaggerContext.
     */
    public static DaggerContext getInstance() {
        if (daggerContext == null) {
            throw new DaggerContextException("DaggerContext object is not initialized");
        }
        return daggerContext;
    }

    /**
     * Initialization of a new DaggerContext.
     *
     * @param configuration the Configuration
     */
    public static synchronized DaggerContext init(Configuration configuration) {
        if (daggerContext != null) {
            throw new DaggerContextException("DaggerContext object is already initialized");
        }
        daggerContext = new DaggerContext(configuration);
        LOGGER.info("DaggerContext is initialized");
        return daggerContext;
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
        return executionEnvironment;
    }

    public StreamTableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
