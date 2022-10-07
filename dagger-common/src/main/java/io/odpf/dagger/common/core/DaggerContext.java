package io.odpf.dagger.common.core;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.exceptions.DaggerContextException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DaggerContext singleton object initializes with StreamExecutionEnvironment, StreamTableEnvironment and Configuration
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
     * @param executionEnvironment the StreamExecutionEnvironment
     * @param tableEnvironment     the StreamTableEnvironment
     * @param configuration        the Configuration
     */
    private DaggerContext(StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, Configuration configuration) {
        this.executionEnvironment = executionEnvironment;
        this.tableEnvironment = tableEnvironment;
        this.configuration = configuration;
    }

    /**
     * Get the instance of DaggerContext
     */
    public static DaggerContext getInstance() {
        if (daggerContext == null) {
            throw new DaggerContextException("DaggerContext object is not initialized.");
        }
        return daggerContext;
    }

    /**
     * Initialization of a new DaggerContext.
     *
     * @param executionEnvironment the StreamExecutionEnvironment
     * @param tableEnvironment     the StreamTableEnvironment
     * @param configuration        the Configuration
     */
    public synchronized static DaggerContext init(StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, Configuration configuration) {
        if (daggerContext != null) {
            throw new DaggerContextException("DaggerContext object is already initialized.");
        }
        daggerContext = new DaggerContext(executionEnvironment, tableEnvironment, configuration);
        LOGGER.info("DaggerContext is initialized.");
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
