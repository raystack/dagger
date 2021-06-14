package io.odpf.dagger.core.processors.transformers;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that holds Table transformer configuration.
 */
public class TableTransformConfig {
    protected String tableName;
    protected List<TransformConfig> transformers;

    /**
     * Instantiates a new Table transform config.
     */
    public TableTransformConfig() {
        this.tableName = "NULL";
        this.transformers = new ArrayList<>();
    }

    /**
     * Instantiates a new Table transform config.
     *
     * @param tableName    the table name
     * @param transformers the transformers
     */
    public TableTransformConfig(String tableName, List<TransformConfig> transformers) {
        this.tableName = tableName;
        this.transformers = transformers;
    }

    /**
     * Gets table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets transformers.
     *
     * @return the transformers
     */
    public List<TransformConfig> getTransformers() {
        return transformers;
    }
}
