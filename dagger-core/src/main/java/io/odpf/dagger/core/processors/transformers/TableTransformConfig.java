package io.odpf.dagger.core.processors.transformers;

import java.util.ArrayList;
import java.util.List;

public class TableTransformConfig {
    protected String tableName;
    protected List<TransformConfig> transformers;

    public TableTransformConfig() {
        this.tableName = "NULL";
        this.transformers = new ArrayList<>();
    }

    public TableTransformConfig(String tableName, List<TransformConfig> transformers) {
        this.tableName = tableName;
        this.transformers = transformers;
    }

    public String getTableName() {
        return tableName;
    }

    public List<TransformConfig> getTransformers() {
        return transformers;
    }
}
