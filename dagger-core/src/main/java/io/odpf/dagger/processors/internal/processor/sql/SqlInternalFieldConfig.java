package io.odpf.dagger.processors.internal.processor.sql;

import io.odpf.dagger.processors.common.RowManager;

public interface SqlInternalFieldConfig {

    void processInputColumns(RowManager rowManager);
}
