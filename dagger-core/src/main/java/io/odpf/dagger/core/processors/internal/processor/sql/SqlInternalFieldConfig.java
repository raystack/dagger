package io.odpf.dagger.core.processors.internal.processor.sql;

import io.odpf.dagger.core.processors.common.RowManager;

public interface SqlInternalFieldConfig {

    void processInputColumns(RowManager rowManager);
}
