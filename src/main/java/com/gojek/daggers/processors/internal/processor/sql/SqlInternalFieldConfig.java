package com.gojek.daggers.processors.internal.processor.sql;

import com.gojek.daggers.processors.common.RowManager;

public interface SqlInternalFieldConfig {

    void processInputColumns(RowManager rowManager);
}
