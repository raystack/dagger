package com.gojek.daggers.postProcessors.internal.processor.sql;

import com.gojek.daggers.postProcessors.external.common.RowManager;

public interface SqlInternalFieldConfig {

    void processInputColumns(RowManager rowManager);
}
