package com.gojek.daggers.postprocessors.internal.processor.sql;

import com.gojek.daggers.postprocessors.external.common.RowManager;

public interface SqlInternalFieldConfig {

    void processInputColumns(RowManager rowManager);
}
