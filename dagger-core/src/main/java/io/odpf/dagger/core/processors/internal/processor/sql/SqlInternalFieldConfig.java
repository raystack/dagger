package io.odpf.dagger.core.processors.internal.processor.sql;

import io.odpf.dagger.core.processors.common.RowManager;

/**
 * The interface for Sql internal field config.
 */
public interface SqlInternalFieldConfig {

    /**
     * Process input columns.
     *
     * @param rowManager the row manager
     */
    void processInputColumns(RowManager rowManager);
}
