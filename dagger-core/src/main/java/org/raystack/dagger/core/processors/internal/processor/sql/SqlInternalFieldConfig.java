package org.raystack.dagger.core.processors.internal.processor.sql;

import org.raystack.dagger.core.processors.common.RowManager;

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
