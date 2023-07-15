package org.raystack.dagger.core.processors.internal.processor;

import org.raystack.dagger.core.processors.common.RowManager;

/**
 * The interface for Internal config processor.
 */
public interface InternalConfigProcessor {
    /**
     * Check if can process internal post processor.
     *
     * @param type the type
     * @return the boolean
     */
    boolean canProcess(String type);

    /**
     * Process.
     *
     * @param rowManager the row manager
     */
    void process(RowManager rowManager);
}
