package io.odpf.dagger.core.processors.longbow.range;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * The interface Longbow range.
 */
public interface LongbowRange extends Serializable {
    /**
     * Get upper bounds.
     *
     * @param input the input
     * @return the upper bounds
     */
    byte[] getUpperBound(Row input);

    /**
     * Get lower bounds.
     *
     * @param input the input
     * @return the lower bounds
     */
    byte[] getLowerBound(Row input);

    /**
     * Get invalid fields.
     *
     * @return the invalid fields
     */
    String[] getInvalidFields();
}
