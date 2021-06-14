package io.odpf.dagger.core.processors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * The interface Writer output row.
 */
public interface WriterOutputRow extends Serializable {
    /**
     * Get row.
     *
     * @param input the input
     * @return the row
     */
    Row get(Row input);
}
