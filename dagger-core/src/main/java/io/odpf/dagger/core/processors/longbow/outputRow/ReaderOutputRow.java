package io.odpf.dagger.core.processors.longbow.outputRow;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Map;

/**
 * The interface Reader output row.
 */
public interface ReaderOutputRow extends Serializable {
    /**
     * Get row.
     *
     * @param scanResult the scan result
     * @param input      the input
     * @return the row
     */
    Row get(Map<String, Object> scanResult, Row input);
}
