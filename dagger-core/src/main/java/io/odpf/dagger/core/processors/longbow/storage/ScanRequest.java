package io.odpf.dagger.core.processors.longbow.storage;

import org.apache.hadoop.hbase.client.Scan;

/**
 * The interface Scan request.
 */
public interface ScanRequest {
    /**
     * Get scan.
     *
     * @return the scan
     */
    Scan get();

    /**
     * Sets scan range.
     *
     * @param startRow the start row
     * @param stopRow  the stop row
     * @return the scan range
     */
    default Scan setScanRange(byte[] startRow, byte[] stopRow) {
        Scan scan = new Scan();
        scan.withStartRow(startRow, true);
        scan.withStopRow(stopRow, true);
        return scan;
    }

    /**
     * Gets table id.
     *
     * @return the table id
     */
    String getTableId();
}
