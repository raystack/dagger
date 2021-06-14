package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.storage.ScanRequest;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Request scan the proto byte.
 */
public class ProtoByteScanRequest implements ScanRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final byte[] QUALIFIER_NAME = Bytes.toBytes(Constants.LONGBOW_QUALIFIER_DEFAULT);
    private byte[] startRow;
    private byte[] stopRow;
    private String tableId;


    /**
     * Instantiates a new Proto byte scan request.
     *
     * @param startRow the start row
     * @param stopRow  the stop row
     * @param tableId  the table id
     */
    public ProtoByteScanRequest(byte[] startRow, byte[] stopRow, String tableId) {
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.tableId = tableId;
    }

    @Override
    public Scan get() {
        Scan scan = setScanRange(startRow, stopRow);
        scan.addColumn(COLUMN_FAMILY_NAME, QUALIFIER_NAME);
        return scan;
    }

    @Override
    public String getTableId() {
        return tableId;
    }
}
