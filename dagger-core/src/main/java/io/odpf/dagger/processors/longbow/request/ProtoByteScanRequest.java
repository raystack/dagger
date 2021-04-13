package io.odpf.dagger.processors.longbow.request;

import io.odpf.dagger.processors.longbow.storage.ScanRequest;
import io.odpf.dagger.utils.Constants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ProtoByteScanRequest implements ScanRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final byte[] QUALIFIER_NAME = Bytes.toBytes(Constants.LONGBOW_QUALIFIER_DEFAULT);
    private byte[] startRow;
    private byte[] stopRow;
    private String tableId;


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
