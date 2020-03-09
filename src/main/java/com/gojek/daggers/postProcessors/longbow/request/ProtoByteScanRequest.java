package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.storage.ScanRequest;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import static com.gojek.daggers.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_QUALIFIER_DEFAULT;

public class ProtoByteScanRequest implements ScanRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final byte[] QUALIFIER_NAME = Bytes.toBytes(LONGBOW_QUALIFIER_DEFAULT);
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
