package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.ScanRequest;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

import static com.gojek.daggers.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class TableScanRequest implements ScanRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private byte[] startRow;
    private byte[] stopRow;
    private LongbowSchema longbowSchema;
    private String tableId;

    public TableScanRequest(byte[] startRow, byte[] stopRow, LongbowSchema longbowSchema, String tableId) {
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
    }

    @Override
    public Scan get() {
        Scan scan = setScanRange(startRow, stopRow);
        longbowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(column -> scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column)));

        return scan;
    }

    @Override
    public String getTableId() {
        return tableId;
    }

    private boolean isLongbowData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(LONGBOW_DATA);
    }
}
