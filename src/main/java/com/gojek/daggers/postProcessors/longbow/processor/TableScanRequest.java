package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

import static com.gojek.daggers.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class TableScanRequest implements ScanRequest {
    private byte[] startRow;
    private byte[] stopRow;
    private LongbowSchema longbowSchema;
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    public TableScanRequest(byte[] startRow, byte[] stopRow, LongbowSchema longbowSchema) {
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.longbowSchema = longbowSchema;
    }

    @Override
    public Scan get() {
        Scan scan = setScanRange(startRow, stopRow);
        longbowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(column -> scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column)));

        return scan;
    }

    private boolean isLongbowData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(LONGBOW_DATA);
    }
}
