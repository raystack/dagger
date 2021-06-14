package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.storage.ScanRequest;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * Request scan the table.
 */
public class TableScanRequest implements ScanRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);
    private byte[] startRow;
    private byte[] stopRow;
    private LongbowSchema longbowSchema;
    private String tableId;

    /**
     * Instantiates a new Table scan request.
     *
     * @param startRow      the start row
     * @param stopRow       the stop row
     * @param longbowSchema the longbow schema
     * @param tableId       the table id
     */
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
        return c.getKey().contains(Constants.LONGBOW_DATA_KEY);
    }
}
