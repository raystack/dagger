package com.gojek.daggers.postprocessors.longbow.request;

import com.gojek.daggers.postprocessors.longbow.LongbowSchema;
import com.gojek.daggers.postprocessors.longbow.storage.PutRequest;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;

import static com.gojek.daggers.utils.Constants.*;

/**
 * Create PutRequest in form of table. LONGBOW_KEY as range key,
 * LONGBOW_COLUMN_NAME as qualifier, and LONGBOW_DATA as value.
 */
public class TablePutRequest implements PutRequest {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    private LongbowSchema longbowSchema;
    private Row input;
    private String tableId;

    public TablePutRequest(LongbowSchema longbowSchema, Row input, String tableId) {
        this.longbowSchema = longbowSchema;
        this.input = input;
        this.tableId = tableId;
    }

    @Override
    public Put get() {
        Put putRequest = new Put(longbowSchema.getKey(input, 0));
        Timestamp rowtime = (Timestamp) longbowSchema.getValue(input, ROWTIME);
        longbowSchema.getColumnNames(c -> c.getKey().contains(LONGBOW_DATA))
                .forEach(column -> putRequest.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column), rowtime.getTime(),
                        Bytes.toBytes((String) longbowSchema.getValue(input, column))));
        return putRequest;
    }

    @Override
    public String getTableId() {
        return this.tableId;
    }
}
