package io.odpf.dagger.core.processors.longbow.request;

import org.apache.flink.types.Row;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.storage.PutRequest;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static io.odpf.dagger.common.core.Constants.ROWTIME;

/**
 * Create PutRequest in form of table. LONGBOW_KEY as range key,
 * LONGBOW_COLUMN_NAME as qualifier, and LONGBOW_DATA as value.
 */
public class TablePutRequest implements PutRequest {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);

    private LongbowSchema longbowSchema;
    private Row input;
    private String tableId;

    /**
     * Instantiates a new Table put request.
     *
     * @param longbowSchema the longbow schema
     * @param input         the input row
     * @param tableId       the table id
     */
    public TablePutRequest(LongbowSchema longbowSchema, Row input, String tableId) {
        this.longbowSchema = longbowSchema;
        this.input = input;
        this.tableId = tableId;
    }

    @Override
    public Put get() {
        Put putRequest = new Put(longbowSchema.getKey(input, 0));
        Timestamp rowtime = convertToTimeStamp(longbowSchema.getValue(input, ROWTIME));
        longbowSchema.getColumnNames(c -> c.getKey().contains(Constants.LONGBOW_DATA_KEY))
                .forEach(column -> putRequest.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column), rowtime.getTime(),
                        Bytes.toBytes((String) longbowSchema.getValue(input, column))));
        return putRequest;
    }

    private Timestamp convertToTimeStamp(Object timeStampField) {
        if (timeStampField instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) timeStampField);
        }
        return (Timestamp) timeStampField;
    }

    @Override
    public String getTableId() {
        return this.tableId;
    }
}
