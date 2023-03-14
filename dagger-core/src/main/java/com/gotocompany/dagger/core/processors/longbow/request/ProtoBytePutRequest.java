package com.gotocompany.dagger.core.processors.longbow.request;

import com.gotocompany.dagger.common.serde.proto.serialization.ProtoSerializer;
import com.gotocompany.dagger.core.processors.longbow.storage.PutRequest;
import com.gotocompany.dagger.core.utils.Constants;
import org.apache.flink.types.Row;

import com.gotocompany.dagger.core.processors.longbow.LongbowSchema;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static com.gotocompany.dagger.common.core.Constants.ROWTIME;

/**
 * Create PutRequest in form of proto byte.
 */
public class ProtoBytePutRequest implements PutRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final byte[] QUALIFIER_NAME = Bytes.toBytes(Constants.LONGBOW_QUALIFIER_DEFAULT);
    private final LongbowSchema longbowSchema;
    private final Row input;
    private final ProtoSerializer protoSerializer;
    private final String tableId;


    /**
     * Instantiates a new Proto byte put request.
     *
     * @param longbowSchema   the longbow schema
     * @param input           the input
     * @param protoSerializer the proto serializer
     * @param tableId         the table id
     */
    public ProtoBytePutRequest(LongbowSchema longbowSchema, Row input, ProtoSerializer protoSerializer, String tableId) {
        this.longbowSchema = longbowSchema;
        this.input = input;
        this.protoSerializer = protoSerializer;
        this.tableId = tableId;
    }

    @Override
    public Put get() {
        Put putRequest = new Put(longbowSchema.getKey(input, 0));
        Timestamp rowtime = convertToTimeStamp(longbowSchema.getValue(input, ROWTIME));
        putRequest.addColumn(COLUMN_FAMILY_NAME, QUALIFIER_NAME, rowtime.getTime(), protoSerializer.serializeValue(input));
        return putRequest;
    }

    @Override
    public String getTableId() {
        return this.tableId;
    }

    private Timestamp convertToTimeStamp(Object timeStampField) {
        if (timeStampField instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) timeStampField);
        }
        return (Timestamp) timeStampField;
    }
}
