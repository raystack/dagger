package com.gojek.daggers.postprocessors.longbow.request;

import com.gojek.daggers.postprocessors.longbow.LongbowSchema;
import com.gojek.daggers.postprocessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;

import static com.gojek.daggers.utils.Constants.*;

public class ProtoBytePutRequest implements PutRequest {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final byte[] QUALIFIER_NAME = Bytes.toBytes(LONGBOW_QUALIFIER_DEFAULT);
    private LongbowSchema longbowSchema;
    private Row input;
    private ProtoSerializer protoSerializer;
    private String tableId;


    public ProtoBytePutRequest(LongbowSchema longbowSchema, Row input, ProtoSerializer protoSerializer, String tableId) {
        this.longbowSchema = longbowSchema;
        this.input = input;
        this.protoSerializer = protoSerializer;
        this.tableId = tableId;
    }

    @Override
    public Put get() {
        Put putRequest = new Put(longbowSchema.getKey(input, 0));
        Timestamp rowtime = (Timestamp) longbowSchema.getValue(input, ROWTIME);
        putRequest.addColumn(COLUMN_FAMILY_NAME, QUALIFIER_NAME, rowtime.getTime(), protoSerializer.serializeValue(input));
        return putRequest;
    }

    @Override
    public String getTableId() {
        return this.tableId;
    }
}
