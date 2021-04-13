package io.odpf.dagger.processors.longbow.request;

import io.odpf.dagger.processors.longbow.LongbowSchema;
import io.odpf.dagger.processors.longbow.storage.PutRequest;
import io.odpf.dagger.sink.ProtoSerializer;

import org.apache.flink.types.Row;

import java.io.Serializable;

public class PutRequestFactory implements Serializable {

    private LongbowSchema longbowSchema;
    private ProtoSerializer protoSerializer;
    private String tableId;

    public PutRequestFactory(LongbowSchema longbowSchema, ProtoSerializer protoSerializer, String tableId) {
        this.longbowSchema = longbowSchema;
        this.protoSerializer = protoSerializer;
        this.tableId = tableId;
    }

    public PutRequest create(Row input) {
        if (!longbowSchema.isLongbowPlus()) {
            return new TablePutRequest(longbowSchema, input, tableId);
        } else return new ProtoBytePutRequest(longbowSchema, input, protoSerializer, tableId);
    }
}
