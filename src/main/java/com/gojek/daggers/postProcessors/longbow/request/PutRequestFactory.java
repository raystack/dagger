package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class PutRequestFactory implements Serializable {

    private LongbowSchema longbowSchema;
    private ProtoSerializer protoSerializer;

    public PutRequestFactory(LongbowSchema longbowSchema, ProtoSerializer protoSerializer) {
        this.longbowSchema = longbowSchema;
        this.protoSerializer = protoSerializer;
    }

    public PutRequest create(Row input) {
        if (longbowSchema.hasLongbowData()) {
            return new TablePutRequest(longbowSchema, input);
        } else return new ProtoBytePutRequest(longbowSchema, input, protoSerializer);
    }
}
