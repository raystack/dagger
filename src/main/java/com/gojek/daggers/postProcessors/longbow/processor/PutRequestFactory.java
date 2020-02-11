package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.types.Row;

import java.io.Serializable;

import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class PutRequestFactory implements Serializable {

    private LongbowSchema longbowSchema;
    private ProtoSerializer protoSerializer;

    public PutRequestFactory(LongbowSchema longbowSchema, ProtoSerializer protoSerializer) {
        this.longbowSchema = longbowSchema;
        this.protoSerializer = protoSerializer;
    }

    public PutRequest create(Row input) {
        if (longbowSchema.contains(LONGBOW_DATA)) {
            return new TablePutRequest(longbowSchema, input);
        } else return new ProtoBytePutRequest(longbowSchema, input, protoSerializer);
    }
}
