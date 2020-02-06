package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import static com.gojek.daggers.utils.Constants.LONGBOW_VERSION_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_VERSION_KEY;

public class PutRequestFactory {

    private LongbowSchema longbowSchema;
    private Configuration configuration;
    private ProtoSerializer protoSerializer;

    public PutRequestFactory(LongbowSchema longbowSchema, Configuration configuration, ProtoSerializer protoSerializer) {
        this.longbowSchema = longbowSchema;
        this.configuration = configuration;
        this.protoSerializer = protoSerializer;
    }

    public PutRequest create(Row input) {
        if (configuration.getString(LONGBOW_VERSION_KEY, LONGBOW_VERSION_DEFAULT).equals("1")) {
            return new TablePutRequest(longbowSchema, input);
        } else return new ProtoBytePutRequest(longbowSchema, input, protoSerializer);
    }
}
