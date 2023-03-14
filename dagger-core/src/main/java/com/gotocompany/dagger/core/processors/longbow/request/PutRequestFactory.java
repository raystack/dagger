package com.gotocompany.dagger.core.processors.longbow.request;

import com.gotocompany.dagger.common.serde.proto.serialization.ProtoSerializer;
import org.apache.flink.types.Row;

import com.gotocompany.dagger.core.processors.longbow.LongbowSchema;
import com.gotocompany.dagger.core.processors.longbow.storage.PutRequest;

import java.io.Serializable;

/**
 * The factory class for put request.
 */
public class PutRequestFactory implements Serializable {

    private final LongbowSchema longbowSchema;
    private final ProtoSerializer protoSerializer;
    private final String tableId;

    /**
     * Instantiates a new Put request factory.
     *
     * @param longbowSchema   the longbow schema
     * @param protoSerializer the proto serializer
     * @param tableId         the table id
     */
    public PutRequestFactory(LongbowSchema longbowSchema, ProtoSerializer protoSerializer, String tableId) {
        this.longbowSchema = longbowSchema;
        this.protoSerializer = protoSerializer;
        this.tableId = tableId;
    }

    /**
     * Create put request.
     *
     * @param input the input
     * @return the put request
     */
    public PutRequest create(Row input) {
        if (!longbowSchema.isLongbowPlus()) {
            return new TablePutRequest(longbowSchema, input, tableId);
        } else {
            return new ProtoBytePutRequest(longbowSchema, input, protoSerializer, tableId);
        }
    }
}
