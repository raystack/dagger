package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.DynamicMessage;

/**
 * The interface Proto handler.
 */
public interface ProtoHandler {
    /**
     * Can handle boolean.
     *
     * @return the boolean
     */
    boolean canHandle();

    /**
     * Transform to protobuf message builder.
     *
     * @param builder the builder
     * @param field   the field
     * @return the dynamic message . builder
     */
    DynamicMessage.Builder transformToProtoBuilder(DynamicMessage.Builder builder, Object field);

    /**
     * Transform from post processor object.
     *
     * @param field the field
     * @return the object
     */
    Object transformFromPostProcessor(Object field);

    /**
     * Transform from protobuf message.
     *
     * @param field the field
     * @return the object
     */
    Object transformFromProto(Object field);

    /**
     * Transform from parquet object.
     *
     * @param field the field
     * @return the object
     */
    Object transformFromParquet(Object field);

    /**
     * Transform to json object.
     *
     * @param field the field
     * @return the object
     */
    Object transformToJson(Object field);

    /**
     * Gets type information.
     *
     * @return the type information
     */
    TypeInformation getTypeInformation();
}
