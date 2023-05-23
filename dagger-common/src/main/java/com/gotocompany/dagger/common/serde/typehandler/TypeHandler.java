package com.gotocompany.dagger.common.serde.typehandler;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.DynamicMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;

/**
 * The interface Proto handler.
 */
public interface TypeHandler {
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
     * Transform from protobuf message.
     *
     * @param field the field
     * @param cache
     * @return the object
     */
    Object transformFromProtoUsingCache(Object field, FieldDescriptorCache cache);

    /**
     * Transform from parquet SimpleGroup.
     *
     * @param simpleGroup the SimpleGroup object
     * @return the transformed object
     */
    Object transformFromParquet(SimpleGroup simpleGroup);

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
