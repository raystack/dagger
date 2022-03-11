package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.types.Row;

import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY;

public class DeserializerFactory {
    public static DaggerDeserializer<Row> create(SourceName sourceName, DataTypes inputDataType, StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        switch (sourceName) {
            case KAFKA:
                return createKafkaDeserializer(inputDataType, streamConfig, configuration, stencilClientOrchestrator);
            case PARQUET:
                return createParquetDeserializer(inputDataType, streamConfig, configuration, stencilClientOrchestrator);
            default: {
                String message = String.format("Invalid stream configuration: No suitable deserializer could be constructed for source of type %s", sourceName);
                throw new IllegalConfigurationException(message);
            }
        }
    }

    private static DaggerDeserializer<Row> createParquetDeserializer(DataTypes inputDataType, StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        if (inputDataType == DataTypes.PROTO) {
            int timestampFieldIndex = Integer.parseInt(streamConfig.getEventTimestampFieldIndex());
            String protoClassName = streamConfig.getProtoClass();
            String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
            return new SimpleGroupDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator);
        }
        String message = String.format("Invalid stream configuration: No suitable Parquet deserializer could be constructed for STREAM_INPUT_DATATYPE with value %s", inputDataType);
        throw new IllegalConfigurationException(message);
    }

    private static DaggerDeserializer<Row> createKafkaDeserializer(DataTypes inputDataType, StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        switch (inputDataType) {
            case PROTO: {
                int timestampFieldIndex = Integer.parseInt(streamConfig.getEventTimestampFieldIndex());
                String protoClassName = streamConfig.getProtoClass();
                String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
                return new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator);
            }
            case JSON: {
                return new JsonDeserializer(streamConfig.getJsonSchema(), streamConfig.getJsonEventTimestampFieldName());
            }
            default: {
                String message = String.format("Invalid stream configuration: No suitable Kafka deserializer could be constructed for STREAM_INPUT_DATATYPE with value %s", inputDataType);
                throw new IllegalConfigurationException(message);
            }
        }
    }
}