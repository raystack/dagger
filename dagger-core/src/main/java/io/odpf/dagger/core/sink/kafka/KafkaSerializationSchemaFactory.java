package io.odpf.dagger.core.sink.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.sink.kafka.builder.KafkaJsonSerializerBuilder;
import io.odpf.dagger.core.sink.kafka.builder.KafkaProtoSerializerBuilder;
import io.odpf.dagger.core.utils.Constants;

public class KafkaSerializationSchemaFactory {
    public static KafkaSerializerBuilder getSerializationSchema(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames) {
        DataTypes dataTypes = DataTypes.valueOf(configuration.getString(Constants.SINK_KAFKA_DATA_TYPE, "PROTO"));

        if (dataTypes == DataTypes.JSON) {
            return new KafkaJsonSerializerBuilder(configuration);
        }
        return new KafkaProtoSerializerBuilder(configuration, stencilClientOrchestrator, columnNames);
    }
}
