package org.raystack.dagger.core.sink.kafka;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.serde.DataTypes;
import org.raystack.dagger.core.sink.kafka.builder.KafkaJsonSerializerBuilder;
import org.raystack.dagger.core.sink.kafka.builder.KafkaProtoSerializerBuilder;
import org.raystack.dagger.core.utils.Constants;

public class KafkaSerializationSchemaFactory {
    public static KafkaSerializerBuilder getSerializationSchema(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames) {
        DataTypes dataTypes = DataTypes.valueOf(configuration.getString(Constants.SINK_KAFKA_DATA_TYPE, "PROTO"));

        if (dataTypes == DataTypes.JSON) {
            return new KafkaJsonSerializerBuilder(configuration);
        }
        return new KafkaProtoSerializerBuilder(configuration, stencilClientOrchestrator, columnNames);
    }
}
