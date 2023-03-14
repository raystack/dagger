package com.gotocompany.dagger.core.sink.kafka;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.common.serde.DataTypes;
import com.gotocompany.dagger.core.sink.kafka.builder.KafkaJsonSerializerBuilder;
import com.gotocompany.dagger.core.sink.kafka.builder.KafkaProtoSerializerBuilder;
import com.gotocompany.dagger.core.utils.Constants;

public class KafkaSerializationSchemaFactory {
    public static KafkaSerializerBuilder getSerializationSchema(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, String[] columnNames) {
        DataTypes dataTypes = DataTypes.valueOf(configuration.getString(Constants.SINK_KAFKA_DATA_TYPE, "PROTO"));

        if (dataTypes == DataTypes.JSON) {
            return new KafkaJsonSerializerBuilder(configuration);
        }
        return new KafkaProtoSerializerBuilder(configuration, stencilClientOrchestrator, columnNames);
    }
}
