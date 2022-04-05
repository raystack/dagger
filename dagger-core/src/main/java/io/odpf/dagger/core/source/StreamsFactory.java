package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.streamtype.KafkaSourceJsonSchemaStreamType;
import io.odpf.dagger.core.streamtype.KafkaSourceProtoSchema;
import io.odpf.dagger.core.streamtype.ParquetSourceProtoSchema;
import io.odpf.dagger.core.streamtype.StreamType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamsFactory {
    public static List<StreamType<Row>> getStreamTypes(Configuration configuration,
                                                       StencilClientOrchestrator stencilClientOrchestrator) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<StreamType<Row>> streamTypes = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            KafkaSourceJsonSchemaStreamType.KafkaSourceJsonSchemaStreamTypeBuilder kafkaSourceJsonSchemaStreamTypeBuilder = new KafkaSourceJsonSchemaStreamType.KafkaSourceJsonSchemaStreamTypeBuilder(streamConfig, configuration);
            KafkaSourceProtoSchema.KafkaSourceProtoTypeBuilder kafkaSourceProtoTypeBuilder = new KafkaSourceProtoSchema.KafkaSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
            ParquetSourceProtoSchema.ParquetSourceProtoTypeBuilder parquetSourceProtoTypeBuilder = new ParquetSourceProtoSchema.ParquetSourceProtoTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);


            List<StreamType.Builder<Row>> availableStreamTypeBuilders = Arrays
                    .asList(kafkaSourceJsonSchemaStreamTypeBuilder, kafkaSourceProtoTypeBuilder, parquetSourceProtoTypeBuilder);
            StreamType<Row> streamType = availableStreamTypeBuilders
                    .stream()
                    .filter(StreamType.Builder::canBuild)
                    .findFirst()
                    .orElseThrow(() -> new DaggerConfigurationException("Invalid stream config: no suitable stream type can be constructed"))
                    .build();

            streamTypes.add(streamType);
        }
        return streamTypes;
    }
}

