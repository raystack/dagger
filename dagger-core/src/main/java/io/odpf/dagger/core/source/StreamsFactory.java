package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.streamtype.KafkaSourceJsonSchemaStreamType;
import io.odpf.dagger.core.streamtype.KafkaSourceProtoSchemaStreamType;
import io.odpf.dagger.core.streamtype.ParquetSourceProtoSchemaStreamType;
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
            KafkaSourceProtoSchemaStreamType.KafkaSourceProtoSchemaStreamTypeBuilder kafkaSourceProtoSchemaStreamTypeBuilder = new KafkaSourceProtoSchemaStreamType.KafkaSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);
            ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder parquetSourceProtoSchemaStreamTypeBuilder = new ParquetSourceProtoSchemaStreamType.ParquetSourceProtoSchemaStreamTypeBuilder(streamConfig, configuration, stencilClientOrchestrator);


            List<StreamType.Builder<Row>> availableStreamTypeBuilders = Arrays
                    .asList(kafkaSourceJsonSchemaStreamTypeBuilder, kafkaSourceProtoSchemaStreamTypeBuilder, parquetSourceProtoSchemaStreamTypeBuilder);
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

