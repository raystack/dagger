package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.core.source.*;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY;

public class KafkaSourceProtoType extends StreamType<Row> {

    public KafkaSourceProtoType(Source source, String streamName, DataTypes inputDataType, DaggerDeserializer<Row> deserializer) {
        super(source, streamName, inputDataType, deserializer);
    }

    public static class KafkaSourceProtoTypeBuilder extends StreamType.Builder<Row> {
        protected final DataTypes SUPPORTED_INPUT_DATA_TYPE = DataTypes.PROTO;
        protected final SourceName SUPPORTED_SOURCE_NAME = SourceName.KAFKA;
        protected final SourceType SUPPORTED_SOURCE_TYPE = SourceType.UNBOUNDED;

        public KafkaSourceProtoTypeBuilder(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
            super(streamConfig, configuration, stencilClientOrchestrator);
        }

        @Override
        public boolean canBuild() {
            SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
            if (sourceDetailsArray.length != 1) {
                return false;
            } else {
                SourceName sourceName = sourceDetailsArray[0].getSourceName();
                SourceType sourceType = sourceDetailsArray[0].getSourceType();
                DataTypes inputDataType = DataTypes.valueOf(streamConfig.getDataType());
                return sourceName.equals(SUPPORTED_SOURCE_NAME) &&
                        sourceType.equals(SUPPORTED_SOURCE_TYPE) &&
                        inputDataType.equals(SUPPORTED_INPUT_DATA_TYPE);
            }
        }

        @Override
        public KafkaSourceProtoType build() {
            ProtoDeserializer deserializer = buildProtoDeserializer();
            KafkaRecordDeserializationSchema<Row> kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.of(deserializer);
            Source source = buildKafkaSource(kafkaRecordDeserializationSchema);
            String streamName = streamConfig.getSchemaTable();
            return new KafkaSourceProtoType(source, streamName, SUPPORTED_INPUT_DATA_TYPE, deserializer);
        }

        private ProtoDeserializer buildProtoDeserializer() {
            int timestampFieldIndex = Integer.parseInt(streamConfig.getEventTimestampFieldIndex());
            String protoClassName = streamConfig.getProtoClass();
            String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
            return new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator);
        }

        private KafkaSource<Row> buildKafkaSource(KafkaRecordDeserializationSchema<Row> deserializer) {
            return KafkaSource.<Row>builder()
                    .setTopicPattern(streamConfig.getTopicPattern())
                    .setStartingOffsets(streamConfig.getStartingOffset())
                    .setProperties(streamConfig.getKafkaProps(configuration))
                    .setDeserializer(deserializer)
                    .build();
        }
    }
}
