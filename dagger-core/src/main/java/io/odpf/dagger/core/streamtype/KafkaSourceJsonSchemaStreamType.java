package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

public class KafkaSourceJsonSchemaStreamType extends StreamType<Row> {
    private KafkaSourceJsonSchemaStreamType(Source source, String streamName, DataTypes inputDataType, DaggerDeserializer<Row> deserializer) {
        super(source, streamName, inputDataType, deserializer);
    }

    public static class KafkaSourceJsonSchemaStreamTypeBuilder extends StreamType.Builder<Row> {

        protected static final DataTypes SUPPORTED_INPUT_DATA_TYPE = DataTypes.JSON;
        protected static final SourceName SUPPORTED_SOURCE_NAME = SourceName.KAFKA;
        protected static final SourceType SUPPORTED_SOURCE_TYPE = SourceType.UNBOUNDED;

        public KafkaSourceJsonSchemaStreamTypeBuilder(StreamConfig streamConfig, Configuration configuration) {
            super(streamConfig, configuration);
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
                return sourceName.equals(SUPPORTED_SOURCE_NAME)
                        && sourceType.equals(SUPPORTED_SOURCE_TYPE)
                        && inputDataType.equals(SUPPORTED_INPUT_DATA_TYPE);
            }
        }

        @Override
        public StreamType<Row> build() {
            JsonDeserializer deserializer = new JsonDeserializer(streamConfig.getJsonSchema(), streamConfig.getJsonEventTimestampFieldName());
            KafkaRecordDeserializationSchema<Row> kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.of(deserializer);
            Source source = buildKafkaSource(kafkaRecordDeserializationSchema);
            String streamName = streamConfig.getSchemaTable();
            return new KafkaSourceJsonSchemaStreamType(source, streamName, SUPPORTED_INPUT_DATA_TYPE, deserializer);
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
