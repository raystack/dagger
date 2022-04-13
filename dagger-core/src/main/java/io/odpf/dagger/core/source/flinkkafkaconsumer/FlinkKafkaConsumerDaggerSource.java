package io.odpf.dagger.core.source.flinkkafkaconsumer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.dagger.core.source.DaggerSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import static io.odpf.dagger.core.source.SourceName.KAFKA_CONSUMER;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;

public class FlinkKafkaConsumerDaggerSource implements DaggerSource<Row> {

    private final DaggerDeserializer<Row> deserializer;
    private final StreamConfig streamConfig;
    private final Configuration configuration;
    private static final SourceName SUPPORTED_SOURCE_NAME = KAFKA_CONSUMER;
    private static final SourceType SUPPORTED_SOURCE_TYPE = UNBOUNDED;

    public FlinkKafkaConsumerDaggerSource(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.deserializer = deserializer;
    }

    private FlinkKafkaConsumerCustom buildSource() {
        KafkaDeserializationSchema kafkaDeserializationSchema = (KafkaDeserializationSchema<Row>) deserializer;
        return new FlinkKafkaConsumerCustom(streamConfig.getTopicPattern(),
                kafkaDeserializationSchema, streamConfig.getKafkaProps(configuration), configuration);
    }

    @Override
    public DataStream<Row> register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy) {
        FlinkKafkaConsumerCustom source = buildSource();
        return executionEnvironment.addSource(source.assignTimestampsAndWatermarks(watermarkStrategy));
    }

    @Override
    public boolean canBuild() {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        if (sourceDetailsArray.length != 1) {
            return false;
        } else {
            SourceName sourceName = sourceDetailsArray[0].getSourceName();
            SourceType sourceType = sourceDetailsArray[0].getSourceType();
            return sourceName.equals(SUPPORTED_SOURCE_NAME) && sourceType.equals(SUPPORTED_SOURCE_TYPE)
                    && deserializer instanceof KafkaDeserializationSchema;
        }
    }
}
