package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.DaggerSource;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

public class DaggerOldKafkaSource implements DaggerSource {
    private StreamConfig streamConfig;
    private final KafkaDeserializationSchema deserializationSchema;
    private final Configuration configuration;

    public DaggerOldKafkaSource(StreamConfig streamConfig, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.deserializationSchema = deserializationSchema;
        this.configuration = configuration;
    }

    @Override
    public boolean canHandle() {
        return SourceName.valueOf(streamConfig.getSourceType()).equals(SourceName.OLD_KAFKA_SOURCE);
    }

    @Override
    public DataStreamSource register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy, String streamName) {
        FlinkKafkaConsumerCustom flinkKafkaConsumerCustom = getFlinkKafkaConsumerCustom();
        return executionEnvironment.addSource(flinkKafkaConsumerCustom.assignTimestampsAndWatermarks(watermarkStrategy));
    }

    FlinkKafkaConsumerCustom getFlinkKafkaConsumerCustom() {
        return new FlinkKafkaConsumerCustom(streamConfig.getTopicPattern(), deserializationSchema, streamConfig.getKafkaProps(configuration), configuration);
    }
}
