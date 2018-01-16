package com.gojek.daggers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

public class KafkaProtoStreamingTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

    private final FlinkKafkaConsumerBase<Row> kafkaConsumer;
    private String rowTimeAttributeName;
    private Long watermarkDelay;

    public KafkaProtoStreamingTableSource(FlinkKafkaConsumerBase<Row> kafkaConsumer, String rowTimeAttributeName, Long watermarkDelay) {
        this.kafkaConsumer = kafkaConsumer;
        this.rowTimeAttributeName = rowTimeAttributeName;
        this.watermarkDelay = watermarkDelay;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return kafkaConsumer.getProducedType();
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(getReturnType());
    }

    @Override
    public String explainSource() {
        return "Lets you write sql queries for streaming protobuf data from kafka!!";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(kafkaConsumer);
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return Collections.singletonList(new RowtimeAttributeDescriptor(rowTimeAttributeName, new ExistingField(rowTimeAttributeName), new BoundedOutOfOrderTimestamps(watermarkDelay)));
    }

}
