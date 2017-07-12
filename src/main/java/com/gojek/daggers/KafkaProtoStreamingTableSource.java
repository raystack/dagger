package com.gojek.daggers;

import com.gojek.esb.booking.BookingLogMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.sources.DefinedRowtimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class KafkaProtoStreamingTableSource implements StreamTableSource<Row>, DefinedRowtimeAttribute {

    private final FlinkKafkaConsumerBase kafkaConsumer;
    private final RowTimestampExtractor rowTimestampExtractor;
    private ProtoType protoType;
    private String rowTimeAttributeName;

    public KafkaProtoStreamingTableSource(FlinkKafkaConsumerBase kafkaConsumer, RowTimestampExtractor rowTimestampExtractor, ProtoType protoType, String rowTimeAttributeName) {

        this.kafkaConsumer = kafkaConsumer;
        this.rowTimestampExtractor = rowTimestampExtractor;
        this.protoType = protoType;
        this.rowTimeAttributeName = rowTimeAttributeName;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return protoType.getRowType();
    }

    @Override
    public String explainSource() {
        return "Lets you write sql queries for streaming protobuf data from kafka!!";
    }

    @Override
    public String getRowtimeAttribute() {
        return rowTimeAttributeName;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(kafkaConsumer).assignTimestampsAndWatermarks(rowTimestampExtractor);
    }

}
