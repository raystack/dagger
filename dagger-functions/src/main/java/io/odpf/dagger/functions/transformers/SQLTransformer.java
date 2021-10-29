package io.odpf.dagger.functions.transformers;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;
import io.odpf.dagger.common.watermark.RowtimeFieldWatermark;
import io.odpf.dagger.common.watermark.StreamWatermarkAssigner;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;


/**
 * Enables to apply a SQL transformation on top of streaming data in post processors.
 */
public class SQLTransformer implements Serializable, Transformer {
    private final String[] columnNames;
    private final String sqlQuery;
    private final String tableName;
    private final long allowedLatenessInMs;
    private static final String ROWTIME = "rowtime";

    /**
     * Instantiates a new Sql transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public SQLTransformer(Map<String, String> transformationArguments, String[] columnNames, Configuration configuration) {
        this.columnNames = columnNames;
        this.sqlQuery = transformationArguments.get("sqlQuery");
        this.tableName = transformationArguments.getOrDefault("tableName", "data_stream");
        this.allowedLatenessInMs = Long.parseLong(transformationArguments.getOrDefault("allowedLatenessInMs", "0"));
    }

    @Override
    public StreamInfo transform(StreamInfo inputStreamInfo) {
        DataStream<Row> inputStream = inputStreamInfo.getDataStream();
        if (sqlQuery == null) {
            throw new IllegalArgumentException("SQL Query must pe provided in Transformation Arguments");
        }
        String schema = String.join(",", columnNames);
        if (Arrays.asList(columnNames).contains(ROWTIME)) {
            schema = schema.replace(ROWTIME, ROWTIME + ".rowtime");
            inputStream = assignTimeAttribute(inputStream);
        }
        StreamExecutionEnvironment streamExecutionEnvironment = inputStream.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = getStreamTableEnvironment(streamExecutionEnvironment);
        streamTableEnvironment.registerDataStream(tableName, inputStream, schema);

        Table table = streamTableEnvironment.sqlQuery(sqlQuery);
        SingleOutputStreamOperator<Row> outputStream = streamTableEnvironment
                .toRetractStream(table, Row.class)
                .filter(value -> value.f0)
                .map(value -> value.f1);
        return new StreamInfo(outputStream, table.getSchema().getFieldNames());
    }

    /**
     * Gets stream table environment.
     *
     * @param streamExecutionEnvironment the stream execution environment
     * @return the stream table environment
     */
    protected StreamTableEnvironment getStreamTableEnvironment(StreamExecutionEnvironment streamExecutionEnvironment) {
        return StreamTableEnvironment.create(streamExecutionEnvironment);
    }

    private DataStream<Row> assignTimeAttribute(DataStream<Row> inputStream) {
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(new RowtimeFieldWatermark(columnNames));
        return streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, allowedLatenessInMs);
    }
}
