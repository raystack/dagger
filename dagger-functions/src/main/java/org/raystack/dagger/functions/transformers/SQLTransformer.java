package org.raystack.dagger.functions.transformers;

import org.raystack.dagger.common.core.DaggerContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.core.StreamInfo;
import org.raystack.dagger.common.core.Transformer;
import org.raystack.dagger.common.watermark.RowtimeFieldWatermark;
import org.raystack.dagger.common.watermark.StreamWatermarkAssigner;

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
    private final DaggerContext daggerContext;

    /**
     * Instantiates a new Sql transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param daggerContext           the daggerContext
     */
    public SQLTransformer(Map<String, String> transformationArguments, String[] columnNames, DaggerContext daggerContext) {
        this.columnNames = columnNames;
        this.sqlQuery = transformationArguments.get("sqlQuery");
        this.tableName = transformationArguments.getOrDefault("tableName", "data_stream");
        this.allowedLatenessInMs = Long.parseLong(transformationArguments.getOrDefault("allowedLatenessInMs", "0"));
        this.daggerContext = daggerContext;
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
        StreamTableEnvironment streamTableEnvironment = daggerContext.getTableEnvironment();
        streamTableEnvironment.registerDataStream(tableName, inputStream, schema);

        Table table = streamTableEnvironment.sqlQuery(sqlQuery);
        SingleOutputStreamOperator<Row> outputStream = streamTableEnvironment
                .toRetractStream(table, Row.class)
                .filter(value -> value.f0)
                .map(value -> value.f1);
        return new StreamInfo(outputStream, table.getSchema().getFieldNames());
    }

    private DataStream<Row> assignTimeAttribute(DataStream<Row> inputStream) {
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(new RowtimeFieldWatermark(columnNames));
        return streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, allowedLatenessInMs);
    }
}
