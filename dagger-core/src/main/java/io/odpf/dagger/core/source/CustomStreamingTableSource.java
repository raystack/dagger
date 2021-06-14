package io.odpf.dagger.core.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

/**
 * A class that responsible for defines stream table and rowtime attributes.
 */
public class CustomStreamingTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

    private final String rowTimeAttributeName;
    private final Long watermarkDelay;
    private final boolean enablePerPartitionWatermark;
    private final DataStream<Row> dataStream;

    /**
     * Instantiates a new Custom streaming table source.
     *
     * @param rowTimeAttributeName        the row time attribute name
     * @param watermarkDelay              the watermark delay
     * @param enablePerPartitionWatermark the enable per partition watermark
     * @param dataStream                  the data stream
     */
    public CustomStreamingTableSource(String rowTimeAttributeName, Long watermarkDelay, boolean enablePerPartitionWatermark, DataStream<Row> dataStream) {
        this.rowTimeAttributeName = rowTimeAttributeName;
        this.watermarkDelay = watermarkDelay;
        this.enablePerPartitionWatermark = enablePerPartitionWatermark;
        this.dataStream = dataStream;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return this.dataStream.getType();
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
        return this.dataStream;
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        WatermarkStrategy ws =
                enablePerPartitionWatermark ? new PreserveWatermarks() : new BoundedOutOfOrderTimestamps(watermarkDelay);
        return Collections.singletonList(
                new RowtimeAttributeDescriptor(rowTimeAttributeName, new ExistingField(rowTimeAttributeName), ws));
    }
}
