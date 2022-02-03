package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;
import io.odpf.dagger.functions.transformers.feature.FeatureWithTypeHandler;

import java.util.ArrayList;
import java.util.Map;

/**
 * Converts to feast Features from post processors.
 * This is required to do aggregation and feature transformation from a single dagger.
 */
public class FeatureWithTypeTransformer implements MapFunction<Row, Row>, Transformer {

    private FeatureWithTypeHandler featureWithTypeHandler;

    /**
     * Instantiates a new Feature with type transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public FeatureWithTypeTransformer(Map<String, Object> transformationArguments, String[] columnNames, Configuration configuration) {
        this.featureWithTypeHandler = new FeatureWithTypeHandler(transformationArguments, columnNames);
    }

    @Override
    public Row map(Row inputRow) {
        ArrayList<Row> featureRows = featureWithTypeHandler.populateFeatures(inputRow);

        Row outputRow = new Row(inputRow.getArity());
        for (int index = 0; index < inputRow.getArity(); index++) {
            outputRow.setField(index, inputRow.getField(index));
        }
        outputRow.setField(featureWithTypeHandler.getOutputColumnIndex(), featureRows.toArray(new Row[0]));
        return outputRow;
    }

    @Override
    public StreamInfo transform(StreamInfo inputStreamInfo) {
        DataStream<Row> inputStream = inputStreamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        return new StreamInfo(outputStream, inputStreamInfo.getColumnNames());
    }

}
