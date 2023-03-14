package com.gotocompany.dagger.functions.transformers;

import com.gotocompany.dagger.common.core.DaggerContext;
import com.gotocompany.dagger.functions.transformers.feature.FeatureWithTypeHandler;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import com.gotocompany.dagger.common.core.StreamInfo;
import com.gotocompany.dagger.common.core.Transformer;

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
     * @param daggerContext           the daggerContext
     */
    public FeatureWithTypeTransformer(Map<String, Object> transformationArguments, String[] columnNames, DaggerContext daggerContext) {
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
