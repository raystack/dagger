package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;
import io.odpf.dagger.functions.udfs.aggregate.feast.FeatureUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Converts to feast Features from post processors.
 */
public class FeatureTransformer implements MapFunction<Row, Row>, Transformer {
    private static final int FEATURE_ROW_LENGTH = 3;
    private static final String KEY_COLUMN_NAME = "keyColumnName";
    private static final String VALUE_COLUMN_NAME = "valueColumnName";
    private final String keyColumn;
    private final String valueColumn;
    private String[] columnNames;

    /**
     * Instantiates a new Feature transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public FeatureTransformer(Map<String, String> transformationArguments, String[] columnNames, Configuration configuration) {
        this.columnNames = columnNames;
        this.keyColumn = transformationArguments.get(KEY_COLUMN_NAME);
        this.valueColumn = transformationArguments.get(VALUE_COLUMN_NAME);
    }

    @Override
    public Row map(Row inputRow) throws Exception {
        int featureKeyIndex = Arrays.asList(columnNames).indexOf(keyColumn);
        int featureValueIndex = Arrays.asList(columnNames).indexOf(valueColumn);
        if (featureKeyIndex == -1 || featureValueIndex == -1) {
            throw new IllegalArgumentException("FeatureKey OR FeatureValue is not defined OR doesn't exists");
        }
        ArrayList<Row> featureRows = new ArrayList<>();
        String featureKey = String.valueOf(inputRow.getField(featureKeyIndex));
        Object featureValue = inputRow.getField(featureValueIndex);

        FeatureUtils.populateFeatures(featureRows, featureKey, featureValue, FEATURE_ROW_LENGTH);

        Row outputRow = new Row(inputRow.getArity());
        for (int index = 0; index < inputRow.getArity(); index++) {
            outputRow.setField(index, inputRow.getField(index));
        }
        outputRow.setField(featureValueIndex, featureRows.toArray(new Row[0]));
        return outputRow;
    }

    @Override
    public StreamInfo transform(StreamInfo inputStreamInfo) {
        DataStream<Row> inputStream = inputStreamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        return new StreamInfo(outputStream, inputStreamInfo.getColumnNames());
    }

}
