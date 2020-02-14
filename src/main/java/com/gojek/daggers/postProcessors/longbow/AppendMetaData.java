package com.gojek.daggers.postProcessors.longbow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

import static com.gojek.daggers.utils.Constants.*;

public class AppendMetaData implements MapFunction<Row, Row> {
    private static final int LONGBOW_EXTRA_FIELDS = 4;
    private Configuration configuration;
    private String inputProtoClassName;


    public AppendMetaData(Configuration configuration, String inputProtoClassName) {
        this.configuration = configuration;
        this.inputProtoClassName = inputProtoClassName;
    }

    @Override
    public Row map(Row row) {
        int inputRowSize = row.getArity();
        Row outputRow = new Row(inputRowSize + LONGBOW_EXTRA_FIELDS);
        IntStream.range(0, inputRowSize).forEach(i -> outputRow.setField(i, row.getField(i)));
        outputRow.setField(inputRowSize, configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT));
        outputRow.setField(inputRowSize + 1, configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT));
        outputRow.setField(inputRowSize + 2, configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT));
        outputRow.setField(inputRowSize + 3, inputProtoClassName);
        return outputRow;
    }
}
