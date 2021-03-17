package com.gojek.daggers.processors.common;

import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.processors.types.FilterDecorator;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static com.gojek.daggers.utils.Constants.INTERNAL_VALIDATION_FILED;

public class ValidRecordsDecorator extends RichFilterFunction<Row> implements FilterDecorator {

    private final String tableName;
    private final int validationIndex;
    protected ErrorReporter errorReporter;

    public ValidRecordsDecorator(String tableName, String[] columns) {
        this.tableName = tableName;
        validationIndex = Arrays.asList(columns).indexOf(INTERNAL_VALIDATION_FILED);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), configuration);
    }

    @Override
    public Boolean canDecorate() {
        return true;
    }

    @Override
    public boolean filter(Row value) throws Exception {
        if (!(boolean) value.getField(validationIndex)) {
            Exception ex = new InvalidProtocolBufferException("Bad Record Encountered for table `" + this.tableName + "`");
            errorReporter.reportFatalException(ex);
            throw ex;
        }
        return true;

    }
}
