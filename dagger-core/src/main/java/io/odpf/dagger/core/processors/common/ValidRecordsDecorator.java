package io.odpf.dagger.core.processors.common;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.types.Row;

import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.Constants;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.processors.types.FilterDecorator;

import java.util.Arrays;

/**
 * The Valid records decorator.
 */
public class ValidRecordsDecorator extends RichFilterFunction<Row> implements FilterDecorator {

    private final String tableName;
    private final int validationIndex;
    private final Configuration configuration;
    /**
     * The Error reporter.
     */
    protected ErrorReporter errorReporter;

    /**
     * Instantiates a new Valid records decorator.
     *
     * @param tableName     the table name
     * @param columns       the columns
     * @param configuration
     */
    public ValidRecordsDecorator(String tableName, String[] columns, Configuration configuration) {
        this.tableName = tableName;
        validationIndex = Arrays.asList(columns).indexOf(Constants.INTERNAL_VALIDATION_FIELD_KEY);
        this.configuration = configuration;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext().getMetricGroup(), this.configuration);
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
