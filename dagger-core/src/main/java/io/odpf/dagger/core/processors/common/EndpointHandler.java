package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.types.SourceConfig;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.protohandler.ProtoHandler;
import io.odpf.dagger.core.protohandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;

/**
 * The Endpoint handler.
 */
public class EndpointHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointHandler.class.getName());
    private SourceConfig sourceConfig;
    private MeterStatsManager meterStatsManager;
    private ErrorReporter errorReporter;
    private String[] inputProtoClasses;
    private Map<String, Descriptors.FieldDescriptor> descriptorMap;
    private ColumnNameManager columnNameManager;
    private DescriptorManager descriptorManager;
    private Descriptors.Descriptor descriptor;

    /**
     * Instantiates a new Endpoint handler.
     *
     * @param sourceConfig      the source config
     * @param meterStatsManager the meter stats manager
     * @param errorReporter     the error reporter
     * @param inputProtoClasses the input proto classes
     * @param columnNameManager the column name manager
     * @param descriptorManager the descriptor manager
     */
    public EndpointHandler(SourceConfig sourceConfig,
                           MeterStatsManager meterStatsManager,
                           ErrorReporter errorReporter,
                           String[] inputProtoClasses,
                           ColumnNameManager columnNameManager,
                           DescriptorManager descriptorManager) {
        this.sourceConfig = sourceConfig;
        this.meterStatsManager = meterStatsManager;
        this.errorReporter = errorReporter;
        this.inputProtoClasses = inputProtoClasses;
        this.columnNameManager = columnNameManager;
        this.descriptorManager = descriptorManager;
    }

    /**
     * Get endpoint or query variables values.
     *
     * @param rowManager   the row manager
     * @param resultFuture the result future
     * @return the array object
     */
    public Object[] getEndpointOrQueryVariablesValues(RowManager rowManager, ResultFuture<Row> resultFuture) {
        String queryVariables = sourceConfig.getVariables();
        if (StringUtils.isEmpty(queryVariables)) {
            return new Object[0];
        }

        String[] requiredInputColumns = queryVariables.split(",");
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        if (descriptorMap == null) {
            descriptorMap = createDescriptorMap(requiredInputColumns, inputProtoClasses, resultFuture);
        }

        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                throw new InvalidConfigurationException(String.format("Column '%s' not found as configured in the endpoint/query variable", inputColumnName));
            }

            Descriptors.FieldDescriptor fieldDescriptor = descriptorMap.get(inputColumnName);
            Object singleColumnValue;
            if (fieldDescriptor != null) {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
                singleColumnValue = protoHandler.transformToJson(rowManager.getFromInput(inputColumnIndex));
            } else {
                singleColumnValue = rowManager.getFromInput(inputColumnIndex);
            }
            inputColumnValues.add(singleColumnValue);
        }
        return inputColumnValues.toArray();
    }

    /**
     * Check if the query is invalid.
     *
     * @param resultFuture            the result future
     * @param rowManager              the row manager
     * @param endpointVariablesValues the endpoint variables values
     * @return the boolean
     */
    public boolean isQueryInvalid(ResultFuture<Row> resultFuture, RowManager rowManager, Object[] endpointVariablesValues) {
        if (!StringUtils.isEmpty(sourceConfig.getVariables()) && (Arrays.asList(endpointVariablesValues).isEmpty() || Arrays.stream(endpointVariablesValues).allMatch(""::equals))) {
            LOGGER.warn("Could not populate any request variable. Skipping external calls");
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
            resultFuture.complete(singleton(rowManager.getAll()));
            return true;
        }
        return false;
    }

    private Map<String, Descriptors.FieldDescriptor> createDescriptorMap(String[] requiredInputColumns,
                                                                         String[] inputProtoClassNames,
                                                                         ResultFuture<Row> resultFuture) {
        HashMap<String, Descriptors.FieldDescriptor> descriptorHashMap = new HashMap<>();
        Descriptors.Descriptor currentDescriptor;
        for (String columnName : requiredInputColumns) {
            for (String protoClassName : inputProtoClassNames) {
                currentDescriptor = getInputDescriptor(resultFuture, protoClassName);
                Descriptors.FieldDescriptor currentFieldDescriptor = currentDescriptor.findFieldByName(columnName);
                if (currentFieldDescriptor != null && descriptorHashMap.get(columnName) == null) {
                    descriptorHashMap.put(columnName, currentFieldDescriptor);
                }
            }
        }
        return descriptorHashMap;
    }

    private Descriptors.Descriptor getInputDescriptor(ResultFuture<Row> resultFuture, String protoClassName) {
        try {
            descriptor = descriptorManager.getDescriptor(protoClassName);
        } catch (DescriptorNotFoundException descriptorNotFound) {
            reportAndThrowError(resultFuture, descriptorNotFound);
        }
        return descriptor;
    }

    private void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }
}
