package com.gotocompany.dagger.core.processors.external.grpc;

import com.gotocompany.dagger.core.exception.GrpcFailureException;
import com.gotocompany.dagger.core.metrics.aspects.ExternalSourceAspects;
import com.gotocompany.dagger.core.metrics.reporters.ErrorReporter;
import com.gotocompany.dagger.core.processors.ColumnNameManager;
import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.gotocompany.dagger.core.processors.common.PostResponseTelemetry;
import com.gotocompany.dagger.core.processors.common.RowManager;
import com.gotocompany.dagger.common.metrics.managers.MeterStatsManager;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.dagger.core.utils.DescriptorsUtil;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.grpc.stub.StreamObserver;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * The Grpc response handler.
 */
public class GrpcResponseHandler implements StreamObserver<DynamicMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcResponseHandler.class.getName());
    private final RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Descriptors.Descriptor descriptor;
    private ResultFuture<Row> resultFuture;
    private GrpcSourceConfig grpcSourceConfig;
    private MeterStatsManager meterStatsManager;
    private Instant startTime;
    private ErrorReporter errorReporter;
    private PostResponseTelemetry postResponseTelemetry;

    /**
     * Instantiates a new Grpc response handler.
     *
     * @param grpcSourceConfig      the grpc source config
     * @param meterStatsManager     the meter stats manager
     * @param rowManager            the row manager
     * @param columnNameManager     the column name manager
     * @param outputDescriptor      the output descriptor
     * @param resultFuture          the result future
     * @param errorReporter         the error reporter
     * @param postResponseTelemetry the post response telemetry
     */
    public GrpcResponseHandler(GrpcSourceConfig grpcSourceConfig, MeterStatsManager meterStatsManager, RowManager rowManager, ColumnNameManager columnNameManager, Descriptors.Descriptor outputDescriptor, ResultFuture<Row> resultFuture, ErrorReporter errorReporter, PostResponseTelemetry postResponseTelemetry) {

        this.grpcSourceConfig = grpcSourceConfig;
        this.meterStatsManager = meterStatsManager;
        this.rowManager = rowManager;
        this.columnNameManager = columnNameManager;
        this.descriptor = outputDescriptor;
        this.resultFuture = resultFuture;
        this.errorReporter = errorReporter;
        this.postResponseTelemetry = postResponseTelemetry;
    }

    private void successHandler(DynamicMessage message) {
        Map<String, OutputMapping> outputMappings = grpcSourceConfig.getOutputMapping();
        ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());

        try {
            String json = JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames().print(message);

            outputMappingKeys.forEach(key -> {
                OutputMapping outputMappingKeyConfig = outputMappings.get(key);
                Object value;
                try {

                    value = JsonPath.parse(json).read(outputMappingKeyConfig.getPath(), Object.class);

                } catch (PathNotFoundException e) {
                    postResponseTelemetry.failureReadingPath(meterStatsManager);
                    LOGGER.error(e.getMessage());
                    reportAndThrowError(e);
                    return;
                }
                int fieldIndex = columnNameManager.getOutputIndex(key);
                setField(key, value, fieldIndex);
            });
        } catch (InvalidProtocolBufferException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
            LOGGER.error(e.getMessage());
            reportAndThrowError(e);
            return;
        }
        postResponseTelemetry.sendSuccessTelemetry(meterStatsManager, startTime);
        resultFuture.complete(Collections.singleton(rowManager.getAll()));

    }

    private void setField(String key, Object value, int fieldIndex) {
        if (!grpcSourceConfig.isRetainResponseType() || grpcSourceConfig.hasType()) {
            setFieldUsingType(key, value, fieldIndex);
        } else {
            rowManager.setInOutput(fieldIndex, value);
        }
    }

    private void setFieldUsingType(String key, Object value, int fieldIndex) {
        Descriptors.FieldDescriptor fieldDescriptor = null;
        try {
            fieldDescriptor = DescriptorsUtil.getFieldDescriptor(descriptor, key);
            if (fieldDescriptor == null) {
                throw new IllegalArgumentException("Field Descriptor not found for field: " + key);
            }
        } catch (RuntimeException exception) {
            reportAndThrowError(exception);
        }
        TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
        rowManager.setInOutput(fieldIndex, typeHandler.transformFromPostProcessor(value));
    }


    private void reportAndThrowError(Exception e) {
        errorReporter.reportFatalException(e);
        resultFuture.completeExceptionally(e);
    }


    /**
     * Failure handler.
     *
     * @param logMessage the log message
     */
    public void failureHandler(String logMessage) {
        postResponseTelemetry.sendFailureTelemetry(meterStatsManager, startTime);
        LOGGER.error(logMessage);
        Exception grpcFailureException = new GrpcFailureException(logMessage);
        if (grpcSourceConfig.isFailOnErrors()) {
            reportAndThrowError(grpcFailureException);
        } else {
            errorReporter.reportNonFatalException(grpcFailureException);
        }
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    /**
     * Start timer.
     */
    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public void onNext(DynamicMessage message) {
        successHandler(message);
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
        meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
        failureHandler(t.getMessage());

    }

    @Override
    public void onCompleted() {
    }


}
