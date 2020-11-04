package com.gojek.daggers.postProcessors.external.grpc;

import com.gojek.daggers.exception.GrpcFailureException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.PostResponseTelemetry;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.stub.StreamObserver;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.OTHER_ERRORS;

public class GrpcResponseHandler implements StreamObserver<DynamicMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcResponseHandler.class.getName());
    private final RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Descriptors.Descriptor descriptor;
    private ResultFuture<Row> resultFuture;
    private GrpcSourceConfig grpcSourceConfig;
    private MeterStatsManager meterStatsManager;
    private Descriptors.Descriptor grpcResponseDescriptor;
    private Instant startTime;
    private ErrorReporter errorReporter;
    private PostResponseTelemetry postResponseTelemetry;

    public GrpcResponseHandler(GrpcSourceConfig grpcSourceConfig, MeterStatsManager meterStatsManager, RowManager rowManager, ColumnNameManager columnNameManager, Descriptors.Descriptor outputDescriptor, Descriptors.Descriptor grpcResponseDescriptor, ResultFuture<Row> resultFuture, ErrorReporter errorReporter, PostResponseTelemetry postResponseTelemetry) {

        this.grpcSourceConfig = grpcSourceConfig;
        this.meterStatsManager = meterStatsManager;
        this.rowManager = rowManager;
        this.columnNameManager = columnNameManager;
        this.descriptor = outputDescriptor;
        this.grpcResponseDescriptor = grpcResponseDescriptor;
        this.resultFuture = resultFuture;
        this.errorReporter = errorReporter;
        this.postResponseTelemetry = postResponseTelemetry;
    }

    private void successHandler(DynamicMessage message)   {
        Map<String, OutputMapping> outputMappings = grpcSourceConfig.getOutputMapping();
        ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());

        outputMappingKeys.forEach(key -> {
            OutputMapping outputMappingKeyConfig = outputMappings.get(key);
            Object value;
            try {

                Descriptors.FieldDescriptor fieldDescriptor;
                    fieldDescriptor = grpcResponseDescriptor.findFieldByName(outputMappingKeyConfig.getPath());
                    value = message.getField(fieldDescriptor);

            } catch (Exception e) {
                meterStatsManager.markEvent(OTHER_ERRORS);
                reportAndThrowError(e);
                return;
            }
            int fieldIndex = columnNameManager.getOutputIndex(key);
            setField(key, value, fieldIndex);
        });
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
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(key);
        if (fieldDescriptor == null) {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Field Descriptor not found for field: " + key);
            reportAndThrowError(illegalArgumentException);
        }
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
        rowManager.setInOutput(fieldIndex, protoHandler.transformFromPostProcessor(value));
    }


    private void reportAndThrowError(Exception e) {
        errorReporter.reportFatalException(e);
        resultFuture.completeExceptionally(e);
    }


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
        meterStatsManager.markEvent(OTHER_ERRORS);
        failureHandler(t.getMessage());

    }

    @Override
    public void onCompleted() {
    }


}
