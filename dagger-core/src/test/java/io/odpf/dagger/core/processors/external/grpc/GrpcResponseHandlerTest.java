package io.odpf.dagger.core.processors.external.grpc;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestGrpcResponse;
import io.odpf.dagger.consumer.TestLocation;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.FAILURES_ON_READING_PATH;
import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.OTHER_ERRORS;
import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.SUCCESS_RESPONSE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcResponseHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private GrpcSourceConfig grpcSourceConfig;

    @Mock
    private ErrorReporter errorReporter;

    private Descriptors.Descriptor descriptor;
    private List<String> outputColumnNames;
    private String[] inputColumnNames;
    private HashMap<String, OutputMapping> outputMapping;
    private HashMap<String, String> headers;
    private Row streamData;
    private RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Row inputData;

    @Before
    public void setup() {
        initMocks(this);
        descriptor = TestGrpcResponse.getDescriptor();
        outputColumnNames = Arrays.asList("success", "value", "surge_factor");
        inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        outputMapping = new HashMap<>();
        headers = new HashMap<>();
        headers.put("content-type", "application/json");
        streamData = new Row(2);
        inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(2));
        rowManager = new RowManager(streamData);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        grpcSourceConfig = new GrpcSourceConfig("localhost", 5000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.GrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);
    }

    @Test
    public void shouldDetectProperBodyAndHandleResponseIfRetainResponseTypeIsFalse() throws InvalidProtocolBufferException {
        outputMapping.put("success", new OutputMapping("$.success"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.TestGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setType("io.odpf.dagger.consumer.TestGrpcResponse");
        DynamicMessage message = DynamicMessage.parseFrom(TestGrpcResponse.getDescriptor(), TestGrpcResponse.newBuilder().setSuccess(true).build().toByteArray());
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, true);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        grpcResponseHandler.onNext(message);


        verify(meterStatsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldDetectProperBodyAndHandleResponseIfRetainResponseTypeIsFalseANdTypeHasDifferentDatType() throws InvalidProtocolBufferException {

        descriptor = TestBookingLogMessage.getDescriptor();

        outputMapping.put("driver_id", new OutputMapping("$.driver_id"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.TestGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);
        grpcSourceConfig.setRetainResponseType(false);
        grpcSourceConfig.setType("io.odpf.dagger.consumer.TestAggregatedSupplyMessage");


        streamData.setField(0, inputData);
        inputData.setField(2, "123456");
        outputColumnNames = Arrays.asList("driver_id");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);


        TestBookingLogMessage build = TestBookingLogMessage.newBuilder().setDriverId("250").build();
        DynamicMessage message = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), build.toByteArray());
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, TestBookingLogMessage.getDescriptor(), resultFuture, errorReporter, new PostResponseTelemetry());


        streamData.setField(1, new Row(1));
        rowManager = new RowManager(streamData);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(1);
        outputData.setField(0, "250");
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        grpcResponseHandler.onNext(message);


        verify(meterStatsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }


    @Test
    public void shouldRecordErrorInCaseOfUnknownException() throws InvalidProtocolBufferException {
        outputMapping.put("success", new OutputMapping("$.success"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.TestGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, null);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        grpcResponseHandler.onError(new Throwable("io.grpc.StatusRuntimeException: UNKNOWN"));


        verify(meterStatsManager, times(1)).markEvent(OTHER_ERRORS);
        verify(errorReporter, times(1)).reportNonFatalException(any());
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldRecordFatalErrorInCaseOfUnknownExceptionWithFailOnErrorTrue() throws InvalidProtocolBufferException {
        outputMapping.put("success", new OutputMapping("$.success"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.TestGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setFailOnErrors(true);
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, null);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        grpcResponseHandler.onError(new Throwable("io.grpc.StatusRuntimeException: UNKNOWN"));

        verify(meterStatsManager, times(1)).markEvent(OTHER_ERRORS);
        verify(errorReporter, times(1)).reportFatalException(any());
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    //todo: check this test
    @Test
    public void shouldDetectExceptionIfMessageIsWrong() throws InvalidProtocolBufferException {
        outputMapping.put("success", new OutputMapping("$.order_number"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.de.meta.WrongGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setRetainResponseType(true);
        grpcSourceConfig.setFailOnErrors(true);

        DynamicMessage message = DynamicMessage.parseFrom(TestGrpcResponse.getDescriptor(), TestGrpcResponse.newBuilder().setSuccess(true).build().toByteArray());

        descriptor = TestBookingLogMessage.getDescriptor();
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, null);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        grpcResponseHandler.onNext(message);


        verify(meterStatsManager, times(1)).markEvent(FAILURES_ON_READING_PATH);
        verify(errorReporter, times(1)).reportFatalException(any());
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldThrowErrorWhenFieldIsNotPresentInOutputDescriptor() throws InvalidProtocolBufferException {
        outputMapping.put("value", new OutputMapping("$.field3"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.TestGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setType("io.odpf.dagger.consumer.TestGrpcResponse");


        DynamicMessage message = DynamicMessage.parseFrom(TestGrpcResponse.getDescriptor(), TestGrpcResponse.newBuilder().setSuccess(true).build().toByteArray());
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, TestBookingLogMessage.getDescriptor(), resultFuture, errorReporter, new PostResponseTelemetry());

        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, true);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        try {
            grpcResponseHandler.onNext(message);
        } catch (Exception ignored) {
        } finally {
            verify(errorReporter, times(1)).reportFatalException(any());
            verify(resultFuture, times(1)).completeExceptionally(any(IllegalArgumentException.class));
        }


    }


    @Test
    public void shouldDetectProperComplexBodyAndHandleResponseIfRetainResponseTypeIsFalse() throws InvalidProtocolBufferException {
        outputMapping.put("address", new OutputMapping("$.driver_pickup_location.address"));
        outputMapping.put("name", new OutputMapping("$.driver_pickup_location.name"));


        TestLocation location = TestLocation.newBuilder().setAddress("Indonesia").setName("GojekTech").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setDriverPickupLocation(location).setCustomerId("123456").build();

        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "io.odpf.dagger.consumer.TestGrpcRequest", "io.odpf.dagger.consumer.TestGrpcResponse", "io.odpf.dagger.consumer.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);
        grpcSourceConfig.setRetainResponseType(true);

        outputColumnNames = Arrays.asList("address", "name");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);


        DynamicMessage message = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, "Indonesia");
        outputData.setField(1, "GojekTech");
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);

        grpcResponseHandler.startTimer();
        grpcResponseHandler.onNext(message);


        verify(meterStatsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

}
