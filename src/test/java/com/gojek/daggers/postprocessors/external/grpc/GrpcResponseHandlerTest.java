package com.gojek.daggers.postprocessors.external.grpc;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.Aspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.external.common.OutputMapping;
import com.gojek.daggers.postprocessors.external.common.PostResponseTelemetry;
import com.gojek.daggers.postprocessors.external.common.RowManager;
import com.gojek.esb.bicicilan.BiDriverCicilanLogMessage;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.consumer.TestGrpcResponse;
import com.gojek.esb.de.meta.GrpcResponse;
import com.gojek.esb.types.Location;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
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
        outputColumnNames = Arrays.asList(new String[]{"success", "value", "surge_factor"});
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
        grpcSourceConfig = new GrpcSourceConfig("localhost", 5000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);
    }

    @Test
    public void shouldDetectProperBodyAndHandleResponseIfRetainResponseTypeIsFalse() throws InvalidProtocolBufferException {
        outputMapping.put("success", new OutputMapping("$.success"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setType("com.gojek.esb.de.meta.GrpcResponse");
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

        descriptor = BiDriverCicilanLogMessage.getDescriptor();

        outputMapping.put("driver_id", new OutputMapping("$.driver_id"));
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);
        grpcSourceConfig.setRetainResponseType(false);
        grpcSourceConfig.setType("com.gojek.esb.bicicilan.BiDriverCicilanLogMessage");


        streamData.setField(0, inputData);
        inputData.setField(2, "123456");
        outputColumnNames = Arrays.asList(new String[]{"driver_id"});
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);


        BookingLogMessage build = BookingLogMessage.newBuilder().setDriverId("250").build();
        DynamicMessage message = DynamicMessage.parseFrom(BookingLogMessage.getDescriptor(), build.toByteArray());
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, BiDriverCicilanLogMessage.getDescriptor(), resultFuture, errorReporter, new PostResponseTelemetry());

        
        streamData.setField(1, new Row(1));
        rowManager = new RowManager(streamData);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(1);
        outputData.setField(0, 250);
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
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

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
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

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
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.WrongGrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setRetainResponseType(true);
        grpcSourceConfig.setFailOnErrors(true);

        DynamicMessage message = DynamicMessage.parseFrom(GrpcResponse.getDescriptor(), GrpcResponse.newBuilder().setSuccess(true).build().toByteArray());

        descriptor = BookingLogMessage.getDescriptor();
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
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);

        grpcSourceConfig.setType("com.gojek.esb.de.meta.GrpcResponse");


        DynamicMessage message = DynamicMessage.parseFrom(TestGrpcResponse.getDescriptor(), TestGrpcResponse.newBuilder().setSuccess(true).build().toByteArray());
        GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, meterStatsManager, rowManager, columnNameManager, BookingLogMessage.getDescriptor(), resultFuture, errorReporter, new PostResponseTelemetry());

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
        outputMapping.put("address", new OutputMapping("$.driver_arrived_location.address"));
        outputMapping.put("name", new OutputMapping("$.driver_arrived_location.name"));

        Location location = Location.newBuilder().setAddress("Indonesia").setName("GojekTech").build();
        BookingLogMessage bookingLogMessage = BookingLogMessage.newBuilder().setDriverArrivedLocation(location).setCustomerId("123456").build();

        grpcSourceConfig = new GrpcSourceConfig("localhost", 8000, "com.gojek.esb.GrpcRequest", "com.gojek.esb.de.meta.GrpcResponse", "com.gojek.esb.test/TestMethod", "{\"key\": \"%s\"}", "customer_id", outputMapping);
        grpcSourceConfig.setRetainResponseType(true);

        outputColumnNames = Arrays.asList(new String[]{"address", "name"});
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);


        DynamicMessage message = DynamicMessage.parseFrom(BookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());
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
