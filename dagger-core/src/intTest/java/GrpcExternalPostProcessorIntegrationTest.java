import io.odpf.dagger.consumer.TestGrpcRequest;
import io.odpf.dagger.consumer.TestGrpcResponse;
import io.odpf.dagger.consumer.TestServerGrpc;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.processors.PostProcessorFactory;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.processors.types.PostProcessor;
import io.odpf.dagger.utils.Constants;
import org.grpcmock.GrpcMock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static io.odpf.dagger.utils.Constants.INPUT_STREAMS;
import static io.odpf.dagger.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
import static io.odpf.dagger.utils.Constants.STENCIL_ENABLE_KEY;
import static org.grpcmock.GrpcMock.stubFor;
import static org.grpcmock.GrpcMock.unaryMethod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class GrpcExternalPostProcessorIntegrationTest {

    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();
    private static Configuration configuration = new Configuration();
    private int port;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {
        String streams = "[{\"TOPIC_NAMES\":\"SG_GO_CAR-booking-log\",\"TABLE_NAME\":\"booking\",\"PROTO_CLASS_NAME\":\"io.odpf.dagger.booking.BookingLogMessage\",\"EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"10.200.216.49:6668,10.200.219.198:6668,10.200.216.58:6668,10.200.216.54:6668,10.200.216.56:6668,10.200.216.63:6668\",\"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-config\",\"STREAM_NAME\":\"p-godata-id-mainstream\"}]";
        configuration.setString(POST_PROCESSOR_ENABLED_KEY, "true");
        configuration.setString(INPUT_STREAMS, streams);
        configuration.setString(STENCIL_ENABLE_KEY, "false");

        GrpcMock.configureFor(GrpcMock.grpcMock(0).build().start());
        port = GrpcMock.getGlobalPort();
    }

    @Test
    public void shouldPopulateFieldFromGrpcOnSuccess() throws Exception {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"grpc\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"localhost\",\n" +
                "        \"service_port\": "+ port +",\n" +
                "        \"request_pattern\": \"{'field1': '%s'}\",\n" +
                "        \"request_variables\": \"order_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": false,\n" +
                "        \"retain_response_type\": true,\n" +
                "        \"grpc_stencil_url\": \"http://localhost:8000/messages.desc\",\n" +
                "        \"grpc_request_proto_schema\": \"io.odpf.dagger.consumer.TestGrpcRequest\",\n" +
                "        \"grpc_response_proto_schema\": \"io.odpf.dagger.consumer.TestGrpcResponse\",\n" +
                "        \"grpc_method_url\": \"io.odpf.dagger.consumer.TestServer/TestRpcMethod\",\n" +
                "        \"capacity\": \"30\",\n" +
                "        \"headers\": {\n" +
                "           \"content-type\": \"application/json\" \n" +
                "          }, \n" +
                "        \"output_mapping\": {\n" +
                "          \"field3\": {\n" +
                "            \"path\": \"$.field3\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  } \n" +
                "}";


        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        TestGrpcRequest matchRequest = TestGrpcRequest.newBuilder()
                .setField1("RB-123")
                .build();
        TestGrpcResponse response = TestGrpcResponse.newBuilder()
                .setField3("Grpc Response Success")
                .build();

        stubFor(unaryMethod(TestServerGrpc.getTestRpcMethodMethod())
                .withRequest(matchRequest)
                .willReturn(response));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("Grpc Response Success", CollectSink.values.get(0).getField(0));
    }


    @Test
    public void shouldPopulateFieldFromGrpcOnSuccessWithExternalAndInternalSource() throws Exception {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"grpc\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"localhost\" ,\n" +
                "        \"service_port\": "+ port +",\n" +
                "        \"request_pattern\": \"{'field1': '%s'}\",\n" +
                "        \"request_variables\": \"order_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": false,\n" +
                "        \"retain_response_type\": true,\n" +
                "        \"grpc_stencil_url\": \"http://localhost:8000/messages.desc\",\n" +
                "        \"grpc_request_proto_schema\": \"io.odpf.dagger.consumer.TestGrpcRequest\",\n" +
                "        \"grpc_response_proto_schema\": \"io.odpf.dagger.consumer.TestGrpcResponse\",\n" +
                "        \"grpc_method_url\": \"io.odpf.dagger.consumer.TestServer/TestRpcMethod\",\n" +
                "        \"capacity\": \"30\",\n" +
                "        \"output_mapping\": {\n" +
                "          \"field3\": {\n" +
                "            \"path\": \"$.field3\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }, \n" +
                "  \"internal_source\": [\n" +
                "    {\n" +
                "      \"output_field\": \"event_timestamp\",\n" +
                "      \"type\": \"function\",\n" +
                "      \"value\": \"CURRENT_TIMESTAMP\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"output_field\": \"order_id\",\n" +
                "      \"type\": \"sql\",\n" +
                "      \"value\": \"order_id\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"output_field\": \"customer_id\",\n" +
                "      \"type\": \"sql\",\n" +
                "      \"value\": \"customer_id\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        TestGrpcRequest matchRequest = TestGrpcRequest.newBuilder()
                .setField1("RB-123")
                .build();
        TestGrpcResponse response = TestGrpcResponse.newBuilder()
                .setField3("Grpc Response Success")
                .build();

        stubFor(unaryMethod(TestServerGrpc.getTestRpcMethodMethod())
                .withRequest(matchRequest)
                .willReturn(response));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
        assertEquals("Grpc Response Success", CollectSink.values.get(0).getField(0));
        assertEquals("RB-123", CollectSink.values.get(0).getField(2));
        assertEquals("123", CollectSink.values.get(0).getField(3));
    }

    @Test
    public void shouldPopulateFieldFromGrpcOnSuccessWithAllThreeSourcesIncludingTransformer() throws Exception {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"grpc\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"localhost\" ,\n" +
                "        \"service_port\": "+ port +",\n" +
                "        \"request_pattern\": \"{'field1': '%s'}\",\n" +
                "        \"request_variables\": \"order_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": false,\n" +
                "        \"retain_response_type\": true,\n" +
                "        \"grpc_stencil_url\": \"http://localhost:8000/messages.desc\",\n" +
                "        \"grpc_request_proto_schema\": \"io.odpf.dagger.consumer.TestGrpcRequest\",\n" +
                "        \"grpc_response_proto_schema\": \"io.odpf.dagger.consumer.TestGrpcResponse\",\n" +
                "        \"grpc_method_url\": \"io.odpf.dagger.consumer.TestServer/TestRpcMethod\",\n" +
                "        \"capacity\": \"30\",\n" +
                "        \"output_mapping\": {\n" +
                "          \"field3\": {\n" +
                "            \"path\": \"$.field3\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }, \n" +
                "  \"internal_source\": [\n" +
                "    {\n" +
                "      \"output_field\": \"event_timestamp\",\n" +
                "      \"type\": \"function\",\n" +
                "      \"value\": \"CURRENT_TIMESTAMP\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"output_field\": \"order_id\",\n" +
                "      \"type\": \"sql\",\n" +
                "      \"value\": \"order_id\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"output_field\": \"customer_id\",\n" +
                "      \"type\": \"sql\",\n" +
                "      \"value\": \"customer_id\"\n" +
                "    }\n" +
                "  ], \n" +
                "   \"transformers\": [" +
                "     {\n" +
                "       \"transformation_class\": \"com.gojek.dagger.transformer.ClearColumnTransformer\",\n" +
                "       \"transformation_arguments\": {\n" +
                "         \"targetColumnName\": \"customer_id\"\n" +
                "       }\n" +
                "     }\n" +
                "   ] \n" +
                " }";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        TestGrpcRequest matchRequest = TestGrpcRequest.newBuilder()
                .setField1("RB-123")
                .build();
        TestGrpcResponse response = TestGrpcResponse.newBuilder()
                .setField3("Grpc Response Success")
                .build();

        stubFor(unaryMethod(TestServerGrpc.getTestRpcMethodMethod())
                .withRequest(matchRequest)
                .willReturn(response));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
        assertEquals("Grpc Response Success", CollectSink.values.get(0).getField(0));
        assertEquals("RB-123", CollectSink.values.get(0).getField(2));
        assertEquals("", CollectSink.values.get(0).getField(3));
    }

    private static class CollectSink implements SinkFunction<Row> {

        static final List<Row> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Row inputRow, Context context) {
            values.add(inputRow);
        }
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        StreamInfo postProcessedStream = streamInfo;
        for (PostProcessor postProcessor : postProcessors) {
            postProcessedStream = postProcessor.process(postProcessedStream);
        }
        return postProcessedStream;
    }
}
