//package io.odpf.dagger.core;
//
//import org.apache.flink.connector.kafka.source.KafkaSource;
//
//import com.gojek.de.stencil.StencilClientFactory;
//import com.gojek.de.stencil.client.StencilClient;
//import io.odpf.dagger.common.configuration.Configuration;
//import io.odpf.dagger.common.core.StencilClientOrchestrator;
//import io.odpf.dagger.core.streams.InputStreams;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//
//import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT;
//import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_ENABLE_KEY;
//import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT;
//import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY;
//import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT;
//import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_URLS_KEY;
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.when;
//import static org.mockito.MockitoAnnotations.initMocks;
//
//
//public class StreamsTest {
//
//    @Mock
//    private StencilClientOrchestrator stencilClientOrchestrator;
//
//    @Mock
//    private Configuration configuration;
//
//    private StencilClient stencilClient;
//
//    @Before
//    public void setup() {
//        initMocks(this);
//
//        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT);
//        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
//        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
//
//        stencilClient = StencilClientFactory.getClient();
//        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
//    }
//
//    @Test
//    public void shouldTakeAJSONArrayWithSingleObject() {
//        String configString = "[\n"
//                + "        {\n"
//                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
//                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
//                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
//                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
//                + "        }\n"
//                + "]";
//
//        when(configuration.getString("STREAMS", "")).thenReturn(configString);
//        InputStreams streams = new InputStreams(configuration, "rowtime", stencilClientOrchestrator);
//        Map<String, KafkaSource> mapOfStreams = streams.getKafkaSource();
//        assertEquals(1, mapOfStreams.size());
//        assertEquals("data_stream", mapOfStreams.keySet().toArray()[0]);
//    }
//
//    @Test
//    public void shouldAddTopicsStreamsAndProtosToMetrics() {
//        String configString = "[\n"
//                + "        {\n"
//                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
//                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
//                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
//                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
//                + "        }\n"
//                + "]";
//
//        ArrayList<String> topicNames = new ArrayList<>();
//        topicNames.add("test-topic");
//        ArrayList<String> protoName = new ArrayList<>();
//        protoName.add("io.odpf.dagger.consumer.TestBookingLogMessage");
//        ArrayList<String> streamName = new ArrayList<>();
//        streamName.add("");
//        HashMap<String, List<String>> metrics = new HashMap<>();
//        metrics.put("input_topic", topicNames);
//        metrics.put("input_proto", protoName);
//        metrics.put("input_stream", streamName);
//
//        System.out.println(metrics);
//
//        when(configuration.getString("STREAMS", "")).thenReturn(configString);
//        InputStreams streams = new InputStreams(configuration, "rowtime", stencilClientOrchestrator);
//        streams.preProcessBeforeNotifyingSubscriber();
//        Map<String, List<String>> telemetry = streams.getTelemetry();
//
//        assertEquals(metrics, telemetry);
//    }
//
//    @Test
//    public void shouldAddTopicsStreamsAndProtosToMetricsInCaseOfJoins() {
//        String configString = "[\n"
//                + "        {\n"
//                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
//                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
//                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
//                + "            \"SOURCE_KAFKA_NAME\": \"mainstream\",\n"
//                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
//                + "        },\n"
//                + "        {\n"
//                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"1\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
//                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogKey\",\n"
//                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream_1\",\n"
//                + "            \"SOURCE_KAFKA_NAME\": \"locstream\",\n"
//                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
//                + "        }\n"
//                + "]";
//
//        ArrayList<String> topicNames = new ArrayList<>();
//        topicNames.add("test-topic");
//        topicNames.add("test-topic");
//        ArrayList<String> protoName = new ArrayList<>();
//        protoName.add("io.odpf.dagger.consumer.TestBookingLogMessage");
//        protoName.add("io.odpf.dagger.consumer.TestBookingLogKey");
//        ArrayList<String> streamName = new ArrayList<>();
//        streamName.add("mainstream");
//        streamName.add("locstream");
//        HashMap<String, List<String>> metrics = new HashMap<>();
//        metrics.put("input_topic", topicNames);
//        metrics.put("input_proto", protoName);
//        metrics.put("input_stream", streamName);
//
//        System.out.println(metrics);
//
//        when(configuration.getString("STREAMS", "")).thenReturn(configString);
//        InputStreams streams = new InputStreams(configuration, "rowtime", stencilClientOrchestrator);
//        streams.preProcessBeforeNotifyingSubscriber();
//        Map<String, List<String>> telemetry = streams.getTelemetry();
//
//        assertEquals(metrics, telemetry);
//    }
//
//    @Test
//    public void shouldReturnProtoClassName() {
//        String configString = "[\n"
//                + "        {\n"
//                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
//                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
//                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
//                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
//                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
//                + "        }\n"
//                + "]";
//
//        LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
//        protoClassForTable.put("data_stream", "io.odpf.dagger.consumer.TestBookingLogMessage");
//
//        when(configuration.getString("STREAMS", "")).thenReturn(configString);
//        InputStreams streams = new InputStreams(configuration, "rowtime", stencilClientOrchestrator);
//
//        assertEquals(protoClassForTable, streams.getProtos());
//    }
//}
