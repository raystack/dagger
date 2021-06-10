package io.odpf.dagger.core;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.source.FlinkKafkaConsumerCustom;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.common.core.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class StreamsTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    private StencilClient stencilClient;
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        configuration = new Configuration();
        configuration.setString(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT);
        configuration.setBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        configuration.setString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);

        stencilClient = StencilClientFactory.getClient();
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
    }

    @Test
    public void shouldTakeAJSONArrayWithSingleObject() {
        String configString = "[\n"
                + "        {\n"
                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
                + "        }\n"
                + "]";

        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", stencilClientOrchestrator, false, 0);
        Map<String, FlinkKafkaConsumerCustom> mapOfStreams = streams.getStreams();
        assertEquals(1, mapOfStreams.size());
        assertEquals("data_stream", mapOfStreams.keySet().toArray()[0]);
    }

    @Test
    public void shouldAddTopicsStreamsAndProtosToMetrics() {
        String configString = "[\n"
                + "        {\n"
                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
                + "        }\n"
                + "]";

        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("test-topic");
        ArrayList<String> protoName = new ArrayList<>();
        protoName.add("io.odpf.dagger.consumer.TestBookingLogMessage");
        ArrayList<String> streamName = new ArrayList<>();
        streamName.add("");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("input_topic", topicNames);
        metrics.put("input_proto", protoName);
        metrics.put("input_stream", streamName);

        System.out.println(metrics);

        configuration = new Configuration();
        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", stencilClientOrchestrator, false, 0);
        streams.preProcessBeforeNotifyingSubscriber();
        Map<String, List<String>> telemetry = streams.getTelemetry();

        assertEquals(metrics, telemetry);
    }

    @Test
    public void shouldAddTopicsStreamsAndProtosToMetricsInCaseOfJoins() {
        String configString = "[\n"
                + "        {\n"
                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
                + "            \"SOURCE_KAFKA_NAME\": \"mainstream\",\n"
                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"1\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogKey\",\n"
                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream_1\",\n"
                + "            \"SOURCE_KAFKA_NAME\": \"locstream\",\n"
                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
                + "        }\n"
                + "]";

        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("test-topic");
        topicNames.add("test-topic");
        ArrayList<String> protoName = new ArrayList<>();
        protoName.add("io.odpf.dagger.consumer.TestBookingLogMessage");
        protoName.add("io.odpf.dagger.consumer.TestBookingLogKey");
        ArrayList<String> streamName = new ArrayList<>();
        streamName.add("mainstream");
        streamName.add("locstream");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("input_topic", topicNames);
        metrics.put("input_proto", protoName);
        metrics.put("input_stream", streamName);

        System.out.println(metrics);

        configuration = new Configuration();
        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", stencilClientOrchestrator, false, 0);
        streams.preProcessBeforeNotifyingSubscriber();
        Map<String, List<String>> telemetry = streams.getTelemetry();

        assertEquals(metrics, telemetry);
    }

    @Test
    public void shouldReturnProtoClassName() {
        String configString = "[\n"
                + "        {\n"
                + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
                + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
                + "        }\n"
                + "]";

        LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
        protoClassForTable.put("data_stream", "io.odpf.dagger.consumer.TestBookingLogMessage");

        configuration = new Configuration();
        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", stencilClientOrchestrator, false, 0);

        Assert.assertEquals(protoClassForTable, streams.getProtos());
    }
}
