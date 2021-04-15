package io.odpf.dagger.core;

import io.odpf.dagger.source.FlinkKafkaConsumerCustom;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.utils.Constants.*;
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
        configuration.setString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        configuration.setString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        configuration.setBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT);
        configuration.setString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT);

        stencilClient = StencilClientFactory.getClient();
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
    }

    @Test
    public void shouldTakeAJSONArrayWithSingleObject() {
        String configString = "[\n"
                + "        {\n"
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
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
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
                + "        }\n"
                + "]";

        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("GO_RIDE-booking-log");
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
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"STREAM_NAME\": \"mainstream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"1\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"io.odpf.dagger.consumer.TestBookingLogKey\",\n"
                + "            \"TABLE_NAME\": \"data_stream_1\",\n"
                + "            \"STREAM_NAME\": \"locstream\",\n"
                + "            \"TOPIC_NAMES\": \"surge-s2idcluster-log\"\n"
                + "        }\n"
                + "]";

        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("GO_RIDE-booking-log");
        topicNames.add("surge-s2idcluster-log");
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
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"io.odpf.dagger.consumer.TestBookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
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
