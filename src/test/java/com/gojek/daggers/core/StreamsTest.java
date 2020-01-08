package com.gojek.daggers.core;

import com.gojek.de.stencil.StencilClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class StreamsTest {

    @Test
    public void shouldTakeAJSONArrayWithSingleObject() {
        String configString = "[\n"
                + "        {\n"
                + "            \"EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"p-esb-kafka-mirror-b-01:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"com.gojek.esb.booking.BookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
                + "        }\n"
                + "]";

        Configuration configuration = new Configuration();
        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", StencilClientFactory.getClient(), false, 0);
        Map<String, FlinkKafkaConsumer011<Row>> mapOfStreams = streams.getStreams();
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
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"p-esb-kafka-mirror-b-01:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"com.gojek.esb.booking.BookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
                + "        }\n"
                + "]";

        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("GO_RIDE-booking-log");
        ArrayList<String> protoName = new ArrayList<>();
        protoName.add("com.gojek.esb.booking.BookingLogMessage");
        ArrayList<String> streamName = new ArrayList<>();
        streamName.add("");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("input_topic", topicNames);
        metrics.put("input_proto", protoName);
        metrics.put("input_stream", streamName);

        System.out.println(metrics);

        Configuration configuration = new Configuration();
        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", StencilClientFactory.getClient(), false, 0);
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
                + "            \"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"p-esb-kafka-mirror-b-01:6667\",\n"
                + "            \"KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
                + "            \"PROTO_CLASS_NAME\": \"com.gojek.esb.booking.BookingLogMessage\",\n"
                + "            \"TABLE_NAME\": \"data_stream\",\n"
                + "            \"TOPIC_NAMES\": \"GO_RIDE-booking-log\"\n"
                + "        }\n"
                + "]";

        LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
        protoClassForTable.put("data_stream", "com.gojek.esb.booking.BookingLogMessage");

        Configuration configuration = new Configuration();
        configuration.setString("STREAMS", configString);
        Streams streams = new Streams(configuration, "rowtime", StencilClientFactory.getClient(), false, 0);

        Assert.assertEquals(protoClassForTable, streams.getProtos());
    }
}
