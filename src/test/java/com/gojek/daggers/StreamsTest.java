package com.gojek.daggers;

import com.gojek.de.stencil.StencilClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class StreamsTest {
    @Test
    public void shouldTakeAJSONArrayWithSingleObject() {
        String jsonArray = "[\n"
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
        configuration.setString("STREAMS", jsonArray);
        Streams streams = new Streams(configuration, "rowtime", StencilClientFactory.getClient(), false, 0);
        Map<String, FlinkKafkaConsumer011<Row>> mapOfStreams = streams.getStreams();
        assertEquals(1, mapOfStreams.size());
        assertEquals("data_stream", mapOfStreams.keySet().toArray()[0]);
    }
}
