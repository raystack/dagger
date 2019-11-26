package com.gojek.daggers.core;

import com.gojek.dagger.udf.*;
import com.gojek.daggers.source.KafkaProtoStreamingTableSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class StreamManagerTest {

    private StreamManager streamManager;

    private String jsonArray = "[\n"
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

    @Mock
    private StreamExecutionEnvironment env;

    @Mock
    private Configuration configuration;

    @Mock
    private ExecutionConfig executionConfig;

    @Mock
    private TableConfig tableConfig;

    @Mock
    private CheckpointConfig checkpointConfig;

    @Mock
    private StreamTableEnvironment tableEnvironment;

    @Mock
    private DataStream<Row> dataStream;

    @Before
    public void setup() {

        initMocks(this);
        when(configuration.getInteger("PARALLELISM", 1)).thenReturn(1);
        when(configuration.getInteger("WATERMARK_INTERVAL_MS", 10000)).thenReturn(10000);
        when(configuration.getLong("CHECKPOINT_INTERVAL", 30000)).thenReturn(30000L);
        when(configuration.getLong("CHECKPOINT_TIMEOUT", 900000)).thenReturn(900000L);
        when(configuration.getLong("CHECKPOINT_MIN_PAUSE", 5000)).thenReturn(5000L);
        when(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1)).thenReturn(1);
        when(configuration.getString("ROWTIME_ATTRIBUTE_NAME", "")).thenReturn("");
        when(configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false)).thenReturn(false);
        when(configuration.getLong("WATERMARK_DELAY_MS", 10000)).thenReturn(10000L);
        when(configuration.getString("STREAMS", "")).thenReturn(jsonArray);
        when(configuration.getBoolean("ENABLE_STENCIL_URL", false)).thenReturn(false);
        when(configuration.getString("STENCIL_URL", "")).thenReturn("");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink job")).thenReturn("SQL Flink job");
        when(configuration.getString("SINK_TYPE", "influx")).thenReturn("influx");
        when(configuration.getString("SQL_QUERY", "")).thenReturn("");
        when(configuration.getInteger("MIN_IDLE_STATE_RETENTION_TIME", 8)).thenReturn(8);
        when(configuration.getInteger("MAX_IDLE_STATE_RETENTION_TIME", 9)).thenReturn(9);
        when(env.getConfig()).thenReturn(executionConfig);
        when(env.getCheckpointConfig()).thenReturn(checkpointConfig);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);

        streamManager = new StreamManager(configuration, env, tableEnvironment);
    }

    @Test
    public void shouldRegisterRequiredConfigsOnExecutionEnvironement() {

        streamManager.registerConfigs();

        verify(env, Mockito.times(1)).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        verify(env, Mockito.times(1)).setParallelism(1);
        verify(env, Mockito.times(1)).enableCheckpointing(30000);
        verify(executionConfig, Mockito.times(1)).setAutoWatermarkInterval(10000);
        verify(executionConfig, Mockito.times(1)).setGlobalJobParameters(configuration);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointTimeout(900000L);
        verify(checkpointConfig, Mockito.times(1)).setMinPauseBetweenCheckpoints(5000L);
        verify(checkpointConfig, Mockito.times(1)).setMaxConcurrentCheckpoints(1);
    }

    @Test
    public void shouldRegisterSource() {
        streamManager.registerConfigs();
        streamManager.registerSource();

        verify(tableEnvironment, Mockito.times(1)).registerTableSource(eq("data_stream"), any(KafkaProtoStreamingTableSource.class));
    }

    @Test
    public void shouldRegisterFunctions() {
        streamManager.registerConfigs();
        streamManager.registerSource();
        streamManager.registerFunctions();

        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("S2Id"), any(S2Id.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("GEOHASH"), any(GeoHash.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("ElementAt"), any(ElementAt.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("ServiceArea"), any(ServiceArea.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("ServiceAreaId"), any(ServiceAreaId.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("DistinctCount"), any(DistinctCount.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("DistinctByCurrentStatus"), any(DistinctByCurrentStatus.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("Distance"), any(Distance.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("AppBetaUsers"), any(AppBetaUsers.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("KeyValue"), any(KeyValue.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("DartContains"), any(DartContains.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("DartGet"), any(DartGet.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("Features"), any(Features.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("TimestampFromUnix"), any(TimestampFromUnix.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("ConcurrentTransactions"), any(ConcurrentTransactions.class));
        verify(tableEnvironment, Mockito.times(1)).registerFunction(eq("SecondsElapsed"), any(SecondsElapsed.class));
    }

    @Test
    public void shouldCreateOutputStream() {
        StreamManagerStub streamManagerStub = new StreamManagerStub(configuration, env, tableEnvironment, new StreamInfo(dataStream, new String[]{}));
        streamManagerStub.registerOutputStream();

        verify(tableEnvironment, Mockito.times(1)).sqlQuery("");
    }

    @Test
    public void shouldExecuteJob() throws Exception {
        streamManager.execute();

        verify(env, Mockito.times(1)).execute("SQL Flink job");
    }

    class StreamManagerStub extends StreamManager {

        private StreamInfo streamInfo;

        public StreamManagerStub(Configuration configuration, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, StreamInfo streamInfo) {
            super(configuration, executionEnvironment, tableEnvironment);
            this.streamInfo = streamInfo;
        }

        @Override
        protected StreamInfo createStreamInfo(Table table) {
            return streamInfo;
        }

    }
}