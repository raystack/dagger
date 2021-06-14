package io.odpf.dagger.core;

import io.odpf.dagger.common.core.StreamInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import io.odpf.dagger.core.source.CustomStreamingTableSource;
import io.odpf.dagger.core.source.FlinkKafkaConsumerCustom;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


@PrepareForTest(TableSchema.class)
@RunWith(PowerMockRunner.class)
public class StreamManagerTest {

    private StreamManager streamManager;

    private String jsonArray = "[\n"
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

    @Mock
    private DataStreamSource<Row> source;

    @Mock
    private TypeInformation<Row> typeInformation;

    @Mock
    private TableSchema schema;

    @Before
    public void setup() {

        initMocks(this);
        when(configuration.getInteger("FLINK_PARALLELISM", 1)).thenReturn(1);
        when(configuration.getInteger("FLINK_WATERMARK_INTERVAL_MS", 10000)).thenReturn(10000);
        when(configuration.getLong("FLINK_CHECKPOINT_INTERVAL_MS", 30000)).thenReturn(30000L);
        when(configuration.getLong("FLINK_CHECKPOINT_TIMEOUT_MS", 900000)).thenReturn(900000L);
        when(configuration.getLong("FLINK_CHECKPOINT_MIN_PAUSE_MS", 5000)).thenReturn(5000L);
        when(configuration.getInteger("FLINK_CHECKPOINT_MAX_CONCURRENT", 1)).thenReturn(1);
        when(configuration.getString("FLINK_ROWTIME_ATTRIBUTE_NAME", "")).thenReturn("");
        when(configuration.getBoolean("FLINK_WATERMARK_PER_PARTITION_ENABLE", false)).thenReturn(false);
        when(configuration.getLong("FLINK_WATERMARK_DELAY_MS", 10000)).thenReturn(10000L);
        when(configuration.getString("STREAMS", "")).thenReturn(jsonArray);
        when(configuration.getBoolean("SCHEMA_REGISTRY_STENCIL_ENABLE", false)).thenReturn(false);
        when(configuration.getString("SCHEMA_REGISTRY_STENCIL_URLS", "")).thenReturn("");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink job")).thenReturn("SQL Flink job");
        when(configuration.getString("SINK_TYPE", "influx")).thenReturn("influx");
        when(configuration.getString("FLINK_SQL_QUERY", "")).thenReturn("");
        when(configuration.getInteger("FLINK_RETENTION_MIN_IDLE_STATE_HOUR", 8)).thenReturn(8);
        when(configuration.getInteger("FLINK_RETENTION_MAX_IDLE_STATE_HOUR", 9)).thenReturn(9);
        when(env.getConfig()).thenReturn(executionConfig);
        when(env.getCheckpointConfig()).thenReturn(checkpointConfig);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(env.addSource(any(FlinkKafkaConsumerCustom.class))).thenReturn(source);
        when(source.getType()).thenReturn(typeInformation);
        when(typeInformation.getTypeClass()).thenReturn(Row.class);
        when(schema.getFieldNames()).thenReturn(new String[0]);
        PowerMockito.mockStatic(TableSchema.class);
        when(TableSchema.fromTypeInfo(typeInformation)).thenReturn(schema);
        streamManager = new StreamManager(configuration, env, tableEnvironment);
    }

    @Test
    public void shouldRegisterRequiredConfigsOnExecutionEnvironment() {

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
    public void shouldRegisterSourceWithPreprocessors() {
        streamManager.registerConfigs();
        streamManager.registerSourceWithPreProcessors();

        verify(env, Mockito.times(1)).addSource(any(FlinkKafkaConsumerCustom.class));
        verify(tableEnvironment, Mockito.times(1)).registerTableSource(eq("data_stream"), any(CustomStreamingTableSource.class));
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

    final class StreamManagerStub extends StreamManager {

        private StreamInfo streamInfo;

        private StreamManagerStub(Configuration configuration, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, StreamInfo streamInfo) {
            super(configuration, executionEnvironment, tableEnvironment);
            this.streamInfo = streamInfo;
        }

        @Override
        protected StreamInfo createStreamInfo(Table table) {
            return streamInfo;
        }
    }
}
