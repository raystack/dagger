package io.odpf.dagger.core;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.UserConfiguration;
import io.odpf.dagger.common.core.StreamInfo;
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
    private ParameterTool parameter;

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
    private SingleOutputStreamOperator<Row> singleOutputStream;

    @Mock
    private DataStreamSource<Row> source;

    @Mock
    private TypeInformation<Row> typeInformation;

    @Mock
    private TableSchema schema;

    private UserConfiguration userConfiguration;

    @Before
    public void setup() {

        initMocks(this);
        when(parameter.getInt("FLINK_PARALLELISM", 1)).thenReturn(1);
        when(parameter.getInt("FLINK_WATERMARK_INTERVAL_MS", 10000)).thenReturn(10000);
        when(parameter.getLong("FLINK_CHECKPOINT_INTERVAL_MS", 30000)).thenReturn(30000L);
        when(parameter.getLong("FLINK_CHECKPOINT_TIMEOUT_MS", 900000)).thenReturn(900000L);
        when(parameter.getLong("FLINK_CHECKPOINT_MIN_PAUSE_MS", 5000)).thenReturn(5000L);
        when(parameter.getInt("FLINK_CHECKPOINT_MAX_CONCURRENT", 1)).thenReturn(1);
        when(parameter.get("FLINK_ROWTIME_ATTRIBUTE_NAME", "")).thenReturn("");
        when(parameter.getBoolean("FLINK_WATERMARK_PER_PARTITION_ENABLE", false)).thenReturn(false);
        when(parameter.getLong("FLINK_WATERMARK_DELAY_MS", 10000)).thenReturn(10000L);
        when(parameter.get("STREAMS", "")).thenReturn(jsonArray);
        when(parameter.getBoolean("SCHEMA_REGISTRY_STENCIL_ENABLE", false)).thenReturn(false);
        when(parameter.get("SCHEMA_REGISTRY_STENCIL_URLS", "")).thenReturn("");
        when(parameter.get("FLINK_JOB_ID", "SQL Flink job")).thenReturn("SQL Flink job");
        when(parameter.get("SINK_TYPE", "influx")).thenReturn("influx");
        when(parameter.get("FLINK_SQL_QUERY", "")).thenReturn("");
        when(parameter.getInt("FLINK_RETENTION_MIN_IDLE_STATE_HOUR", 8)).thenReturn(8);
        when(parameter.getInt("FLINK_RETENTION_MAX_IDLE_STATE_HOUR", 9)).thenReturn(9);
        when(env.getConfig()).thenReturn(executionConfig);
        when(env.getCheckpointConfig()).thenReturn(checkpointConfig);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(env.addSource(any(FlinkKafkaConsumerCustom.class))).thenReturn(source);
        when(source.getType()).thenReturn(typeInformation);
        when(typeInformation.getTypeClass()).thenReturn(Row.class);
        when(schema.getFieldNames()).thenReturn(new String[0]);
        PowerMockito.mockStatic(TableSchema.class);
        this.userConfiguration = new UserConfiguration(parameter);
        when(TableSchema.fromTypeInfo(typeInformation)).thenReturn(schema);
        streamManager = new StreamManager(userConfiguration, env, tableEnvironment);
    }

    @Test
    public void shouldRegisterRequiredConfigsOnExecutionEnvironment() {

        streamManager.registerConfigs();

        verify(env, Mockito.times(1)).setParallelism(1);
        verify(env, Mockito.times(1)).enableCheckpointing(30000);
        verify(executionConfig, Mockito.times(1)).setAutoWatermarkInterval(10000);
        verify(executionConfig, Mockito.times(1)).setGlobalJobParameters(parameter);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointTimeout(900000L);
        verify(checkpointConfig, Mockito.times(1)).setMinPauseBetweenCheckpoints(5000L);
        verify(checkpointConfig, Mockito.times(1)).setMaxConcurrentCheckpoints(1);
    }

    @Test
    public void shouldRegisterSourceWithPreprocessorsWithWaterMarks() {
        when(env.addSource(any(FlinkKafkaConsumerCustom.class))).thenReturn(source);
        when(source.assignTimestampsAndWatermarks(any(WatermarkStrategy.class))).thenReturn(singleOutputStream);

        StreamManagerStub streamManagerStub = new StreamManagerStub(userConfiguration, env, tableEnvironment, new StreamInfo(dataStream, new String[]{}));
        streamManagerStub.registerConfigs();
        streamManagerStub.registerSourceWithPreProcessors();

        verify(tableEnvironment, Mockito.times(1)).fromDataStream(any(), new ApiExpression[]{});
    }

    @Test
    public void shouldCreateOutputStream() {
        StreamManagerStub streamManagerStub = new StreamManagerStub(userConfiguration, env, tableEnvironment, new StreamInfo(dataStream, new String[]{}));
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

        private StreamManagerStub(UserConfiguration userConfiguration, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment, StreamInfo streamInfo) {
            super(userConfiguration, executionEnvironment, tableEnvironment);
            this.streamInfo = streamInfo;
        }

        @Override
        protected StreamInfo createStreamInfo(Table table) {
            return streamInfo;
        }
    }
}
