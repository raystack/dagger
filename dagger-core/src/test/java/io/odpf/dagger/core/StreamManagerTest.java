package io.odpf.dagger.core;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.source.Stream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

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
            + "            \"INPUT_SOURCE_TYPE\": \"KAFKA_SOURCE\",\n"
            + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
            + "        }\n"
            + "]";

    @Mock
    private StreamExecutionEnvironment env;

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

    @Mock
    private Configuration configuration;

    @Mock
    private Stream stream;

    @Mock
    private WatermarkStrategy<Row> watermarkStrategy;

    @Before
    public void setup() {

        initMocks(this);
        when(configuration.getInteger("FLINK_PARALLELISM", 1)).thenReturn(1);
        when(configuration.getInteger("FLINK_WATERMARK_INTERVAL_MS", 10000)).thenReturn(10000);
        when(configuration.getLong("FLINK_CHECKPOINT_INTERVAL_MS", 30000L)).thenReturn(30000L);
        when(configuration.getLong("FLINK_CHECKPOINT_TIMEOUT_MS", 900000L)).thenReturn(900000L);
        when(configuration.getLong("FLINK_CHECKPOINT_MIN_PAUSE_MS", 5000L)).thenReturn(5000L);
        when(configuration.getInteger("FLINK_CHECKPOINT_MAX_CONCURRENT", 1)).thenReturn(1);
        when(configuration.getString("FLINK_ROWTIME_ATTRIBUTE_NAME", "")).thenReturn("");
        when(configuration.getBoolean("FLINK_WATERMARK_PER_PARTITION_ENABLE", false)).thenReturn(false);
        when(configuration.getLong("FLINK_WATERMARK_DELAY_MS", 10000L)).thenReturn(10000L);
        when(configuration.getString("STREAMS", "")).thenReturn(jsonArray);
        when(configuration.getBoolean("SCHEMA_REGISTRY_STENCIL_ENABLE", false)).thenReturn(false);
        when(configuration.getString("SCHEMA_REGISTRY_STENCIL_URLS", "")).thenReturn("");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink job")).thenReturn("SQL Flink job");
        when(configuration.getString("SINK_TYPE", "influx")).thenReturn("influx");
        when(configuration.getString("FLINK_SQL_QUERY", "")).thenReturn("");
        when(configuration.getInteger("FLINK_RETENTION_IDLE_STATE_MINUTE", 10)).thenReturn(10);
        when(env.getConfig()).thenReturn(executionConfig);
        when(env.getCheckpointConfig()).thenReturn(checkpointConfig);
        when(tableEnvironment.getConfig()).thenReturn(tableConfig);
        when(env.fromSource(any(KafkaSource.class), any(WatermarkStrategy.class), any(String.class))).thenReturn(source);
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

        verify(env, Mockito.times(1)).setParallelism(1);
        verify(env, Mockito.times(1)).enableCheckpointing(30000);
        verify(executionConfig, Mockito.times(1)).setAutoWatermarkInterval(10000);
        verify(executionConfig, Mockito.times(1)).setGlobalJobParameters(configuration.getParam());
        verify(checkpointConfig, Mockito.times(1)).setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        verify(checkpointConfig, Mockito.times(1)).setCheckpointTimeout(900000L);
        verify(checkpointConfig, Mockito.times(1)).setMinPauseBetweenCheckpoints(5000L);
        verify(checkpointConfig, Mockito.times(1)).setMaxConcurrentCheckpoints(1);
        verify(tableConfig, Mockito.times(1)).setIdleStateRetention(Duration.ofMinutes(10));
    }

    @Test
    public void shouldRegisterSourceWithPreprocessorsWithWaterMarks() {
        when(singleOutputStream.assignTimestampsAndWatermarks(any(WatermarkStrategy.class))).thenReturn(singleOutputStream);
        when(stream.registerSource(any(StreamExecutionEnvironment.class), any(WatermarkStrategy.class), any(String.class))).thenReturn(singleOutputStream);
        when(singleOutputStream.getType()).thenReturn(typeInformation);
        when(stream.getStreamName()).thenReturn("data_stream");

        StreamManagerStub streamManagerStub = new StreamManagerStub(configuration, env, tableEnvironment, new StreamInfo(dataStream, new String[]{}));
        streamManagerStub.registerConfigs();
        streamManagerStub.registerSourceWithPreProcessors();

        verify(tableEnvironment, Mockito.times(1)).fromDataStream(any(), new ApiExpression[]{});
    }

    @Test
    public void shouldCreateValidSourceWithWatermarks() {
        when(singleOutputStream.assignTimestampsAndWatermarks(any(WatermarkStrategy.class))).thenReturn(singleOutputStream);
        when(stream.registerSource(any(StreamExecutionEnvironment.class), any(WatermarkStrategy.class), any(String.class))).thenReturn(singleOutputStream);
        when(singleOutputStream.getType()).thenReturn(typeInformation);
        when(stream.getStreamName()).thenReturn("data_stream");

        StreamManagerStub streamManagerStub = new StreamManagerStub(configuration, env, tableEnvironment, new StreamInfo(dataStream, new String[]{}));
        streamManagerStub.registerConfigs();
        streamManagerStub.registerSourceWithPreProcessors();

        verify(stream, Mockito.times(1)).registerSource(any(StreamExecutionEnvironment.class), any(WatermarkStrategy.class), any(String.class));
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

        @Override
        List<Stream> getStreams() {
            return Collections.singletonList(stream);
        }
    }
}
