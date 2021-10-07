package io.odpf.dagger.core;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.UserConfiguration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.udfs.UdfFactory;
import io.odpf.dagger.core.exception.UDFFactoryClassNotDefinedException;
import io.odpf.dagger.core.processors.PostProcessorFactory;
import io.odpf.dagger.core.processors.PreProcessorConfig;
import io.odpf.dagger.core.processors.PreProcessorFactory;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.types.Preprocessor;
import io.odpf.dagger.core.sink.SinkOrchestrator;
import io.odpf.dagger.core.utils.Constants;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;

import static io.odpf.dagger.core.utils.Constants.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * The Stream manager.
 */
public class StreamManager {

    private StencilClientOrchestrator stencilClientOrchestrator;
    private final UserConfiguration userConf;
    private final StreamExecutionEnvironment executionEnvironment;
    private StreamTableEnvironment tableEnvironment;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();

    /**
     * Instantiates a new Stream manager.
     *
     * @param userConf             the configuration in form of param
     * @param executionEnvironment the execution environment
     * @param tableEnvironment     the table environment
     */
    public StreamManager(UserConfiguration userConf, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment) {
        this.userConf = userConf;
        this.executionEnvironment = executionEnvironment;
        this.tableEnvironment = tableEnvironment;
    }

    /**
     * Register configs stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerConfigs() {
        stencilClientOrchestrator = new StencilClientOrchestrator(userConf);
        executionEnvironment.setMaxParallelism(userConf.getParam().getInt(Constants.FLINK_PARALLELISM_MAX_KEY, Constants.FLINK_PARALLELISM_MAX_DEFAULT));
        executionEnvironment.setParallelism(userConf.getParam().getInt(FLINK_PARALLELISM_KEY, FLINK_PARALLELISM_DEFAULT));
        executionEnvironment.getConfig().setAutoWatermarkInterval(userConf.getParam().getInt(FLINK_WATERMARK_INTERVAL_MS_KEY, FLINK_WATERMARK_INTERVAL_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        executionEnvironment.enableCheckpointing(userConf.getParam().getLong(FLINK_CHECKPOINT_INTERVAL_MS_KEY, FLINK_CHECKPOINT_INTERVAL_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(userConf.getParam().getLong(FLINK_CHECKPOINT_TIMEOUT_MS_KEY, FLINK_CHECKPOINT_TIMEOUT_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(userConf.getParam().getLong(FLINK_CHECKPOINT_MIN_PAUSE_MS_KEY, FLINK_CHECKPOINT_MIN_PAUSE_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(userConf.getParam().getInt(FLINK_CHECKPOINT_MAX_CONCURRENT_KEY, FLINK_CHECKPOINT_MAX_CONCURRENT_DEFAULT));
        executionEnvironment.getConfig().setGlobalJobParameters(userConf.getParam());

        // TODO : change the time duration accordingly
        tableEnvironment.getConfig().setIdleStateRetentionTime(Time.hours(userConf.getParam().getInt(FLINK_RETENTION_MIN_IDLE_STATE_HOUR_KEY, FLINK_RETENTION_MIN_IDLE_STATE_HOUR_DEFAULT)),
                Time.hours(userConf.getParam().getInt(FLINK_RETENTION_MAX_IDLE_STATE_HOUR_KEY, FLINK_RETENTION_MAX_IDLE_STATE_HOUR_DEFAULT)));

        return this;
    }


    /**
     * Register source with pre processors stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerSourceWithPreProcessors() {
        long watermarkDelay = userConf.getParam().getLong(FLINK_WATERMARK_DELAY_MS_KEY, FLINK_WATERMARK_DELAY_MS_DEFAULT);
        Streams kafkaStreams = getKafkaStreams();
        kafkaStreams.notifySubscriber(telemetryExporter);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(userConf);
        kafkaStreams.getStreams().forEach((tableName, kafkaConsumer) -> {
            DataStream<Row> kafkaStream = executionEnvironment.addSource(kafkaConsumer);
            // TODO : generifying all watermark delay things
            SingleOutputStreamOperator<Row> rowSingleOutputStreamOperator = kafkaStream
                    .assignTimestampsAndWatermarks(WatermarkStrategy.
                            <Row>forBoundedOutOfOrderness(Duration.ofMillis(watermarkDelay))
                            .withTimestampAssigner((SerializableTimestampAssigner<Row>) (element, recordTimestamp) -> {
                                int index = element.getArity() - 1;
                                return ((Timestamp) element.getField(index)).getTime();
                            }));

            TableSchema tableSchema = TableSchema.fromTypeInfo(kafkaStream.getType());

            StreamInfo streamInfo = new StreamInfo(rowSingleOutputStreamOperator, tableSchema.getFieldNames());
            streamInfo = addPreProcessor(streamInfo, tableName, preProcessorConfig);

            // TODO : The schema thing is deprecated in 1.14, handle this deprecation in the serialization story
            Table table = tableEnvironment.fromDataStream(streamInfo.getDataStream(), getApiExpressions(streamInfo));
            tableEnvironment.createTemporaryView(tableName, table);
        });
        return this;
    }

    private ApiExpression[] getApiExpressions(StreamInfo streamInfo) {
        String[] columnNames = streamInfo.getColumnNames();
        ApiExpression[] expressions = new ApiExpression[columnNames.length];
        if (columnNames.length == 0) {
            return expressions;
        }
        for (int i = 0; i < columnNames.length - 1; i++) {
            ApiExpression expression = $(columnNames[i]);
            expressions[i] = expression;
        }
        // TODO : fix according to the rowtime attribute name
        expressions[columnNames.length - 1] = $(ROWTIME).rowtime();
        return expressions;
    }


    /**
     * Register functions stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerFunctions() {
        String[] functionFactoryClasses = userConf.getParam()
                .get(Constants.FUNCTION_FACTORY_CLASSES_KEY, Constants.FUNCTION_FACTORY_CLASSES_DEFAULT)
                .split(",");

        for (String className : functionFactoryClasses) {
            try {
                UdfFactory udfFactory = getUdfFactory(className);
                udfFactory.registerFunctions();
            } catch (ReflectiveOperationException e) {
                throw new UDFFactoryClassNotDefinedException(e.getMessage());
            }
        }
        return this;
    }

    private UdfFactory getUdfFactory(String udfFactoryClassName) throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<?> udfFactoryClass = Class.forName(udfFactoryClassName);
        Constructor<?> udfFactoryClassConstructor = udfFactoryClass.getConstructor(StreamTableEnvironment.class, UserConfiguration.class);
        return (UdfFactory) udfFactoryClassConstructor.newInstance(tableEnvironment, userConf);
    }

    /**
     * Register output stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerOutputStream() {
        Table table = tableEnvironment.sqlQuery(userConf.getParam().get(Constants.FLINK_SQL_QUERY_KEY, Constants.FLINK_SQL_QUERY_DEFAULT));
        StreamInfo streamInfo = createStreamInfo(table);
        streamInfo = addPostProcessor(streamInfo);
        addSink(streamInfo);
        return this;
    }

    /**
     * Execute dagger job.
     *
     * @throws Exception the exception
     */
    public void execute() throws Exception {
        executionEnvironment.execute(userConf.getParam().get(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT));
    }

    /**
     * Create stream info.
     *
     * @param table the table
     * @return the stream info
     */
    protected StreamInfo createStreamInfo(Table table) {
        // TODO : Will fix this after fixing the serializations
        DataStream<Row> stream = tableEnvironment
                .toRetractStream(table, Row.class)
                .filter(value -> value.f0)
                .map(value -> value.f1);
        return new StreamInfo(stream, table.getSchema().getFieldNames());
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(userConf, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        for (PostProcessor postProcessor : postProcessors) {
            streamInfo = postProcessor.process(streamInfo);
        }
        return streamInfo;
    }

    private StreamInfo addPreProcessor(StreamInfo streamInfo, String tableName, PreProcessorConfig preProcessorConfig) {
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(userConf, preProcessorConfig, tableName, telemetryExporter);
        for (Preprocessor preprocessor : preProcessors) {
            streamInfo = preprocessor.process(streamInfo);
        }
        return streamInfo;
    }


    private void addSink(StreamInfo streamInfo) {
        SinkOrchestrator sinkOrchestrator = new SinkOrchestrator();
        sinkOrchestrator.addSubscriber(telemetryExporter);
        streamInfo.getDataStream().addSink(sinkOrchestrator.getSink(userConf, streamInfo.getColumnNames(), stencilClientOrchestrator));
    }

    private Streams getKafkaStreams() {
        String rowTimeAttributeName = userConf.getParam().get(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        Boolean enablePerPartitionWatermark = userConf.getParam().getBoolean(FLINK_WATERMARK_PER_PARTITION_ENABLE_KEY, FLINK_WATERMARK_PER_PARTITION_ENABLE_DEFAULT);
        Long watermarkDelay = userConf.getParam().getLong(FLINK_WATERMARK_DELAY_MS_KEY, FLINK_WATERMARK_DELAY_MS_DEFAULT);
        return new Streams(userConf, rowTimeAttributeName, stencilClientOrchestrator, enablePerPartitionWatermark, watermarkDelay);
    }
}
