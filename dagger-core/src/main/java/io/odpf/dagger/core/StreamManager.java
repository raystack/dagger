package io.odpf.dagger.core;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.odpf.dagger.core.utils.Constants.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * The Stream manager.
 */
public class StreamManager {

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private StreamExecutionEnvironment executionEnvironment;
    private StreamTableEnvironment tableEnvironment;
    private Streams kafkaStreams;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();

    /**
     * Instantiates a new Stream manager.
     *
     * @param configuration        the configuration
     * @param executionEnvironment the execution environment
     * @param tableEnvironment     the table environment
     */
    public StreamManager(Configuration configuration, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment) {
        this.configuration = configuration;
        this.executionEnvironment = executionEnvironment;
        this.tableEnvironment = tableEnvironment;
    }

    /**
     * Register configs stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerConfigs() {
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        executionEnvironment.setMaxParallelism(configuration.getInteger(Constants.FLINK_PARALLELISM_MAX_KEY, Constants.FLINK_PARALLELISM_MAX_DEFAULT));
        executionEnvironment.setParallelism(configuration.getInteger(FLINK_PARALLELISM_KEY, FLINK_PARALLELISM_DEFAULT));
        executionEnvironment.getConfig().setAutoWatermarkInterval(configuration.getInteger(FLINK_WATERMARK_INTERVAL_MS_KEY, FLINK_WATERMARK_INTERVAL_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        executionEnvironment.enableCheckpointing(configuration.getLong(FLINK_CHECKPOINT_INTERVAL_MS_KEY, FLINK_CHECKPOINT_INTERVAL_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(configuration.getLong(FLINK_CHECKPOINT_TIMEOUT_MS_KEY, FLINK_CHECKPOINT_TIMEOUT_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(configuration.getLong(FLINK_CHECKPOINT_MIN_PAUSE_MS_KEY, FLINK_CHECKPOINT_MIN_PAUSE_MS_DEFAULT));
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(configuration.getInteger(FLINK_CHECKPOINT_MAX_CONCURRENT_KEY, FLINK_CHECKPOINT_MAX_CONCURRENT_DEFAULT));
        executionEnvironment.getConfig().setGlobalJobParameters(configuration);

        tableEnvironment.getConfig().setIdleStateRetentionTime(Time.hours(configuration.getInteger(FLINK_RETENTION_MIN_IDLE_STATE_HOUR_KEY, FLINK_RETENTION_MIN_IDLE_STATE_HOUR_DEFAULT)),
                Time.hours(configuration.getInteger(FLINK_RETENTION_MAX_IDLE_STATE_HOUR_KEY, FLINK_RETENTION_MAX_IDLE_STATE_HOUR_DEFAULT)));
        return this;
    }


    /**
     * Register source with pre processors stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerSourceWithPreProcessors() {
        String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        Long watermarkDelay = configuration.getLong(FLINK_WATERMARK_DELAY_MS_KEY, FLINK_WATERMARK_DELAY_MS_DEFAULT);
        kafkaStreams = getKafkaStreams();
        kafkaStreams.notifySubscriber(telemetryExporter);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        kafkaStreams.getStreams().forEach((tableName, kafkaConsumer) -> {
            DataStream<Row> kafkaStream = executionEnvironment.addSource(kafkaConsumer);
            SingleOutputStreamOperator<Row> rowSingleOutputStreamOperator = kafkaStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(org.apache.flink.streaming.api.windowing.time.Time.of(watermarkDelay, TimeUnit.MILLISECONDS)) {
                @Override
                public long extractTimestamp(Row element) {
                    int index = element.getArity() - 1;
                    return ((Timestamp) element.getField(index)).getTime();
                }
            });

            TableSchema tableSchema = TableSchema.fromTypeInfo(kafkaStream.getType());

            StreamInfo streamInfo = new StreamInfo(rowSingleOutputStreamOperator, TableSchema.fromTypeInfo(kafkaStream.getType()).getFieldNames());
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
        for (int i = 0; i < columnNames.length - 1; i++) {
            ApiExpression expression = $(columnNames[i]);
            expressions[i] = expression;
        }
        expressions[columnNames.length - 1] = $(ROWTIME).rowtime();
        return expressions;
    }


    /**
     * Register functions stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerFunctions() {
        String[] functionFactoryClasses = configuration
                .getString(Constants.FUNCTION_FACTORY_CLASSES_KEY, Constants.FUNCTION_FACTORY_CLASSES_DEFAULT)
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
        Constructor udfFactoryClassConstructor = udfFactoryClass.getConstructor(StreamTableEnvironment.class, Configuration.class);
        return (UdfFactory) udfFactoryClassConstructor.newInstance(tableEnvironment, configuration);
    }

    /**
     * Register output stream manager.
     *
     * @return the stream manager
     */
    public StreamManager registerOutputStream() {
        Table table = tableEnvironment.sqlQuery(configuration.getString(Constants.FLINK_SQL_QUERY_KEY, Constants.FLINK_SQL_QUERY_DEFAULT));
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
        executionEnvironment.execute(configuration.getString(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT));
    }

    /**
     * Create stream info.
     *
     * @param table the table
     * @return the stream info
     */
    protected StreamInfo createStreamInfo(Table table) {
        DataStream<Row> stream = tableEnvironment
                .toRetractStream(table, Row.class)
                .filter(value -> value.f0)
                .map(value -> value.f1);
        return new StreamInfo(stream, table.getSchema().getFieldNames());
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        for (PostProcessor postProcessor : postProcessors) {
            streamInfo = postProcessor.process(streamInfo);
        }
        return streamInfo;
    }

    private StreamInfo addPreProcessor(StreamInfo streamInfo, String tableName, PreProcessorConfig preProcessorConfig) {
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(configuration, preProcessorConfig, tableName, telemetryExporter);
        for (Preprocessor preprocessor : preProcessors) {
            streamInfo = preprocessor.process(streamInfo);
        }
        return streamInfo;
    }


    private void addSink(StreamInfo streamInfo) {
        SinkOrchestrator sinkOrchestrator = new SinkOrchestrator();
        sinkOrchestrator.addSubscriber(telemetryExporter);
        streamInfo.getDataStream().addSink(sinkOrchestrator.getSink(configuration, streamInfo.getColumnNames(), stencilClientOrchestrator));
    }

    private Streams getKafkaStreams() {
        String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        Boolean enablePerPartitionWatermark = configuration.getBoolean(FLINK_WATERMARK_PER_PARTITION_ENABLE_KEY, FLINK_WATERMARK_PER_PARTITION_ENABLE_DEFAULT);
        Long watermarkDelay = configuration.getLong(FLINK_WATERMARK_DELAY_MS_KEY, FLINK_WATERMARK_DELAY_MS_DEFAULT);
        return new Streams(configuration, rowTimeAttributeName, stencilClientOrchestrator, enablePerPartitionWatermark, watermarkDelay);
    }
}
