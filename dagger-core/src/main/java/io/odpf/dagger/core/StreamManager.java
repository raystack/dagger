package io.odpf.dagger.core;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.udfs.UdfFactory;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.exception.UDFFactoryClassNotDefinedException;
import io.odpf.dagger.core.processors.PostProcessorFactory;
import io.odpf.dagger.core.processors.PreProcessorConfig;
import io.odpf.dagger.core.processors.PreProcessorFactory;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.types.Preprocessor;
import io.odpf.dagger.core.sink.SinkOrchestrator;
import io.odpf.dagger.core.source.CustomStreamingTableSource;
import io.odpf.dagger.core.utils.Constants;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static io.odpf.dagger.core.utils.Constants.*;

public class StreamManager {

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private StreamExecutionEnvironment executionEnvironment;
    private StreamTableEnvironment tableEnvironment;
    private Streams kafkaStreams;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();

    public StreamManager(Configuration configuration, StreamExecutionEnvironment executionEnvironment, StreamTableEnvironment tableEnvironment) {
        this.configuration = configuration;
        this.executionEnvironment = executionEnvironment;
        this.tableEnvironment = tableEnvironment;
    }

    public StreamManager registerConfigs() {
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        executionEnvironment.setMaxParallelism(configuration.getInteger(Constants.FLINK_PARALLELISM_MAX_KEY, Constants.FLINK_PARALLELISM_MAX_DEFAULT));
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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


    public StreamManager registerSourceWithPreProcessors() {
        String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        Boolean enablePerPartitionWatermark = configuration.getBoolean(FLINK_WATERMARK_PER_PARTITION_ENABLE_KEY, FLINK_WATERMARK_PER_PARTITION_ENABLE_DEFAULT);
        Long watermarkDelay = configuration.getLong(FLINK_WATERMARK_INTERVAL_DELAY_MS_KEY, FLINK_WATERMARK_INTERVAL_DELAY_MS_DEFAULT);
        kafkaStreams = getKafkaStreams();
        kafkaStreams.notifySubscriber(telemetryExporter);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        kafkaStreams.getStreams().forEach((tableName, kafkaConsumer) -> {
            DataStream<Row> kafkaStream = executionEnvironment.addSource(kafkaConsumer);
            StreamInfo streamInfo = new StreamInfo(kafkaStream, TableSchema.fromTypeInfo(kafkaStream.getType()).getFieldNames());
            streamInfo = addPreProcessor(streamInfo, tableName, preProcessorConfig);
            CustomStreamingTableSource tableSource = new CustomStreamingTableSource(
                    rowTimeAttributeName,
                    watermarkDelay,
                    enablePerPartitionWatermark,
                    streamInfo.getDataStream()
            );
            tableEnvironment.registerTableSource(tableName, tableSource);
        });
        return this;
    }

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

    public StreamManager registerOutputStream() {
        Table table = tableEnvironment.sqlQuery(configuration.getString(Constants.SQL_QUERY, Constants.SQL_QUERY_DEFAULT));
        StreamInfo streamInfo = createStreamInfo(table);
        streamInfo = addPostProcessor(streamInfo);
        addSink(streamInfo);
        return this;
    }

    public void execute() throws Exception {
        executionEnvironment.execute(configuration.getString(FLINK_JOB_ID_KEY, FLINK_JOB_ID_DEFAULT));
    }

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
        Long watermarkDelay = configuration.getLong(FLINK_WATERMARK_INTERVAL_DELAY_MS_KEY, FLINK_WATERMARK_INTERVAL_DELAY_MS_DEFAULT);
        return new Streams(configuration, rowTimeAttributeName, stencilClientOrchestrator, enablePerPartitionWatermark, watermarkDelay);
    }
}
