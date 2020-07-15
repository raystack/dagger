package com.gojek.daggers.core;

import com.gojek.dagger.udf.*;
import com.gojek.dagger.udf.gopay.fraud.RuleViolatedEventUnnest;
import com.gojek.daggers.metrics.telemetry.AggregatedUDFTelemetryPublisher;
import com.gojek.daggers.postProcessors.PostProcessorFactory;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.sink.SinkOrchestrator;
import com.gojek.daggers.source.KafkaProtoStreamingTableSource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gojek.daggers.utils.Constants.*;

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
        executionEnvironment.setMaxParallelism(configuration.getInteger(MAX_PARALLELISM_KEY, MAX_PARALLELISM_DEFAULT));
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.setParallelism(configuration.getInteger("PARALLELISM", 1));
        executionEnvironment.getConfig().setAutoWatermarkInterval(configuration.getInteger("WATERMARK_INTERVAL_MS", 10000));
        executionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        executionEnvironment.enableCheckpointing(configuration.getLong("CHECKPOINT_INTERVAL", 30000));
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(configuration.getLong("CHECKPOINT_TIMEOUT", 900000));
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(configuration.getLong("CHECKPOINT_MIN_PAUSE", 5000));
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1));
        executionEnvironment.getConfig().setGlobalJobParameters(configuration);

        tableEnvironment.getConfig().setIdleStateRetentionTime(Time.hours(configuration.getInteger("MIN_IDLE_STATE_RETENTION_TIME", 8)),
                Time.hours(configuration.getInteger("MAX_IDLE_STATE_RETENTION_TIME", 9)));
        return this;
    }

    public StreamManager registerSource() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        kafkaStreams = getKafkaStreams();
        kafkaStreams.notifySubscriber(telemetryExporter);
        kafkaStreams.getStreams().forEach((tableName, kafkaConsumer) -> {
            KafkaProtoStreamingTableSource tableSource = new KafkaProtoStreamingTableSource(
                    kafkaConsumer,
                    rowTimeAttributeName,
                    watermarkDelay,
                    enablePerPartitionWatermark
            );
            tableEnvironment.registerTableSource(tableName, tableSource);
        });
        return this;
    }

    private Map<String, ScalarFunction> addScalarFunctions() {
        HashMap<String, ScalarFunction> scalarFunctions = new HashMap<>();
        scalarFunctions.put("S2Id", new S2Id());
        scalarFunctions.put("GEOHASH", new GeoHash());
        scalarFunctions.put("ElementAt", new ElementAt(kafkaStreams.getProtos(),
                configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT), getStencilUrls(),
                configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT),
                configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT),
                configuration.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT)));
        scalarFunctions.put("SelectFields", new SelectFields(true, getStencilUrls()));
        scalarFunctions.put("Filters", new Filters(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT), getStencilUrls()));
        scalarFunctions.put("CondEq", new CondEq());
        scalarFunctions.put("ServiceArea", new ServiceArea());
        scalarFunctions.put("ArrayAggregate", new ArrayAggregate());
        scalarFunctions.put("ArrayOperate", new ArrayOperate());
        scalarFunctions.put("ServiceAreaId", new ServiceAreaId());
        scalarFunctions.put("Distance", new Distance());
        scalarFunctions.put("AppBetaUsers", new AppBetaUsers());
        scalarFunctions.put("KeyValue", new KeyValue());
        scalarFunctions.put("DartGet", DartGet.withGcsDataStore(getGcsProjectId(), getGcsBucketId()));
        scalarFunctions.put("DartContains", DartContains.withGcsDataStore(getGcsProjectId(), getGcsBucketId()));
        scalarFunctions.put("TimestampFromUnix", new TimestampFromUnix());
        scalarFunctions.put("SecondsElapsed", new SecondsElapsed());
        scalarFunctions.put("StartOfWeek", new StartOfWeek());
        scalarFunctions.put("EndOfWeek", new EndOfWeek());
        scalarFunctions.put("StartOfMonth", new StartOfMonth());
        scalarFunctions.put("EndOfMonth", new EndOfMonth());
        scalarFunctions.put("TimeInDate", new TimeInDate());
        scalarFunctions.put("MapGet", new MapGet());
        scalarFunctions.put("ExponentialMovingAverage", new ExponentialMovingAverage());
        scalarFunctions.put("LinearTrend", new LinearTrend());
        scalarFunctions.put("ListContains", new ListContains());
        scalarFunctions.put("ToDouble", new ToDouble());
        return scalarFunctions;
    }

    private Map<String, TableFunction> addTableFunctions() {
        HashMap<String, TableFunction> tableFunctions = new HashMap<>();
        tableFunctions.put("RuleViolatedEventUnnest", new RuleViolatedEventUnnest());
        tableFunctions.put("OutlierMad", new OutlierMad());
        tableFunctions.put("HistogramBucket", new HistogramBucket());
        return tableFunctions;
    }

    private List<String> getStencilUrls() {
        return Arrays.stream(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT).split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    private Map<String, AggregateFunction> addAggregateFunctions() {
        HashMap<String, AggregateFunction> aggregateFunctions = new HashMap<>();
        aggregateFunctions.put("DistinctCount", new DistinctCount());
        aggregateFunctions.put("DistinctByCurrentStatus", new DistinctByCurrentStatus());
        aggregateFunctions.put("Features", new Features());
        aggregateFunctions.put("FeaturesWithType", new FeaturesWithType());
        aggregateFunctions.put("ConcurrentTransactions", new ConcurrentTransactions(7200));
        aggregateFunctions.put("DistanceAggregator", new DistanceAggregator());
        aggregateFunctions.put("CollectArray", new CollectArray());
        aggregateFunctions.put("PercentileAggregator", new PercentileAggregator());
        aggregateFunctions.put("Top", new Top());
        return aggregateFunctions;
    }

    public StreamManager registerFunctions() {
        Map<String, ScalarFunction> scalarFunctions = addScalarFunctions();
        Map<String, TableFunction> tableFunctions = addTableFunctions();
        Map<String, AggregateFunction> aggregateFunctions = addAggregateFunctions();
        AggregatedUDFTelemetryPublisher udfTelemetryPublisher = new AggregatedUDFTelemetryPublisher(configuration, aggregateFunctions);
        udfTelemetryPublisher.notifySubscriber(telemetryExporter);
        scalarFunctions.forEach((scalarFunctionName, scalarUDF) -> tableEnvironment.registerFunction(scalarFunctionName, scalarUDF));
        tableFunctions.forEach((tableFunctionName, tableUDF) -> tableEnvironment.registerFunction(tableFunctionName, tableUDF));
        aggregateFunctions.forEach((aggregateFunctionName, aggregateUDF) -> tableEnvironment.registerFunction(aggregateFunctionName, aggregateUDF));
        return this;
    }

    public StreamManager registerOutputStream() {
        Table table = tableEnvironment.sqlQuery(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT));
        StreamInfo streamInfo = createStreamInfo(table);
        streamInfo = addPostProcessor(streamInfo);
        addSink(streamInfo);
        return this;
    }

    public void execute() throws Exception {
        executionEnvironment.execute(configuration.getString("FLINK_JOB_ID", "SQL Flink job"));
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
        StreamInfo postProcessedStream = streamInfo;
        for (PostProcessor postProcessor : postProcessors) {
            postProcessedStream = postProcessor.process(postProcessedStream);
        }
        return postProcessedStream;
    }

    private void addSink(StreamInfo streamInfo) {
        SinkOrchestrator sinkOrchestrator = new SinkOrchestrator();
        sinkOrchestrator.addSubscriber(telemetryExporter);
        streamInfo.getDataStream().addSink(sinkOrchestrator.getSink(configuration, streamInfo.getColumnNames(), stencilClientOrchestrator));
    }

    private Streams getKafkaStreams() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        return new Streams(configuration, rowTimeAttributeName, stencilClientOrchestrator, enablePerPartitionWatermark, watermarkDelay);
    }

    private String getRedisServer() {
        return configuration.getString(REDIS_SERVER_KEY, REDIS_SERVER_DEFAULT);
    }

    private String getGcsProjectId() {
        return configuration.getString(GCS_PROJECT_ID, GCS_PROJECT_DEFAULT);
    }

    private String getGcsBucketId() {
        return configuration.getString(GCS_BUCKET_ID, GCS_BUCKET_DEFAULT);
    }
}
