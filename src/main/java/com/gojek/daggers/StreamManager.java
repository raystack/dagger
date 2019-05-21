package com.gojek.daggers;

import com.gojek.dagger.udf.*;
import com.gojek.dagger.udf.accumulator.distinctCount.DistinctCountAccumulator;
import com.gojek.dagger.udf.accumulator.feast.FeatureAccumulator;
import com.gojek.dagger.udf.dart.store.RedisConfig;
import com.gojek.daggers.postprocessor.PostProcessor;
import com.gojek.daggers.postprocessor.PostProcessorFactory;
import com.gojek.de.stencil.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.scala.StreamTableEnvironment;
import org.apache.flink.table.api.scala.TableConversions;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StreamManager {
    private StreamInfo streamInfo;
    private Configuration configuration;
    private StencilClient stencilClient;
    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment tableEnvironment;


    public StreamManager(Configuration configuration) {
        this(configuration, StreamExecutionEnvironment.getExecutionEnvironment());
    }

    StreamManager(Configuration configuration, StreamExecutionEnvironment executionEnvironment) {
        this.configuration = configuration;
        environment = executionEnvironment;
    }

    public StreamManager createTableEnv() {
        tableEnvironment = TableEnvironment.getTableEnvironment(environment);
        return this;
    }

    public StreamManager createStencilClient() {
        Boolean enableRemoteStencil = configuration.getBoolean("ENABLE_STENCIL_URL", false);
        List<String> stencilUrls = Arrays.stream(configuration.getString("STENCIL_URL", "").split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        stencilClient = enableRemoteStencil
                ? StencilClientFactory.getClient(stencilUrls, configuration.toMap())
                : StencilClientFactory.getClient();
        return this;
    }

    public StreamManager registerConfigs() {
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(configuration.getInteger("PARALLELISM", 1));
        environment.getConfig().setAutoWatermarkInterval(configuration.getInteger("WATERMARK_INTERVAL_MS", 10000));
        environment.enableCheckpointing(configuration.getLong("CHECKPOINT_INTERVAL", 30000));
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(configuration.getLong("CHECKPOINT_TIMEOUT", 900000));
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(configuration.getLong("CHECKPOINT_MIN_PAUSE", 5000));
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1));
        environment.getConfig().setGlobalJobParameters(configuration);
        return this;
    }

    public StreamManager registerSource() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        createStreams().getStreams().forEach((tableName, kafkaConsumer) -> {
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

    public StreamManager registerFunctions() {
        tableEnvironment.registerFunction("S2Id", new S2Id());
        tableEnvironment.registerFunction("GEOHASH", new GeoHash());
        tableEnvironment.registerFunction("ElementAt", new ElementAt(createStreams().getProtos().entrySet().iterator().next().getValue(), stencilClient));
        tableEnvironment.registerFunction("ServiceArea", new ServiceArea());
        tableEnvironment.registerFunction("ServiceAreaId", new ServiceAreaId());
        tableEnvironment.registerFunction("DistinctCount", new DistinctCount(), TypeInformation.of(Integer.class), TypeInformation.of(DistinctCountAccumulator.class));
        tableEnvironment.registerFunction("Distance", new Distance());
        tableEnvironment.registerFunction("AppBetaUsers", new AppBetaUsers());
        tableEnvironment.registerFunction("KeyValue", new KeyValue());
        String redisServer = configuration.getString("REDIS_SERVER", "localhost");
        tableEnvironment.registerFunction("DartContains", DartContains.withRedisDataStore(new RedisConfig(redisServer)));
        tableEnvironment.registerFunction("DartGet", DartGet.withRedisDataStore(new RedisConfig(redisServer)));
        tableEnvironment.registerFunction("Features", new Features(), TypeInformation.of(Row[].class), TypeInformation.of(FeatureAccumulator.class));
        tableEnvironment.registerFunction("TimestampFromUnix", new TimestampFromUnix());
        tableEnvironment.registerFunction("ConcurrentTransactions", new ConcurrentTransactions(7200), TypeInformation.of(Integer.class), TypeInformation.of(ConcurrentState.class));
        tableEnvironment.registerFunction("SecondsElapsed", new SecondsElapsed());
        return this;
    }

    public StreamManager createStream() {
        Table table = tableEnvironment.sqlQuery(configuration.getString("SQL_QUERY", ""));
        DataStream<Row> stream = new TableConversions(table)
                .toRetractStream(TypeInformation.of(Row.class))
                .javaStream()
                .filter(value -> ((Boolean) value._1()))
                .map(value -> value._2);
        streamInfo = new StreamInfo(stream, table.getSchema().getColumnNames());
        return this;
    }

    public StreamManager addPostProcessor() {
        Optional<PostProcessor> postProcessor = PostProcessorFactory.getPostProcessor(configuration, stencilClient, streamInfo.getColumnNames());
        postProcessor.ifPresent(p -> streamInfo = p.process(streamInfo));
        return this;
    }

    public StreamManager addSink() {
        streamInfo.getDataStream().addSink(SinkFactory.getSinkFunction(configuration, streamInfo.getColumnNames(), stencilClient));
        return this;
    }

    public void execute() throws Exception {
        environment.execute(configuration.getString("FLINK_JOB_ID", "SQL Flink job"));
    }

    private Streams createStreams() {
        String rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "");
        Boolean enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false);
        Long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000);
        return new Streams(configuration, rowTimeAttributeName, stencilClient, enablePerPartitionWatermark, watermarkDelay);
    }
}
