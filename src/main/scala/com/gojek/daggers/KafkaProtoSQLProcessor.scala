package com.gojek.daggers

import java.util
import java.util.TimeZone

import com.gojek.dagger.udf._
import com.gojek.dagger.udf.dart.store.RedisConfig
import com.gojek.daggers.config.ConfigurationProviderFactory
import com.gojek.de.stencil.StencilClientFactory
import org.apache.flink.api.scala._
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

object KafkaProtoSQLProcessor {

  def main(args: Array[String]) {
    try {

      val configuration: Configuration = new ConfigurationProviderFactory(args).provider().get()

      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      val parallelism = configuration.getInteger("PARALLELISM", 1)
      env.setParallelism(parallelism)
      val autoWatermarkInterval = configuration.getInteger("WATERMARK_INTERVAL_MS", 10000)
      env.getConfig.setAutoWatermarkInterval(autoWatermarkInterval)

      env.enableCheckpointing(configuration.getLong("CHECKPOINT_INTERVAL", 30000))
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
      env.getCheckpointConfig.setCheckpointTimeout(configuration.getLong("CHECKPOINT_TIMEOUT", 900000))
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(configuration.getLong("CHECKPOINT_MIN_PAUSE", 5000))
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1))
      env.getConfig.setGlobalJobParameters(configuration)

      val enableRemoteStencil = configuration.getBoolean("ENABLE_STENCIL_URL", false)

      val stencilUrls: util.List[String] = configuration.getString("STENCIL_URL", "").split(",").map(_.trim).toList

      val stencilClient = if (!enableRemoteStencil) StencilClientFactory.getClient() else StencilClientFactory.getClient(
        stencilUrls, configuration.toMap)

      val rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "")

      val enablePerPartitionWatermark = configuration.getBoolean("ENABLE_PER_PARTITION_WATERMARK", false)

      val watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", 10000)

      val streams = new Streams(configuration, rowTimeAttributeName, stencilClient, enablePerPartitionWatermark, watermarkDelay)

      val tableEnv = TableEnvironment.getTableEnvironment(env)

      for ((tableName, kafkaConsumer) <- streams.getStreams) {
        val tableSource: KafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(
          kafkaConsumer,
          rowTimeAttributeName,
          watermarkDelay,
          enablePerPartitionWatermark
        )
        tableEnv.registerTableSource(tableName, tableSource)
      }

      tableEnv.registerFunction("S2Id", new S2Id())
      tableEnv.registerFunction("GEOHASH", new GeoHash())
      tableEnv.registerFunction("DistinctByCurrentStatus", new DistinctByCurrentStatus)
      tableEnv.registerFunction("ElementAt", new ElementAt(streams.getProtos.entrySet().iterator().next().getValue, stencilClient))
      tableEnv.registerFunction("ServiceArea", new ServiceArea())
      tableEnv.registerFunction("ServiceAreaId", new ServiceAreaId())
      tableEnv.registerFunction("DistinctCount", new DistinctCount())
      tableEnv.registerFunction("Distance", new Distance())
      tableEnv.registerFunction("AppBetaUsers", new AppBetaUsers())
      tableEnv.registerFunction("KeyValue", new KeyValue())
      val redisServer = configuration.getString("REDIS_SERVER", "localhost")
      tableEnv.registerFunction("DartContains", DartContains.withRedisDataStore(new RedisConfig(redisServer)))
      tableEnv.registerFunction("DartGet", DartGet.withRedisDataStore(new RedisConfig(redisServer)))
      tableEnv.registerFunction("Features", new Features())
      tableEnv.registerFunction("TimestampFromUnix", new TimestampFromUnix())

      val resultTable2 = tableEnv.sqlQuery(configuration.getString("SQL_QUERY", ""))
      val value = resultTable2.toAppendStream[Row]

      val deNormaliseStream = new DeNormaliseStream(value, configuration, resultTable2, stencilClient)
      deNormaliseStream.apply()

      env.execute(configuration.getString("FLINK_JOB_ID", "SQL Flink job"))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new ProgramInvocationException(e)
      }
    }
  }
}
