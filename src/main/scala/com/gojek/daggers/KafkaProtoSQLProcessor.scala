package com.gojek.daggers

import java.util.TimeZone

import com.gojek.dagger.udf._
import com.gojek.daggers.config.ConfigurationProviderFactory
import com.gojek.de.stencil.StencilClientFactory
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

object KafkaProtoSQLProcessor {

  def main(args: Array[String]) {

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
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(configuration.getLong("PAUSE_BETWEEN_CHECKPOINTS", 5000))
    env.getCheckpointConfig.setCheckpointTimeout(configuration.getLong("CHECKPOINT_TIMEOUT", 180000))
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1))
    env.getConfig.setGlobalJobParameters(configuration)

    val enableRemoteStencil = configuration.getBoolean("ENABLE_STENCIL_URL", false)

    val stencilClient = if (!enableRemoteStencil) StencilClientFactory.getClient() else StencilClientFactory.getClient(
      configuration.getString("STENCIL_URL", ""), configuration.toMap)

    val rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "")

    val streams = new Streams(configuration, rowTimeAttributeName, stencilClient)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    for ((tableName, kafkaConsumer) <- streams.getStreams) {
      val tableSource: KafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(kafkaConsumer, rowTimeAttributeName, configuration.getLong("WATERMARK_DELAY_MS", 10000))
      tableEnv.registerTableSource(tableName, tableSource)
    }
    val lifeTime = configuration.getLong("JOB_LIFE_IN_MS", 0)
    if (lifeTime != 0L) {
      val timer = new java.util.Timer()
      val task = new java.util.TimerTask {
        override def run(): Unit = System.exit(0)
      }
      timer.schedule(task, lifeTime)
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

    val resultTable2 = tableEnv.sqlQuery(configuration.getString("SQL_QUERY", ""))

    resultTable2.toAppendStream[Row]
      .addSink(SinkFactory.getSinkFunction(configuration, resultTable2.getSchema.getColumnNames, stencilClient))
    env.execute(configuration.getString("FLINK_JOB_ID", "SQL Flink job"))
  }

}
