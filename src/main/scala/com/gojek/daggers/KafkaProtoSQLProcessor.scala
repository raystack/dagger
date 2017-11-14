package com.gojek.daggers

import java.util
import java.util.TimeZone

import com.gojek.dagger.udf.{ElementAt, S2Id, ServiceArea, ServiceAreaId, DistinctCount}
import com.gojek.daggers.config.ConfigurationProviderFactory
import com.gojek.daggers.parser.KafkaEnvironmentVariables
import com.gojek.daggers.sink.{InfluxDBFactoryWrapper, InfluxRowSink}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

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

    env.enableCheckpointing(configuration.getLong("CHECKPOINT_INTERVAL", 10000))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(configuration.getLong("PAUSE_BETWEEN_CHECKPOINTS", 5000))
    env.getCheckpointConfig.setCheckpointTimeout(configuration.getLong("CHECKPOINT_TIMEOUT", 60000))
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(configuration.getInteger("MAX_CONCURRECT_CHECKPOINTS", 1))

    println(configuration.getInteger("WATERMARK_INTERVAL_MS", 10000))
    println(configuration.getInteger("WATERMARK_DELAY_MS", 10000))
    val props = KafkaEnvironmentVariables.parse(configuration)

    val protoClassName: String = configuration.getString("PROTO_CLASS_NAME", "")

    val protoType: ProtoType = new ProtoType(protoClassName)
    val topicNames: util.List[String] =  util.Arrays.asList(configuration.getString("TOPIC_NAMES", "").split(","): _*)
    val kafkaConsumer = new FlinkKafkaConsumer010[Row](topicNames, new ProtoDeserializer(protoClassName, protoType), props)

    val rowTimeAttributeName = configuration.getString("ROWTIME_ATTRIBUTE_NAME", "")
    val tableSource: KafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(kafkaConsumer, new RowTimestampExtractor(configuration.getInteger("EVENT_TIMESTAMP_FIELD_INDEX", 0), configuration), protoType, rowTimeAttributeName)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.registerTableSource(configuration.getString("TABLE_NAME", ""), tableSource)
    tableEnv.registerFunction("S2Id", new S2Id())
    tableEnv.registerFunction("ElementAt",new ElementAt(protoClassName))
    tableEnv.registerFunction("ServiceArea",new ServiceArea())
    tableEnv.registerFunction("ServiceAreaId",new ServiceAreaId())
    tableEnv.registerFunction("DistinctCount",new DistinctCount())

    val resultTable2 = tableEnv.sql(configuration.getString("SQL_QUERY", ""))

    resultTable2.toAppendStream[Row]
      .addSink(new InfluxRowSink(new InfluxDBFactoryWrapper(), resultTable2.getSchema.getColumnNames, configuration))
    env.execute(configuration.getString("FLINK_JOB_ID", "SQL Flink job"))
  }

}
