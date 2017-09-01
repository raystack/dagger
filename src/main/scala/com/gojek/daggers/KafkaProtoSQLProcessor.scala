package com.gojek.daggers

import java.util
import java.util.TimeZone

import com.gojek.dagger.udf.S2Id
import com.gojek.daggers.config.ConfigurationProviderFactory
import com.gojek.daggers.parser.KafkaEnvironmentVariables
import com.gojek.daggers.sink.{InfluxDBFactoryWrapper, InfluxRowSink}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object KafkaProtoSQLProcessor {

  def main(args: Array[String]) {

    val parameters: Configuration = new ConfigurationProviderFactory(args).provider().get()

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val parallelism = parameters.getInteger("PARALLELISM", 1)
    env.setParallelism(parallelism)
    val autoWatermarkInterval = parameters.getInteger("WATERMARK_INTERVAL_MS", 10000)
    env.getConfig.setAutoWatermarkInterval(autoWatermarkInterval)

    val props = KafkaEnvironmentVariables.parse(parameters)

    val protoClassName: String = parameters.getString("PROTO_CLASS_NAME", "")

    val protoType: ProtoType = new ProtoType(protoClassName)
    val topicNames: util.List[String] =  util.Arrays.asList(parameters.getString("TOPIC_NAMES", "").split(","): _*)
    val kafkaConsumer = new FlinkKafkaConsumer010[Row](topicNames, new ProtoDeserializer(protoClassName, protoType), props)

    val rowTimeAttributeName = parameters.getString("ROWTIME_ATTRIBUTE_NAME", "")
    val tableSource: KafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(kafkaConsumer, new RowTimestampExtractor(parameters.getInteger("EVENT_TIMESTAMP_FIELD_INDEX", 0)), protoType, rowTimeAttributeName)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.registerTableSource(parameters.getString("TABLE_NAME", ""), tableSource)
    tableEnv.registerFunction("S2Id", new S2Id())

    val resultTable2 = tableEnv.sql(parameters.getString("SQL_QUERY", ""))

    resultTable2.toAppendStream[Row]
      .addSink(new InfluxRowSink(new InfluxDBFactoryWrapper(), resultTable2.getSchema.getColumnNames, parameters))
    env.execute(parameters.getString("FLINK_JOB_ID", "SQL Flink job"))
  }

}
