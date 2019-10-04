package com.gojek.daggers;

import com.gojek.daggers.config.ConfigurationProviderFactory;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.scala.StreamTableEnvironment;

import java.util.TimeZone;

public class KafkaProtoSQLProcessor {

    public static void main(String[] args) throws ProgramInvocationException {
        try {
            Configuration configuration = new ConfigurationProviderFactory(args).provider().get();
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);
            StreamManager streamManager = new StreamManager(configuration, executionEnvironment, tableEnvironment);
            streamManager
                    .registerConfigs()
                    .registerSource()
                    .registerFunctions()
                    .registerOutputStream()
                    .execute();
        } catch (Exception | AssertionError e) {
            e.printStackTrace();
            throw new ProgramInvocationException(e);
        }
    }
}
