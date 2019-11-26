package com.gojek.daggers;

import com.gojek.daggers.config.ConfigurationProviderFactory;
import com.gojek.daggers.core.StreamManager;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.TimeZone;

public class KafkaProtoSQLProcessor {

    public static void main(String[] args) throws ProgramInvocationException {
        try {
            Configuration configuration = new ConfigurationProviderFactory(args).provider().get();
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
            StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);

            StreamManager streamManager = new StreamManager(configuration, executionEnvironment, tableEnvironment);
            streamManager
                    .registerConfigs()
                    .registerSource()
                    .registerFunctions()
                    .registerTelemetrySubsribers()
                    .registerOutputStream()
                    .execute();
        } catch (Exception | AssertionError e) {
            e.printStackTrace();
            throw new ProgramInvocationException(e);
        }
    }
}
