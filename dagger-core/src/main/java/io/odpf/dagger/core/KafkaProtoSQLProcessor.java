package io.odpf.dagger.core;

import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.config.ConfigurationProvider;
import io.odpf.dagger.core.config.ConfigurationProviderFactory;

import java.util.TimeZone;

/**
 * Main class to run Dagger.
 */
public class KafkaProtoSQLProcessor {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws ProgramInvocationException the program invocation exception
     */
    public static void main(String[] args) throws ProgramInvocationException {
        try {
            ConfigurationProvider provider = new ConfigurationProviderFactory(args).provider();
            Configuration configuration = provider.get();
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, environmentSettings);

            StreamManager streamManager = new StreamManager(configuration, executionEnvironment, tableEnvironment);
            streamManager
                    .registerConfigs()
                    .registerSourceWithPreProcessors()
                    .registerFunctions()
                    .registerOutputStream()
                    .execute();
        } catch (Exception | AssertionError e) {
            e.printStackTrace();
            throw new ProgramInvocationException(e);
        }
    }
}
