package com.gojek.daggers;

import com.gojek.daggers.config.ConfigurationProviderFactory;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;

import java.util.TimeZone;

public class KafkaProtoSQLProcessor {

    public static void main(String[] args) throws ProgramInvocationException {
        try {

            Configuration configuration = new ConfigurationProviderFactory(args).provider().get();
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            StreamManager streamManager = new StreamManager(configuration);
            streamManager
                    .registerConfigs()
                    .createStencilClient()
                    .createTableEnv()
                    .registerSource()
                    .registerFunctions()
                    .createStream()
                    .addPostProcessor()
                    .addSink()
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
            throw new ProgramInvocationException(e);
        }
    }
}
