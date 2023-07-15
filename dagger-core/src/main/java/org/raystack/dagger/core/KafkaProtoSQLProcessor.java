package org.raystack.dagger.core;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.core.config.ConfigurationProvider;
import org.raystack.dagger.core.config.ConfigurationProviderFactory;
import org.apache.flink.client.program.ProgramInvocationException;
import org.raystack.dagger.common.core.DaggerContext;

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
            DaggerContext daggerContext = DaggerContext.init(configuration);
            StreamManager streamManager = new StreamManager(daggerContext);
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
