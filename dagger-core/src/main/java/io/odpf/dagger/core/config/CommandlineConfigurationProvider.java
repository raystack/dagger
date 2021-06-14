package io.odpf.dagger.core.config;

import com.google.gson.Gson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Base64;

/**
 * The class which handle configuration provided from Commandline.
 */
public class CommandlineConfigurationProvider implements ConfigurationProvider {

    private String[] args;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Commandline configuration provider.
     *
     * @param args the args
     */
    public CommandlineConfigurationProvider(String[] args) {

        this.args = args;
    }

    @Override
    public Configuration get() {
        System.out.println("params from " + CommandlineConfigurationProvider.class.getName());
        ParameterTool.fromArgs(args).toMap().entrySet().stream().forEach(System.out::println);
        return constructConfiguration();
    }

    private Configuration constructConfiguration() {
        String[] finalArgs = args;
        if (isEncodedArgsPresent()) {
            finalArgs = parseEncodedProgramArgs();
        }
        return ParameterTool.fromArgs(finalArgs).getConfiguration();
    }

    private boolean isEncodedArgsPresent() {
        String encodedArgs = ParameterTool.fromArgs(args).get("encodedArgs");
        return encodedArgs != null;
    }

    private String[] parseEncodedProgramArgs() {
        String encodedArgs = ParameterTool.fromArgs(args).get("encodedArgs");
        byte[] decoded = Base64.getMimeDecoder().decode(encodedArgs);
        return GSON.fromJson(new String(decoded), String[].class);
    }
}
