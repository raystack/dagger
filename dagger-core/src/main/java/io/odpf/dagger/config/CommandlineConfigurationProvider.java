package io.odpf.dagger.config;

import com.google.gson.Gson;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Base64;

public class CommandlineConfigurationProvider implements ConfigurationProvider {

    private String[] args;
    private static final Gson gson = new Gson();

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
        if (encodedArgs == null) {
            return false;
        }
        return true;
    }

    private String[] parseEncodedProgramArgs() {
        String encodedArgs = ParameterTool.fromArgs(args).get("encodedArgs");
        byte[] decoded = Base64.getMimeDecoder().decode(encodedArgs);
        return gson.fromJson(new String(decoded), String[].class);
    }
}
