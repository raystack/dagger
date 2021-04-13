package io.odpf.dagger.processors.external;

import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.processors.ColumnNameManager;
import com.google.gson.Gson;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

public class SchemaConfig implements Serializable {
    private final Configuration configuration;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private ColumnNameManager columnNameManager;
    private String[] inputProtoClasses;
    private String outputProtoClassName;
    private static final Gson gson = new Gson();

    public SchemaConfig(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager) {
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.inputProtoClasses = getMessageProtoClasses();
        this.outputProtoClassName = configuration.getString(Constants.OUTPUT_PROTO_MESSAGE, "");
    }

    public StencilClientOrchestrator getStencilClientOrchestrator() {
        return stencilClientOrchestrator;
    }

    public ColumnNameManager getColumnNameManager() {
        return columnNameManager;
    }

    public String[] getInputProtoClasses() {
        return inputProtoClasses;
    }

    public String getOutputProtoClassName() {
        return outputProtoClassName;
    }

    private String[] getMessageProtoClasses() {
        String jsonArrayString = configuration.getString(Constants.INPUT_STREAMS, "");
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        ArrayList<String> protoClasses = new ArrayList<>();
        for (Map individualStreamConfig : streamsConfig) {
            protoClasses.add((String) individualStreamConfig.get(Constants.STREAM_PROTO_CLASS_NAME));
        }
        return protoClasses.toArray(new String[0]);
    }
}
