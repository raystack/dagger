package io.odpf.dagger.core.processors.external;

import com.google.gson.Gson;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.ColumnNameManager;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;
import static io.odpf.dagger.core.utils.Constants.SINK_KAFKA_PROTO_MESSAGE;

public class SchemaConfig implements Serializable {
    private final Configuration configuration;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private ColumnNameManager columnNameManager;
    private String[] inputProtoClasses;
    private String outputProtoClassName;
    private static final Gson GSON = new Gson();

    public SchemaConfig(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager) {
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.inputProtoClasses = getMessageProtoClasses();
        this.outputProtoClassName = configuration.getString(SINK_KAFKA_PROTO_MESSAGE, "");
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
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Map[] streamsConfig = GSON.fromJson(jsonArrayString, Map[].class);
        ArrayList<String> protoClasses = new ArrayList<>();
        for (Map individualStreamConfig : streamsConfig) {
            protoClasses.add((String) individualStreamConfig.get(STREAM_INPUT_SCHEMA_PROTO_CLASS));
        }
        return protoClasses.toArray(new String[0]);
    }
}
