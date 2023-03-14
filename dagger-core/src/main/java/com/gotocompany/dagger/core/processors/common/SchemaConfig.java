package com.gotocompany.dagger.core.processors.common;

import com.google.gson.Gson;
import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.core.processors.ColumnNameManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import static com.gotocompany.dagger.common.core.Constants.INPUT_STREAMS;
import static com.gotocompany.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;
import static com.gotocompany.dagger.core.utils.Constants.SINK_KAFKA_PROTO_MESSAGE_KEY;

/**
 * The Schema config.
 */
public class SchemaConfig implements Serializable {
    private final Configuration configuration;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private ColumnNameManager columnNameManager;
    private String[] inputProtoClasses;
    private String outputProtoClassName;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Schema config.
     *
     * @param configuration             the configuration
     * @param stencilClientOrchestrator the stencil client orchestrator
     * @param columnNameManager         the column name manager
     */
    public SchemaConfig(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager) {
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.inputProtoClasses = getMessageProtoClasses();
        this.outputProtoClassName = configuration.getString(SINK_KAFKA_PROTO_MESSAGE_KEY, "");
    }

    /**
     * Gets stencil client orchestrator.
     *
     * @return the stencil client orchestrator
     */
    public StencilClientOrchestrator getStencilClientOrchestrator() {
        return stencilClientOrchestrator;
    }

    /**
     * Gets column name manager.
     *
     * @return the column name manager
     */
    public ColumnNameManager getColumnNameManager() {
        return columnNameManager;
    }

    /**
     * Get input proto classes.
     *
     * @return the input proto classes
     */
    public String[] getInputProtoClasses() {
        return inputProtoClasses;
    }

    /**
     * Gets output proto class name.
     *
     * @return the output proto class name
     */
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
