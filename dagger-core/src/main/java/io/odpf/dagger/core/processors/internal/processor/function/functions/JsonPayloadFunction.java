package io.odpf.dagger.core.processors.internal.processor.function.functions;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.function.FunctionProcessor;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.common.serde.proto.protohandler.TypeInformationFactory;

import com.google.protobuf.Descriptors;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.formats.json.JsonRowSerializationSchema;

import java.util.Map;
import java.io.Serializable;

public class JsonPayloadFunction implements FunctionProcessor, Serializable {
    public static final String JSON_PAYLOAD_FUNCTION_KEY = "JSON_PAYLOAD";
    public static final String SCHEMA_PROTO_CLASS_KEY = "schema_proto_class";

    private InternalSourceConfig internalSourceConfig;
    private SchemaConfig schemaConfig;
    private JsonRowSerializationSchema jsonRowSerializationSchema;

    /**
     * Instantiates a new JsonPayloadFunction processor.
     *
     * @param internalSourceConfig the internal source config
     * @param schemaConfig         the schema config
     */
    public JsonPayloadFunction(InternalSourceConfig internalSourceConfig, SchemaConfig schemaConfig) {
        this.internalSourceConfig = internalSourceConfig;
        this.schemaConfig = schemaConfig;
    }

    @Override
    public boolean canProcess(String functionName) {
        return JSON_PAYLOAD_FUNCTION_KEY.equals(functionName);
    }

    /**
     * Gets payload in JSON.
     *
     * @return the incoming message as JSON
     */
    @Override
    public Object getResult(RowManager rowManager) {
        if (jsonRowSerializationSchema == null) {
            jsonRowSerializationSchema = createJsonRowSerializationSchema();
        }
        return new String(jsonRowSerializationSchema.serialize(rowManager.getInputData()));
    }

    private JsonRowSerializationSchema createJsonRowSerializationSchema() {
        StencilClient stencilClient = schemaConfig.getStencilClientOrchestrator().getStencilClient();
        if (stencilClient == null) {
            throw new InvalidConfigurationException("Invalid configuration: stencil client is null");
        }

        Map<String, String> internalProcessorConfig = internalSourceConfig.getInternalProcessorConfig();
        if (internalProcessorConfig == null) {
            throw new InvalidConfigurationException("Invalid internal source configuration: missing internal processor config");
        }

        if (!internalProcessorConfig.containsKey(SCHEMA_PROTO_CLASS_KEY)) {
            throw new InvalidConfigurationException(String.format("Invalid internal source configuration: missing \"%s\" key in internal processor config", SCHEMA_PROTO_CLASS_KEY));
        }

        String schemaProtoClassKey = internalProcessorConfig.get(SCHEMA_PROTO_CLASS_KEY);
        Descriptors.Descriptor inputDescriptor = stencilClient.get(schemaProtoClassKey);

        return JsonRowSerializationSchema
                .builder()
                .withTypeInfo(TypeInformationFactory.getRowType(inputDescriptor))
                .build();
    }
}
