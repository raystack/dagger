package io.odpf.dagger.core.processors.internal.processor.function.functions;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.internal.processor.function.FunctionProcessor;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.common.serde.proto.protohandler.TypeInformationFactory;
import io.odpf.dagger.common.core.StencilClientOrchestrator;


import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;

import com.google.protobuf.Descriptors;
import com.google.gson.Gson;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;

import java.util.Map;
import java.io.Serializable;

public class JsonPayloadFunction implements FunctionProcessor, Serializable {
    public static final String JSON_PAYLOAD_FUNCTION_KEY = "JSON_PAYLOAD";
    public static final String SCHEMA_PROTO_CLASS_KEY = "schema_proto_class";
    private static final Gson gson = new Gson();

    private Configuration configuration;
    private JsonRowSerializationSchema jsonRowSerializationSchema;

    public JsonPayloadFunction(Configuration configuration) {
        this.configuration = configuration;
        this.jsonRowSerializationSchema = createJsonRowSerializationSchema();
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
        Row inputRow = rowManager.getInputData();
        return new String(jsonRowSerializationSchema.serialize(inputRow));
    }

    /**
     * Creates JSON schema for input row.
     *
     * @return the JSON row serialization schema for input row
     */
    private JsonRowSerializationSchema createJsonRowSerializationSchema() {
        if (configuration == null) {
            throw new InvalidConfigurationException("Invalid configuration: null");
        }

        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        if (streamsConfig == null || streamsConfig.length == 0) {
            throw new InvalidConfigurationException(String.format("Invalid configuration: %s not provided", INPUT_STREAMS));
        }

        String inputProtoClassName = (String) streamsConfig[0].get(STREAM_INPUT_SCHEMA_PROTO_CLASS);

        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        Descriptors.Descriptor inputDescriptor = stencilClientOrchestrator.getStencilClient().get(inputProtoClassName);

        return JsonRowSerializationSchema
                .builder()
                .withTypeInfo(TypeInformationFactory.getRowType(inputDescriptor))
                .build();
    }
}
