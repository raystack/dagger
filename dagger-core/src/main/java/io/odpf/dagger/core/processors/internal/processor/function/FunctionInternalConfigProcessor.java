package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.protohandler.TypeInformationFactory;
import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;

import com.google.protobuf.Descriptors;
import com.google.gson.Gson;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;

/**
 * The Function internal config processor.
 */
public class FunctionInternalConfigProcessor implements InternalConfigProcessor, Serializable {

    public static final String FUNCTION_CONFIG_HANDLER_TYPE = "function";
    public static final String CURRENT_TIMESTAMP_FUNCTION_KEY = "CURRENT_TIMESTAMP";
    public static final String JSON_PAYLOAD_FUNCTION_KEY = "JSON_PAYLOAD";

    private ColumnNameManager columnNameManager;
    private InternalSourceConfig internalSourceConfig;
    private Configuration configuration;
    //private Map<String, Function<RowManager, Object>> functionRegistry;
    private JsonRowSerializationSchema jsonRowSerializationSchema;

    /**
     * Instantiates a new Function internal config processor.
     *
     * @param columnNameManager    the column name manager
     * @param internalSourceConfig the internal source config
     */
    public FunctionInternalConfigProcessor(ColumnNameManager columnNameManager, InternalSourceConfig internalSourceConfig, Configuration configuration) {
        this.columnNameManager = columnNameManager;
        this.internalSourceConfig = internalSourceConfig;
        this.configuration = configuration;
        this.jsonRowSerializationSchema = createJsonRowSerializationSchema();

        // this.functionRegistry = new HashMap<String, Function<RowManager, Object>>();
        // this.functionRegistry.put(CURRENT_TIMESTAMP_FUNCTION_KEY, (rowManager) -> getCurrentTime());
        // this.functionRegistry.put(JSON_PAYLOAD_FUNCTION_KEY, (rowManager) -> getJSONPayload(rowManager));
    }

    @Override
    public boolean canProcess(String type) {
        return FUNCTION_CONFIG_HANDLER_TYPE.equals(type);
    }

    @Override
    public void process(RowManager rowManager) {
        int outputFieldIndex = columnNameManager.getOutputIndex(internalSourceConfig.getOutputField());
        String functionName = internalSourceConfig.getValue();
        if (outputFieldIndex != -1) {
            if (!CURRENT_TIMESTAMP_FUNCTION_KEY.equals(internalSourceConfig.getValue()) && !JSON_PAYLOAD_FUNCTION_KEY.equals(internalSourceConfig.getValue())) {
                throw new InvalidConfigurationException(String.format("The function %s is not supported in custom configuration", internalSourceConfig.getValue()));
            }
            if (CURRENT_TIMESTAMP_FUNCTION_KEY.equals(functionName)) {
                rowManager.setInOutput(outputFieldIndex, getCurrentTime());
            }
            if (JSON_PAYLOAD_FUNCTION_KEY.equals(functionName)) {
                rowManager.setInOutput(outputFieldIndex, getJSONPayload(rowManager));
            }
        }
    }

    /**
     * Gets current time.
     *
     * @return the current time
     */
    protected Timestamp getCurrentTime() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * Gets payload in JSON.
     *
     * @return the incoming message as JSON
     */
    protected String getJSONPayload(RowManager rowManager) {
        Row inputRow = rowManager.getInputData();
        System.out.println("input data ^");
        return new String(jsonRowSerializationSchema.serialize(inputRow));
    }

    private JsonRowSerializationSchema createJsonRowSerializationSchema() {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Gson gsonObj = new Gson();
        Map[] streamsConfig = gsonObj.fromJson(jsonArrayString, Map[].class);
        String inputProtoClassName = (String) streamsConfig[0].get(STREAM_INPUT_SCHEMA_PROTO_CLASS);

        System.out.println("inputProtoClassName: " + inputProtoClassName);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        Descriptors.Descriptor inputDescriptor = stencilClientOrchestrator.getStencilClient().get(inputProtoClassName);

        return JsonRowSerializationSchema
                .builder()
                .withTypeInfo(TypeInformationFactory.getRowType(inputDescriptor))
                .build();
    }
}
