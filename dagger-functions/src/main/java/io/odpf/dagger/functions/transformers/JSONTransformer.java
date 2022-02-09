package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import io.odpf.dagger.common.serde.proto.protohandler.TypeInformationFactory;
import com.google.protobuf.Descriptors;
import io.odpf.stencil.client.StencilClient;
import java.sql.Timestamp;
import java.time.Instant;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

import com.google.gson.Gson;



import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;
import static io.odpf.dagger.common.core.Constants.SINK_KAFKA_PROTO_MESSAGE_KEY;

/**
 * Convert incoming row to JSON.
 */
public class JSONTransformer extends RichMapFunction<Row, Row> implements Serializable, Transformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONTransformer.class.getName());
    private String inputProtoClassName;
    private String outputProtoClassName;
    private Configuration configuration;

    protected static final String INTERNAL_VALIDATION_FILED = "__internal_validation_field__";

    /**
     * Instantiates a new JSON transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public JSONTransformer(Map<String, Object> transformationArguments, String[] columnNames, Configuration configuration) {
        this.configuration = configuration;

        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Gson GSON = new Gson();
        Map[] streamsConfig = GSON.fromJson(jsonArrayString, Map[].class);
        this.inputProtoClassName = (String) streamsConfig[0].get(STREAM_INPUT_SCHEMA_PROTO_CLASS);
        this.outputProtoClassName = configuration.getString(SINK_KAFKA_PROTO_MESSAGE_KEY, "");
    }

    // TODO: remove event_timestamp nested fields that are getting added
    @Override
    public Row map(Row inputRow) {
        JsonRowSerializationSchema jsonRowSerializationSchema = createJsonRowSerializationSchema();
        String json = new String(jsonRowSerializationSchema.serialize(inputRow));

        Row outputRow = Row.withPositions(5);
        outputRow.setField(0, "entity-id");
        outputRow.setField(1, "entity-name");
        outputRow.setField(2, 1); // operation type
        outputRow.setField(3, json);
        outputRow.setField(4, inputRow.getField(inputRow.getArity()-1)); // event_timestamp spike
        return outputRow;
    }

    @Override
    public StreamInfo transform(StreamInfo streamInfo) {
        DataStream<Row> inputStream = streamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        return new StreamInfo(outputStream, streamInfo.getColumnNames());
    }

    private JsonRowSerializationSchema createJsonRowSerializationSchema() {
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        StencilClient stencilClient = stencilClientOrchestrator.getStencilClient();
        Descriptors.Descriptor inputDescriptor = stencilClient.get(inputProtoClassName);

        return JsonRowSerializationSchema
                .builder()
                .withTypeInfo(TypeInformationFactory.getRowType(inputDescriptor))
                .build();
    }

    private Descriptors.Descriptor getOutputDescriptor() {
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        return stencilClientOrchestrator.getStencilClient().get(outputProtoClassName);
    }
}
