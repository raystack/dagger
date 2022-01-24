package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.functions.transformers.hash.PathReader;
import io.odpf.dagger.functions.transformers.hash.field.RowHasher;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Enables encryption on a set of fields as configured.
 * Using SHA-256 hashing to encrypt data.
 */
public class HashTransformer extends RichMapFunction<Row, Row> implements Serializable, Transformer {
    private static final String SINK_KAFKA_PROTO_MESSAGE = "SINK_KAFKA_PROTO_MESSAGE";
    private static final String ENCRYPTION_FIELD_KEY = "maskColumns";
    private final List<String> fieldsToHash;
    private Configuration configuration;

    private final String[] columnNames;
    private Map<String, RowHasher> rowHasherMap;

    /**
     * Instantiates a new Hash transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public HashTransformer(Map<String, Object> transformationArguments, String[] columnNames, Configuration configuration) {
        this.fieldsToHash = getFieldsToHash(transformationArguments);
        this.columnNames = columnNames;
        this.configuration = configuration;
    }

    private ArrayList<String> getFieldsToHash(Map<String, Object> transformationArguments) {
        return (ArrayList<String>) transformationArguments.get(ENCRYPTION_FIELD_KEY);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        if (this.rowHasherMap == null) {
            this.rowHasherMap = createRowHasherMap();
        }
        super.open(internalFlinkConfig);
    }

    @Override
    public StreamInfo transform(StreamInfo streamInfo) {
        DataStream<Row> inputStream = streamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        return new StreamInfo(outputStream, streamInfo.getColumnNames());
    }

    /**
     * Create row hasher map.
     *
     * @return the map
     */
    protected Map<String, RowHasher> createRowHasherMap() {
        String outputProtoClassName = configuration.getString(SINK_KAFKA_PROTO_MESSAGE, "");
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        Descriptors.Descriptor outputDescriptor = stencilClientOrchestrator.getStencilClient().get(outputProtoClassName);
        if (outputDescriptor == null) {
            throw new DescriptorNotFoundException("Output Descriptor for class: " + outputProtoClassName
                    + " not found");
        }
        PathReader pathReader = new PathReader(outputDescriptor, new ArrayList<>(Arrays.asList(columnNames)));
        return pathReader.fieldMaskingPath(fieldsToHash);
    }

    @Override
    public Row map(Row inputRow) {
        Row outPutRow = Row.copy(inputRow);
        for (String fieldPath : rowHasherMap.keySet()) {
            rowHasherMap.get(fieldPath).maskRow(outPutRow);
        }
        return outPutRow;
    }
}
