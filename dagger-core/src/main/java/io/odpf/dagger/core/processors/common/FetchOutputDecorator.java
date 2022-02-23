package io.odpf.dagger.core.processors.common;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.protohandler.ProtoHandlerFactory;
import io.odpf.dagger.core.processors.types.MapDecorator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;

import static io.odpf.dagger.common.core.Constants.ROWTIME;

/**
 * The Fetch output decorator.
 */
public class FetchOutputDecorator implements MapDecorator {


    private String[] outputColumnNames;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String outputProtoClassName;
    private boolean hasSQLTransformer;

    /**
     * Instantiates a new Fetch output decorator.
     *
     * @param schemaConfig      the schema config
     * @param hasSQLTransformer the has sql transformer
     */
    public FetchOutputDecorator(SchemaConfig schemaConfig, boolean hasSQLTransformer) {
        this.outputColumnNames = schemaConfig.getColumnNameManager().getOutputColumnNames();
        this.stencilClientOrchestrator = schemaConfig.getStencilClientOrchestrator();
        this.outputProtoClassName = schemaConfig.getOutputProtoClassName();
        this.hasSQLTransformer = hasSQLTransformer;
    }

    @Override
    public Boolean canDecorate() {
        return false;
    }

    @Override
    public Row map(Row input) {
        RowManager rowManager = new RowManager(input);
        return hasSQLTransformer ? convertLocalDateTime(rowManager.getOutputData()) : rowManager.getOutputData();
    }


    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        return hasSQLTransformer ? inputStream.map(this).returns(getTypeInformation()) : inputStream.map(this);
    }

    private TypeInformation<Row> getTypeInformation() {
        TypeInformation[] typeInformations = new TypeInformation[outputColumnNames.length];
        Arrays.fill(typeInformations, TypeInformation.of(Object.class));
        Descriptors.Descriptor descriptor = getDescriptor();
        if (descriptor != null) {
            for (int index = 0; index < outputColumnNames.length; index++) {
                String outputColumnName = outputColumnNames[index];
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(outputColumnName);
                typeInformations[index] = fieldDescriptor != null
                        ? ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeInformation()
                        : outputColumnName.equals(ROWTIME) ? Types.SQL_TIMESTAMP : TypeInformation.of(Object.class);
            }
        }
        return new RowTypeInfo(typeInformations, outputColumnNames);
    }

    private Descriptors.Descriptor getDescriptor() {
        return stencilClientOrchestrator.getStencilClient().get(outputProtoClassName);
    }

    private Row convertLocalDateTime(Row row) {
        Row outputRow = new Row(row.getArity());
        for (int index = 0; index < outputColumnNames.length; index++) {
            outputRow.setField(index, row.getField(index));
            if (outputColumnNames[index].equals(ROWTIME)) {
                Object timestampValue = outputRow.getField(index);
                if (timestampValue != null) {
                    outputRow.setField(index, Timestamp.valueOf((LocalDateTime) timestampValue));
                }
            }
        }
        return outputRow;
    }

}
