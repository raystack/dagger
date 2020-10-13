package com.gojek.daggers.postProcessors.external.common;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.external.SchemaConfig;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.gojek.daggers.utils.Constants;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class FetchOutputDecorator implements MapDecorator {


    private String[] outputColumnNames;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String outputProtoClassName;

    public FetchOutputDecorator(SchemaConfig schemaConfig) {
        this.outputColumnNames = schemaConfig.getColumnNameManager().getOutputColumnNames();
        this.stencilClientOrchestrator = schemaConfig.getStencilClientOrchestrator();
        this.outputProtoClassName = schemaConfig.getOutputProtoClassName();
    }

    @Override
    public Boolean canDecorate() {
        return false;
    }

    @Override
    public Row map(Row input) {
        RowManager rowManager = new RowManager(input);
        return rowManager.getOutputData();
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.map(this).returns(getTypeInformation());
    }

    private TypeInformation<Row> getTypeInformation() {
        TypeInformation[] typeInformations = new TypeInformation[outputColumnNames.length];
        Arrays.fill(typeInformations, TypeInformation.of(Object.class));
        Descriptors.Descriptor descriptor = getDescriptor();
        if (descriptor != null) {
            for (int index = 0; index < outputColumnNames.length; index++) {
                String outputColumnName = outputColumnNames[index];
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(outputColumnName);
                typeInformations[index] = fieldDescriptor != null ?
                        ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeInformation() :
                        outputColumnName.equals(Constants.ROWTIME) ? Types.SQL_TIMESTAMP : TypeInformation.of(Object.class);
            }
        }
        return new RowTypeInfo(typeInformations, outputColumnNames);
    }

    private Descriptors.Descriptor getDescriptor() {
        return stencilClientOrchestrator.getStencilClient().get(outputProtoClassName);
    }


}
