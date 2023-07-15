package org.raystack.dagger.common.serde.proto.deserialization;

import org.raystack.dagger.common.exceptions.DescriptorNotFoundException;
import org.raystack.dagger.common.serde.typehandler.TypeInformationFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.serde.DaggerInternalTypeInformation;

import java.io.Serializable;

/**
 * Getting TypeInformation required for Flink from the proto.
 */
public class ProtoType implements Serializable, DaggerInternalTypeInformation {

    private transient Descriptor protoFieldDescriptor;
    private String protoClassName;
    private String rowtimeAttributeName;
    private StencilClientOrchestrator stencilClientOrchestrator;

    /**
     * Instantiates a new Proto type.
     *
     * @param protoClassName            the proto class name
     * @param rowtimeAttributeName      the rowtime attribute name
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public ProtoType(String protoClassName, String rowtimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.protoClassName = protoClassName;
        this.rowtimeAttributeName = rowtimeAttributeName;
    }

    /**
     * Gets row type info.
     *
     * @return the row type info
     */
    public TypeInformation<Row> getRowType() {
        TypeInformation<Row> rowNamed = TypeInformationFactory.getRowType(getProtoFieldDescriptor());
        return addInternalFields(rowNamed, rowtimeAttributeName);
    }

    private Descriptor getProtoFieldDescriptor() {
        if (protoFieldDescriptor == null) {
            protoFieldDescriptor = createFieldDescriptor();
        }
        return protoFieldDescriptor;
    }

    private Descriptor createFieldDescriptor() {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(protoClassName);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }
}
