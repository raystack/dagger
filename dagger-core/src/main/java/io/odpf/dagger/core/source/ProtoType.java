package io.odpf.dagger.core.source;

import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.core.exception.DescriptorNotFoundException;
import io.odpf.dagger.core.protohandler.TypeInformationFactory;
import io.odpf.dagger.core.utils.Constants;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class ProtoType implements Serializable {

    private transient Descriptor protoFieldDescriptor;
    private String protoClassName;
    private String rowtimeAttributeName;
    private StencilClientOrchestrator stencilClientOrchestrator;

    public ProtoType(String protoClassName, String rowtimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.protoClassName = protoClassName;
        this.rowtimeAttributeName = rowtimeAttributeName;
    }

    public TypeInformation<Row> getRowType() {
        TypeInformation<Row> rowNamed = TypeInformationFactory.getRowType(getProtoFieldDescriptor());
        RowTypeInfo rowTypeInfo = (RowTypeInfo) rowNamed;
        ArrayList<String> fieldNames = new ArrayList<>(Arrays.asList(rowTypeInfo.getFieldNames()));
        ArrayList<TypeInformation> fieldTypes = new ArrayList<>(Arrays.asList(rowTypeInfo.getFieldTypes()));
        fieldNames.add(Constants.INTERNAL_VALIDATION_FILED);
        fieldTypes.add(Types.BOOLEAN);
        fieldNames.add(rowtimeAttributeName);
        fieldTypes.add(Types.SQL_TIMESTAMP);
        return Types.ROW_NAMED(fieldNames.toArray(new String[0]), fieldTypes.toArray(new TypeInformation[0]));
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
