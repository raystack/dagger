package org.raystack.dagger.common.serde.typehandler;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.common.exceptions.DescriptorNotFoundException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

/**
 * The factory class for Type information.
 */
public class TypeInformationFactory {
    /**
     * Gets row type info.
     *
     * @param descriptor the descriptor
     * @return the row type
     */
    public static TypeInformation<Row> getRowType(Descriptors.Descriptor descriptor) {
        if (descriptor == null) {
            throw new DescriptorNotFoundException();
        }
        String[] fieldNames = descriptor
                .getFields()
                .stream()
                .map(Descriptors.FieldDescriptor::getName)
                .toArray(String[]::new);
        TypeInformation[] fieldTypes = descriptor
                .getFields()
                .stream()
                .map(fieldDescriptor -> TypeHandlerFactory.getTypeHandler(fieldDescriptor).getTypeInformation())
                .toArray(TypeInformation[]::new);
        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }
}
