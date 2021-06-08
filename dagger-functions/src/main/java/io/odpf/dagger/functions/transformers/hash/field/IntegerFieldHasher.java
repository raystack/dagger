package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.exceptions.RowHashException;

public class IntegerFieldHasher implements FieldHasher {

    private final String[] fieldPath;

    public IntegerFieldHasher(String[] fieldPath) {
        this.fieldPath = fieldPath;
    }

    @Override
    public Object maskRow(Object elem) {
        try {
            int fieldValue = getHashFunction()
                    .hashInt((Integer) elem)
                    .asInt();
            return fieldValue;
        } catch (Exception ex) {
            throw new RowHashException("Unable to hash int value for field : " + fieldPath[0], ex);
        }
    }

    @Override
    public boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldPath.length == 1
                && isValidNonRepeatedField(fieldDescriptor)
                && fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT;
    }

    @Override
    public FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor) {
        return this;
    }
}
