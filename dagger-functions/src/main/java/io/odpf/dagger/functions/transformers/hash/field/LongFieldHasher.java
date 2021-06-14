package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.exceptions.RowHashException;

/**
 * The Long field hasher.
 */
public class LongFieldHasher implements FieldHasher {

    private final String[] fieldPath;

    /**
     * Instantiates a new Long field hasher.
     *
     * @param fieldPath the field path
     */
    public LongFieldHasher(String[] fieldPath) {
        this.fieldPath = fieldPath;
    }

    @Override
    public Object maskRow(Object elem) {
        try {
            long fieldValue = getHashFunction()
                    .hashLong((Long) elem)
                    .asLong();
            return fieldValue;
        } catch (Exception ex) {
            throw new RowHashException("Unable to hash long value for field : " + fieldPath[0], ex);
        }
    }

    @Override
    public boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldPath.length == 1
                && isValidNonRepeatedField(fieldDescriptor)
                && fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG;
    }

    @Override
    public FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor) {
        return this;
    }
}
