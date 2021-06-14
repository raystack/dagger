package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.exceptions.InvalidHashFieldException;

/**
 * The Unsupported data type hasher.
 */
public class UnsupportedDataTypeHasher implements FieldHasher {
    private String[] fieldPath;

    /**
     * Instantiates a new Unsupported data type hasher.
     *
     * @param fieldPath the field path
     */
    public UnsupportedDataTypeHasher(String[] fieldPath) {
        this.fieldPath = fieldPath;
    }

    @Override
    public Object maskRow(Object elem) {
        return elem;
    }

    @Override
    public boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor) {
        return false;
    }

    @Override
    public FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldPath.length == 0 || fieldDescriptor == null) {
            throw new InvalidHashFieldException("No primitive field found for hashing");
        } else {
            throw new InvalidHashFieldException("Inner Field : " + fieldPath[0] + " of data type : " + fieldDescriptor.getJavaType()
                    + " not currently supported for hashing");
        }
    }
}
