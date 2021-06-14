package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.exceptions.RowHashException;

import java.nio.charset.StandardCharsets;

/**
 * The String field hasher.
 */
public class StringFieldHasher implements FieldHasher {

    private final String[] fieldPath;

    /**
     * Instantiates a new String field hasher.
     *
     * @param fieldPath the field path
     */
    public StringFieldHasher(String[] fieldPath) {
        this.fieldPath = fieldPath;
    }

    @Override
    public Object maskRow(Object elem) {
        try {
            String fieldValue = getHashFunction()
                    .hashString((String) elem, StandardCharsets.UTF_8)
                    .toString();
            return fieldValue;
        } catch (Exception ex) {
            throw new RowHashException("Unable to hash String value for field : " + fieldPath[0], ex);
        }
    }

    @Override
    public boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldPath.length == 1
                && isValidNonRepeatedField(fieldDescriptor)
                && fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING;
    }

    @Override
    public FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor) {
        return this;
    }
}
