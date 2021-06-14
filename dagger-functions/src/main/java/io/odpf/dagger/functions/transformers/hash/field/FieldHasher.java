package io.odpf.dagger.functions.transformers.hash.field;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.Descriptors;

import java.io.Serializable;

/**
 * The interface Field hasher.
 */
public interface FieldHasher extends Serializable {
    /**
     * Mask row object.
     *
     * @param elem the elem
     * @return the object
     */
    Object maskRow(Object elem);

    /**
     * Check if can process the field hasher.
     *
     * @param fieldDescriptor the field descriptor
     * @return the boolean
     */
    boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor);

    /**
     * Sets child.
     *
     * @param fieldDescriptor the field descriptor
     * @return the child
     */
    FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor);

    /**
     * Gets hash function.
     *
     * @return the hash function
     */
    default HashFunction getHashFunction() {
        return Hashing.sha256();
    }

    /**
     * Check if field descriptor is valid and non repeated field.
     *
     * @param fieldDescriptor the field descriptor
     * @return the boolean
     */
    default boolean isValidNonRepeatedField(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor != null && !fieldDescriptor.isRepeated();
    }
}
