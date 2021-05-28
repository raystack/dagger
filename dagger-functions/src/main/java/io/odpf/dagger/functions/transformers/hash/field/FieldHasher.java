package io.odpf.dagger.functions.transformers.hash.field;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.Descriptors;

import java.io.Serializable;

public interface FieldHasher extends Serializable {
    Object maskRow(Object elem);

    boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor);

    FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor);

    default HashFunction getHashFunction() {
        return Hashing.sha256();
    }

    default boolean isValidNonRepeatedField(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor != null && !fieldDescriptor.isRepeated();
    }
}
