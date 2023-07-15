package org.raystack.dagger.functions.transformers.hash;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.functions.transformers.hash.field.FieldHasher;
import org.raystack.dagger.functions.transformers.hash.field.IntegerFieldHasher;
import org.raystack.dagger.functions.transformers.hash.field.LongFieldHasher;
import org.raystack.dagger.functions.transformers.hash.field.RowHasher;
import org.raystack.dagger.functions.transformers.hash.field.StringFieldHasher;
import org.raystack.dagger.functions.transformers.hash.field.UnsupportedDataTypeHasher;

import java.util.Arrays;
import java.util.List;

/**
 * The factory class for Field hasher.
 */
public class FieldHasherFactory {

    /**
     * Create child hasher.
     *
     * @param fieldPath       the field path
     * @param fieldDescriptor the field descriptor
     * @return the field hasher
     */
    public static FieldHasher createChildHasher(String[] fieldPath, Descriptors.FieldDescriptor fieldDescriptor) {
        List<FieldHasher> fieldHashers = Arrays.asList(
                new StringFieldHasher(fieldPath),
                new IntegerFieldHasher(fieldPath),
                new LongFieldHasher(fieldPath),
                new RowHasher(fieldPath));

        return fieldHashers
                .stream()
                .filter(singleFieldHash -> singleFieldHash.canProcess(fieldDescriptor))
                .findFirst()
                .orElse(new UnsupportedDataTypeHasher(fieldPath))
                .setChild(fieldDescriptor);
    }
}
