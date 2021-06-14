package io.odpf.dagger.functions.transformers.hash;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.transformers.hash.field.FieldHasher;
import io.odpf.dagger.functions.transformers.hash.field.RowHasher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Path reader.
 */
public class PathReader {

    private static Descriptors.Descriptor parentDescriptor;
    private List<String> inputColumns;

    /**
     * Instantiates a new Path reader.
     *
     * @param parentDescriptor the parent descriptor
     * @param inputColumns     the input columns
     */
    public PathReader(Descriptors.Descriptor parentDescriptor, List<String> inputColumns) {
        PathReader.parentDescriptor = parentDescriptor;
        this.inputColumns = inputColumns;
    }

    /**
     * Field masking path map.
     *
     * @param fieldsToHash the fields to hash
     * @return the map
     */
    public Map<String, RowHasher> fieldMaskingPath(List<String> fieldsToHash) {
        HashMap<String, RowHasher> elementNameMap = new HashMap<>();
        for (String fieldToHash : fieldsToHash) {
            String[] splittedFieldPath = fieldToHash.split("\\.");
            Descriptors.FieldDescriptor fieldDescriptor = parentDescriptor.findFieldByName(splittedFieldPath[0]);
            FieldHasher childHashProcessor = FieldHasherFactory
                    .createChildHasher(splittedFieldPath, fieldDescriptor);
            int rootIndex = inputColumns.indexOf(splittedFieldPath[0]);
            elementNameMap.put(fieldToHash, new RowHasher(rootIndex, childHashProcessor));
        }
        return elementNameMap;
    }
}
