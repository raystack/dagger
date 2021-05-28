package io.odpf.dagger.functions.transformers.hash;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.transformers.hash.field.FieldHasher;
import io.odpf.dagger.functions.transformers.hash.field.RowHasher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathReader {

    private static Descriptors.Descriptor parentDescriptor;
    private List<String> inputColumns;

    public PathReader(Descriptors.Descriptor parentDescriptor, List<String> inputColumns) {
        PathReader.parentDescriptor = parentDescriptor;
        this.inputColumns = inputColumns;
    }

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
