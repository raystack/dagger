package io.odpf.dagger.functions.transformers.hash.field;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.transformers.hash.FieldHasherFactory;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * The Row hasher.
 */
public class RowHasher implements FieldHasher {

    private String[] splittedFieldPath;
    private FieldHasher child;
    private int childIndex;

    /**
     * Instantiates a new Row hasher.
     *
     * @param splittedFieldPath the splitted field path
     */
    public RowHasher(String[] splittedFieldPath) {
        this.splittedFieldPath = splittedFieldPath;
    }

    /**
     * Instantiates a new Row hasher with specified child index.
     *
     * @param childIndex the child index
     * @param child      the child
     */
    public RowHasher(int childIndex, FieldHasher child) {
        this.child = child;
        this.childIndex = childIndex;
    }

    @Override
    public Object maskRow(Object elem) {
        Row currentRow = (Row) elem;
        currentRow.setField(childIndex, child.maskRow(currentRow.getField(this.childIndex)));
        return currentRow;
    }

    @Override
    public boolean canProcess(Descriptors.FieldDescriptor fieldDescriptor) {
        return splittedFieldPath.length > 1
                && isValidNonRepeatedField(fieldDescriptor)
                && fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE;
    }

    @Override
    public FieldHasher setChild(Descriptors.FieldDescriptor fieldDescriptor) {
        if (child == null) {
            this.child = createChild(fieldDescriptor);
        }
        return this;
    }

    private FieldHasher createChild(Descriptors.FieldDescriptor fieldDescriptor) {
        String[] childColumnPath = Arrays.copyOfRange(splittedFieldPath, 1, splittedFieldPath.length);
        String childField = childColumnPath[0];
        Descriptors.FieldDescriptor childFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName(childField);
        FieldHasher childHasher = FieldHasherFactory.createChildHasher(childColumnPath, childFieldDescriptor);
        childIndex = childFieldDescriptor.getIndex();
        return childHasher;
    }
}
