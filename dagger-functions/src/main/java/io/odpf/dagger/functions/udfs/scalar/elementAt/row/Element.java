package io.odpf.dagger.functions.udfs.scalar.elementAt.row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor.CustomDescriptor;
import org.apache.flink.types.Row;

import java.util.Optional;

public abstract class Element {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private Element parent;
    private Row row;

    Element(Element parent, Row row, Descriptors.FieldDescriptor fieldDescriptor) {
        this.parent = parent;
        this.row = row;
        this.fieldDescriptor = fieldDescriptor;
    }

    public static Optional<Element> initialize(Element parent, Row row, CustomDescriptor parentDescriptor, String pathElement) {
        Optional<Descriptors.FieldDescriptor> fieldDescriptor = parentDescriptor.getFieldDescriptor(pathElement);
        if (!fieldDescriptor.isPresent()) {
            return Optional.empty();
        }
        if (fieldDescriptor.get().getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return Optional.of(new RowElement(parent, row, fieldDescriptor.get()));
        }
        return Optional.of(new ValueElement(parent, row, fieldDescriptor.get()));
    }

    public Descriptors.FieldDescriptor getFieldDescriptor() {
        return fieldDescriptor;
    }

    public abstract Optional<Element> createNext(String pathElement);

    public Object fetch() {
        if (parent != null) {
            row = (Row) parent.fetch();
        }

        int fieldIndex = fieldDescriptor.getIndex();
        return row.getField(fieldIndex);
    }

}
