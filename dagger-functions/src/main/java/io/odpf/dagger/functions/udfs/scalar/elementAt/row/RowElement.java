package io.odpf.dagger.functions.udfs.scalar.elementAt.row;

import io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor.CustomDescriptor;
import org.apache.flink.types.Row;

import java.util.Optional;

import static com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * The Row element.
 */
class RowElement extends Element {

    /**
     * Instantiates a new Row element.
     *
     * @param parent          the parent
     * @param row             the row
     * @param fieldDescriptor the field descriptor
     */
    RowElement(Element parent, Row row, FieldDescriptor fieldDescriptor) {
        super(parent, row, fieldDescriptor);
    }

    public Optional<Element> createNext(String pathElement) {
        Optional<Element> childElement = initialize(this, null, new CustomDescriptor(getFieldDescriptor().getMessageType()), pathElement);
        return childElement;
    }
}
