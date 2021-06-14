package io.odpf.dagger.functions.udfs.scalar.elementAt.row;


import com.google.protobuf.Descriptors;
import org.apache.flink.types.Row;

import java.util.Optional;

/**
 * The Value element.
 */
class ValueElement extends Element {

    /**
     * Instantiates a new Value element.
     *
     * @param parent          the parent
     * @param row             the row
     * @param fieldDescriptor the field descriptor
     */
    ValueElement(Element parent, Row row, Descriptors.FieldDescriptor fieldDescriptor) {
        super(parent, row, fieldDescriptor);
    }

    @Override
    public Optional<Element> createNext(String pathElement) {
        return Optional.empty();
    }
}
