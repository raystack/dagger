package io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor;

import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.Optional;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;

public class CustomDescriptor {
    private Descriptor descriptor;

    public CustomDescriptor(Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    public Optional<FieldDescriptor> getFieldDescriptor(String path) {
        if (descriptor == null) {
            return Optional.empty();
        }
        List<FieldDescriptor> allFieldDescriptors = descriptor.getFields();
        return allFieldDescriptors
                .stream()
                .filter(f -> f.getName().equals(path))
                .findFirst();
    }

    Optional<Descriptor> getDescriptor(String path) {
        Optional<Descriptors.FieldDescriptor> fieldDescriptorOptional = getFieldDescriptor(path);
        if (!fieldDescriptorOptional.isPresent()) {
            return Optional.empty();
        }
        FieldDescriptor fieldDescriptor = fieldDescriptorOptional.get();
        if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return Optional.of(fieldDescriptor.getMessageType());
        }
        return Optional.empty();
    }

    public Optional<CustomDescriptor> get(String path) {
        Optional<Descriptor> nextDescriptor = getDescriptor(path);
        return nextDescriptor.map(CustomDescriptor::new);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomDescriptor that = (CustomDescriptor) o;
        return descriptor != null ? descriptor.equals(that.descriptor) : that.descriptor == null;
    }

    @Override
    public int hashCode() {
        return descriptor != null ? descriptor.hashCode() : 0;
    }
}
