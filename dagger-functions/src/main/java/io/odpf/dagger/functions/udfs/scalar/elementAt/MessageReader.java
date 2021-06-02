package io.odpf.dagger.functions.udfs.scalar.elementAt;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor.CustomDescriptor;
import io.odpf.dagger.functions.udfs.scalar.elementAt.row.Element;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.protobuf.Descriptors.Descriptor;

public class MessageReader {
    private Row message;
    private String protoClassName;
    private String pathOfMessage;
    private StencilClient stencilClient;

    public MessageReader(Row message, String protoClassName, String pathOfMessage, StencilClient stencilClient) {
        this.message = message;
        this.protoClassName = protoClassName;
        this.pathOfMessage = pathOfMessage;
        this.stencilClient = stencilClient;
    }

    private Descriptor getRootDescriptor() throws ClassNotFoundException {
        Descriptor dsc = stencilClient.get(protoClassName);
        if (dsc == null) {
            throw new ClassNotFoundException(protoClassName);
        }
        return dsc;
    }

    public Object read(String path) throws ClassNotFoundException {
        List<String> pathElements = Arrays.asList(path.split("\\."));
        CustomDescriptor rootCustomDescriptor = new CustomDescriptor(getRootDescriptor());
        Element lastChildElement = null;
        for (String pathElement : pathElements) {
            if (lastChildElement == null) {
                Optional<CustomDescriptor> parentCustomDescriptor = rootCustomDescriptor.get(pathOfMessage);
                if (!parentCustomDescriptor.isPresent()) {
                    return "";
                }
                Optional<Element> child = Element.initialize(null, message, parentCustomDescriptor.get(), pathElement);
                if (!child.isPresent()) {
                    return "";
                }
                lastChildElement = child.get();
            } else {
                Optional<Element> next = lastChildElement.createNext(pathElement);
                if (!next.isPresent()) {
                    return "";
                }
                lastChildElement = next.get();
            }
        }
        return lastChildElement.fetch();
    }
}
