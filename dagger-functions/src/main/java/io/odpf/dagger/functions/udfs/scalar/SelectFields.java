package io.odpf.dagger.functions.udfs.scalar;

import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.longbow.MessageParser;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * The SelectFields udf.
 */
public class SelectFields extends ScalarUdf {
    private StencilClientOrchestrator stencilClientOrchestrator;
    private StencilClient stencilClient;
    private MessageParser messageParser;

    /**
     * Instantiates a new Select fields.
     *
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public SelectFields(StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.messageParser = new MessageParser();
    }

    /**
     * Instantiates a new Select fields.
     *
     * @param stencilClient the stencil client
     */
    public SelectFields(StencilClient stencilClient) {
        this.stencilClient = stencilClient;
        this.messageParser = new MessageParser();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (stencilClient == null) {
            stencilClient = getStencilClient();
        }
    }

    /**
     * This Udf is related to LongbowPlus and can be used alone or other two LongbowPlus UDFs(Filters and CondEq).
     * Can select a single field from the list of proto bytes output from LongbowRead phase.
     * Can be used with or without applying filters on top of LongbowRead output(which will be in repeated bytes).
     *
     * @param inputProtoBytes field containing the whole Data in LongbowRead(proto_data)
     * @param protoClassName  the proto class name of the selected inputData from LongbowRead
     * @param fieldPath       the field path
     * @return the corresponding values for the fieldPath of inputProtoBytes as a list
     * @throws InvalidProtocolBufferException the invalid protocol buffer exception
     * @throws ClassNotFoundException         the class not found exception
     * @author : arujit
     * @team : DE
     */
    public Object[] eval(Object inputProtoBytes, String protoClassName, String fieldPath) throws InvalidProtocolBufferException, ClassNotFoundException {
        if (!(inputProtoBytes instanceof ByteString[])) {
            return null;
        }
        ByteString[] inputData = (ByteString[]) inputProtoBytes;
        Descriptors.Descriptor descriptor = getDescriptor(protoClassName);

        ArrayList<Object> output = new ArrayList<>(inputData.length);
        List<String> keys = new LinkedList<>(Arrays.asList(fieldPath.split("\\.")));

        for (ByteString protoByte : inputData) {
            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protoByte);
            output.add(messageParser.read(dynamicMessage, keys));
        }
        return output.toArray(new Object[0]);
    }

    /**
     * Can select a single field from the list of filtered DynamicMessage output from LongbowRead phase.
     *
     * @param filteredData the filtered data
     * @param fieldPath    the field path
     * @return the corresponding values for the fieldPath of filteredDara as a list
     */
    public Object[] eval(List<DynamicMessage> filteredData, String fieldPath) {
        ArrayList<Object> output = new ArrayList<>(filteredData.size());
        List<String> keys = new LinkedList<>(Arrays.asList(fieldPath.split("\\.")));

        filteredData.forEach(dynamicMessage -> {
            output.add(messageParser.read(dynamicMessage, keys));
        });
        return output.toArray(new Object[0]);
    }

    private Descriptors.Descriptor getDescriptor(String protoClassName) throws ClassNotFoundException {
        Descriptors.Descriptor descriptor = stencilClient.get(protoClassName);
        if (descriptor == null) {
            throw new ClassNotFoundException(protoClassName);
        }
        return descriptor;
    }

    /**
     * Gets stencil client.
     *
     * @return the stencil client
     */
    public StencilClient getStencilClient() {
        if (stencilClient != null) {
            return stencilClient;
        }
        return stencilClientOrchestrator.getStencilClient();
    }

    public TypeInformation<Object[]> getResultType(Class<?>[] signature) {
        return TypeInformation.of(new TypeHint<Object[]>() {
        });
    }
}
