package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_QUALIFIER_DEFAULT;

public class LongbowProtoData implements LongbowData, Serializable {
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String protoClassName;
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);


    public LongbowProtoData(StencilClientOrchestrator stencilClientOrchestrator, String protoClassName) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.protoClassName = protoClassName;
    }

    private Descriptors.Descriptor getProtoParser() {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(protoClassName);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }

    @Override
    public Map<String, List<DynamicMessage>> parse(List<Result> scanResult) {
        ArrayList<DynamicMessage> data = new ArrayList<>();

        try {
            for (int i = 0; i < scanResult.size(); i++) {
                DynamicMessage dynamicMessage = DynamicMessage.parseFrom(getProtoParser(),
                        (scanResult.get(i).getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(LONGBOW_QUALIFIER_DEFAULT))));
                data.add(i, dynamicMessage);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Unable to deserialize BigTable data: InvalidProtocolBufferException");
        }

        HashMap<String, List<DynamicMessage>> longbowData = new HashMap<>();
        longbowData.put("longbow_data", data);
        return longbowData;
    }
}
