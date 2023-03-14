package com.gotocompany.dagger.common.serde.parquet.deserialization;

import com.google.protobuf.Descriptors;
import com.gotocompany.dagger.common.exceptions.DescriptorNotFoundException;
import com.gotocompany.dagger.common.exceptions.serde.DaggerDeserializationException;
import com.gotocompany.dagger.common.serde.proto.deserialization.ProtoType;
import com.gotocompany.dagger.common.serde.typehandler.RowFactory;
import com.gotocompany.dagger.common.serde.typehandler.complex.TimestampHandler;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.common.serde.DaggerDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.sql.Timestamp;
import java.time.Instant;

public class SimpleGroupDeserializer implements DaggerDeserializer<Row> {
    private final String protoClassName;
    private final int timestampFieldIndex;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final TypeInformation<Row> typeInformation;

    public SimpleGroupDeserializer(String protoClassName, int timestampFieldIndex, String rowtimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator) {
        this.protoClassName = protoClassName;
        this.timestampFieldIndex = timestampFieldIndex;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.typeInformation = new ProtoType(protoClassName, rowtimeAttributeName, stencilClientOrchestrator).getRowType();
    }

    private Descriptors.Descriptor getProtoParser() {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(protoClassName);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }

    public Row deserialize(SimpleGroup simpleGroup) {
        Descriptors.Descriptor descriptor = getProtoParser();
        try {
            Row row = RowFactory.createRow(descriptor, simpleGroup, 2);
            return addTimestampFieldToRow(row, simpleGroup, descriptor);
        } catch (RuntimeException e) {
            throw new DaggerDeserializationException(e);
        }
    }

    private Row addTimestampFieldToRow(Row row, SimpleGroup simpleGroup, Descriptors.Descriptor descriptor) {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(timestampFieldIndex);
        TimestampHandler timestampHandler = new TimestampHandler(fieldDescriptor);
        Row timestampRow = (Row) timestampHandler.transformFromParquet(simpleGroup);
        long seconds = timestampRow.getFieldAs(0);
        int nanos = timestampRow.getFieldAs(1);

        row.setField(row.getArity() - 2, true);
        row.setField(row.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(seconds, nanos)));
        return row;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.typeInformation;
    }
}
