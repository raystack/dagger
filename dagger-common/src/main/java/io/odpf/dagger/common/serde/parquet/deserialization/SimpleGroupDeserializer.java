package io.odpf.dagger.common.serde.parquet.deserialization;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.common.exceptions.serde.SimpleGroupParsingException;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoType;
import io.odpf.dagger.common.serde.typehandler.RowFactory;
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
        String timestampFieldName = fieldDescriptor.getName();
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, timestampFieldName)) {
            long timeInMillis = simpleGroup.getLong(timestampFieldName, 0);
            Instant instant = Instant.ofEpochMilli(timeInMillis);

            row.setField(row.getArity() - 2, true);
            row.setField(row.getArity() - 1, Timestamp.from(instant));
            return row;
        } else {
            String errMessage = String.format("Could not extract timestamp with field name %s from simple group of type %s",
                    fieldDescriptor.getName(), simpleGroup.getType().toString());
            throw new SimpleGroupParsingException(errMessage);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.typeInformation;
    }
}
