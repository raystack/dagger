package com.gojek.daggers.source;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DaggerDeserializationException;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.protohandler.RowFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class ProtoDeserializer implements KafkaDeserializationSchema<Row> {

    private String protoClassName;
    private ProtoType protoType;
    private int timestampFieldIndex;
    private StencilClientOrchestrator stencilClientOrchestrator;

    public ProtoDeserializer(String protoClassName, int timestampFieldIndex, String rowtimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator) {
        this.protoClassName = protoClassName;
        this.protoType = new ProtoType(protoClassName, rowtimeAttributeName, stencilClientOrchestrator);
        this.timestampFieldIndex = timestampFieldIndex;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public Row deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws IOException {
        try {
            Descriptors.Descriptor descriptor = getProtoParser();
            DynamicMessage proto = DynamicMessage.parseFrom(descriptor, consumerRecord.value());
            return addTimestampFieldToRow(RowFactory.createRow(proto), proto);
        } catch (DescriptorNotFoundException e) {
            throw new DescriptorNotFoundException(e);
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidProtocolBufferException(e);
        } catch (RuntimeException e) {
            throw new DaggerDeserializationException(e);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return protoType.getRowType();
    }

    private Descriptors.Descriptor getProtoParser() {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(protoClassName);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }

    private Row addTimestampFieldToRow(Row row, DynamicMessage proto) {
        Descriptors.FieldDescriptor fieldDescriptor = proto.getDescriptorForType().findFieldByNumber(timestampFieldIndex);
        Row finalRecord = new Row(row.getArity() + 1);
        for (int fieldIndex = 0; fieldIndex < row.getArity(); fieldIndex++) {
            finalRecord.setField(fieldIndex, row.getField(fieldIndex));
        }
        DynamicMessage timestampProto = (DynamicMessage) proto.getField(fieldDescriptor);
        List<Descriptors.FieldDescriptor> timestampFields = timestampProto.getDescriptorForType().getFields();

        long timestampSeconds = (long) timestampProto.getField(timestampFields.get(0));
        long timestampNanos = (int) timestampProto.getField(timestampFields.get(1));

        finalRecord.setField(finalRecord.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(timestampSeconds, timestampNanos)));
        return finalRecord;
    }
}
