package com.gotocompany.dagger.common.serde.proto.deserialization;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.common.exceptions.DescriptorNotFoundException;
import com.gotocompany.dagger.common.exceptions.serde.DaggerDeserializationException;
import com.gotocompany.dagger.common.serde.DaggerDeserializer;
import com.gotocompany.dagger.common.serde.typehandler.RowFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Deserializer for protobuf messages.
 */
public class ProtoDeserializer implements KafkaDeserializationSchema<Row>, DaggerDeserializer<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoDeserializer.class);
    private final String protoClassName;
    private final int timestampFieldIndex;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final TypeInformation<Row> typeInformation;
    private final FieldDescriptorCache fieldDescriptorCache;
    private final boolean stencilAutoRefreshEnable;

    /**
     * Instantiates a new Proto deserializer.
     *
     * @param protoClassName            the proto class name
     * @param timestampFieldIndex       the timestamp field index
     * @param rowtimeAttributeName      the rowtime attribute name
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public ProtoDeserializer(String protoClassName, int timestampFieldIndex, String rowtimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator) {
        this.protoClassName = protoClassName;
        this.timestampFieldIndex = timestampFieldIndex;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.typeInformation = new ProtoType(protoClassName, rowtimeAttributeName, stencilClientOrchestrator).getRowType();
        this.fieldDescriptorCache = new FieldDescriptorCache(getProtoParser());
        this.stencilAutoRefreshEnable = stencilClientOrchestrator.createStencilConfig().getCacheAutoRefresh();
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public Row deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        Descriptors.Descriptor descriptor = getProtoParser();
        try {
            DynamicMessage proto = DynamicMessage.parseFrom(descriptor, consumerRecord.value());
            return addTimestampFieldToRow(proto);
        } catch (DescriptorNotFoundException e) {
            throw new DescriptorNotFoundException(e);
        } catch (InvalidProtocolBufferException e) {
            LOGGER.warn("Invalid Row encountered for proto " + protoClassName, e);
            return createDefaultInvalidRow(DynamicMessage.getDefaultInstance(descriptor));
        } catch (RuntimeException e) {
            throw new DaggerDeserializationException(e);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.typeInformation;
    }

    private Descriptors.Descriptor getProtoParser() {
        Descriptors.Descriptor dsc = stencilClientOrchestrator.getStencilClient().get(protoClassName);
        if (dsc == null) {
            throw new DescriptorNotFoundException();
        }
        return dsc;
    }

    private Row createDefaultInvalidRow(DynamicMessage defaultInstance) {
        Row row;
        if (stencilAutoRefreshEnable) {
            row = RowFactory.createRow(defaultInstance, 2, fieldDescriptorCache);
        } else {
            row = RowFactory.createRow(defaultInstance, 2);
        }
        row.setField(row.getArity() - 2, false);
        row.setField(row.getArity() - 1, new Timestamp(0));
        return row;
    }

    private Row addTimestampFieldToRow(DynamicMessage proto) {
        Row finalRecord;
        if (stencilAutoRefreshEnable) {
            finalRecord = RowFactory.createRow(proto, 2, fieldDescriptorCache);
        } else {
            finalRecord = RowFactory.createRow(proto, 2);
        }

        Descriptors.FieldDescriptor fieldDescriptor = proto.getDescriptorForType().findFieldByNumber(timestampFieldIndex);
        DynamicMessage timestampProto = (DynamicMessage) proto.getField(fieldDescriptor);
        List<Descriptors.FieldDescriptor> timestampFields = timestampProto.getDescriptorForType().getFields();

        long timestampSeconds = (long) timestampProto.getField(timestampFields.get(0));
        long timestampNanos = (int) timestampProto.getField(timestampFields.get(1));

        finalRecord.setField(finalRecord.getArity() - 2, true);
        finalRecord.setField(finalRecord.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(timestampSeconds, timestampNanos)));
        return finalRecord;
    }


}
