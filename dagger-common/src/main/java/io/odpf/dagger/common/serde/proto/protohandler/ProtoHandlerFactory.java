package io.odpf.dagger.common.serde.proto.protohandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeID;
import io.odpf.dagger.common.serde.parquet.parser.ParquetDataTypeParser;
import io.odpf.dagger.common.serde.parquet.parser.primitive.*;
import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The factory class for Proto handler.
 */
public class ProtoHandlerFactory {
    private static Map<String, ProtoHandler> protoHandlerMap = new ConcurrentHashMap<>();
    private static Map<String, ParquetDataTypeParser> parquetDataTypeParserMap = new ConcurrentHashMap<>();
    private static SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();


    /**
     * Gets proto handler.
     *
     * @param fieldDescriptor the field descriptor
     * @return the proto handler
     */
    public static ProtoHandler getProtoHandler(final Descriptors.FieldDescriptor fieldDescriptor) {
        return protoHandlerMap.computeIfAbsent(fieldDescriptor.getFullName(),
                k -> getSpecificHandlers(fieldDescriptor).stream().filter(ProtoHandler::canHandle)
                        .findFirst().orElseGet(() -> new PrimitiveProtoHandler(fieldDescriptor)));
    }

    public static ParquetDataTypeParser getParquetDataTypeParser(SimpleGroup simpleGroup, String fieldName) {
        validate(simpleGroup, fieldName);
        PrimitiveType.PrimitiveTypeName fieldPrimitiveType = simpleGroup.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
        LogicalTypeAnnotation logicalType = simpleGroup.getType().getType(fieldName).getLogicalTypeAnnotation();
        ParquetDataTypeID parquetDataTypeID = new ParquetDataTypeID(fieldPrimitiveType, logicalType);
        return parquetDataTypeParserMap.computeIfAbsent(parquetDataTypeID.toString(),
                fieldDataTypeID ->
                        getPrimitiveHandlers()
                                .stream()
                                .filter(parquetDataTypeParser -> parquetDataTypeParser.canHandle(simpleGroup, fieldName))
                                .findFirst()
                                .orElseThrow(() -> {
                                    String message = String.format("Error: Could not find a compatible parser to parse " +
                                            "field %s in the simple group of primitive type %s and logical type %s", fieldName,
                                            fieldPrimitiveType, logicalType);
                                    return new DaggerDeserializationException(message);
                                }));
    }

    private static void validate(SimpleGroup simpleGroup, String fieldName) {
        if (!simpleGroupValidation.checkSimpleGroupNotNull(simpleGroup)) {
            throw new DaggerDeserializationException("Error: SimpleGroup argument is null");
        }
        if (!simpleGroupValidation.checkFieldExistsInSimpleGroup(simpleGroup, fieldName)) {
            throw new DaggerDeserializationException("Error: Field name " + fieldName + " does not exist in SimpleGroup argument");
        }
        if (!simpleGroupValidation.checkFieldIsPrimitive(simpleGroup, fieldName)) {
            throw new DaggerDeserializationException("Error: Field name " + fieldName + " is not a primitive type");
        }
    }

    /**
     * Clear proto handler map.
     */
    protected static void clearProtoHandlerMap() {
        protoHandlerMap.clear();
    }

    protected static void clearParquetDataTypeParserMap() {
        parquetDataTypeParserMap.clear();
    }

    private static List<ParquetDataTypeParser> getPrimitiveHandlers() {
        return Arrays.asList(
                new ParquetBooleanParser(simpleGroupValidation),
                new ParquetDoubleParser(simpleGroupValidation),
                new ParquetFloatParser(simpleGroupValidation),
                new ParquetInt32Parser(simpleGroupValidation),
                new ParquetInt64Parser(simpleGroupValidation),
                new ParquetBinaryEnumParser(simpleGroupValidation),
                new ParquetBinaryStringParser(simpleGroupValidation),
                new ParquetTimestampParser(simpleGroupValidation)
        );
    }

    private static List<ProtoHandler> getSpecificHandlers(Descriptors.FieldDescriptor fieldDescriptor) {
        return Arrays.asList(
                new MapProtoHandler(fieldDescriptor),
                new TimestampProtoHandler(fieldDescriptor),
                new EnumProtoHandler(fieldDescriptor),
                new StructMessageProtoHandler(fieldDescriptor),
                new RepeatedStructMessageProtoHandler(fieldDescriptor),
                new RepeatedPrimitiveProtoHandler(fieldDescriptor),
                new RepeatedMessageProtoHandler(fieldDescriptor),
                new RepeatedEnumProtoHandler(fieldDescriptor),
                new MessageProtoHandler(fieldDescriptor)
        );
    }
}
