package com.gotocompany.dagger.core.utils;

import com.google.protobuf.Descriptors;

import java.util.Arrays;

/**
 * Utility class that contains helper methods to get {@link Descriptors} {@link  Descriptors.FieldDescriptor}.
 */
public class DescriptorsUtil {

    /**
     * Gets FieldDescriptor .
     *
     * @param descriptor the descriptor
     * @param columnName  the columnName
     * @return the fieldDescriptor
     */
    public static Descriptors.FieldDescriptor getFieldDescriptor(Descriptors.Descriptor descriptor, String columnName) {
        String[] nestedFields = columnName.split("\\.");
        if (nestedFields.length == 1) {
            return descriptor.findFieldByName(columnName);
        } else {
            return getNestedFieldDescriptor(descriptor, nestedFields);
        }
    }

    /**
     * Gets FieldDescriptor .
     *
     * @param parentDescriptor the descriptor
     * @param nestedColumnNames the array of columnNames
     * @return the fieldDescriptor
     */
    public static Descriptors.FieldDescriptor getNestedFieldDescriptor(Descriptors.Descriptor parentDescriptor, String[] nestedColumnNames) {
        String childColumnName = nestedColumnNames[0];
        if (nestedColumnNames.length == 1) {
            return parentDescriptor.findFieldByName(childColumnName);
        }
        Descriptors.FieldDescriptor childFieldDescriptor = parentDescriptor.findFieldByName(childColumnName);
        if (childFieldDescriptor == null) {
            throw new IllegalArgumentException(String.format("Field Descriptor not found for field: %s in the proto of %s", childColumnName, parentDescriptor.getFullName()));
        }
        Descriptors.Descriptor childDescriptor = childFieldDescriptor.getMessageType();
        return getNestedFieldDescriptor(childDescriptor, Arrays.copyOfRange(nestedColumnNames, 1, nestedColumnNames.length));
    }
}
