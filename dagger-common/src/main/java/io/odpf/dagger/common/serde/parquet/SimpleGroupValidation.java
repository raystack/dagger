package io.odpf.dagger.common.serde.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class SimpleGroupValidation {
    public static boolean checkFieldExistsAndIsInitialized(SimpleGroup simpleGroup, String fieldName) {
        return simpleGroup.getType().containsField(fieldName) && simpleGroup.getFieldRepetitionCount(fieldName) != 0;
    }

    /**
     * This method checks if the map field inside the simple group is
     * serialized using this legacy format:
     * {@code
     * <pre>
     *     repeated group &lt;name&gt; {
     *      &lt;repetition-type&gt; &lt;data-type&gt; key;
     *      &lt;repetition-type&gt; &lt;data-type&gt; value;
     *    }
     * </pre>
     * }
     * The outer group is always repeated. key and value are constant field names.
     *
     * @param simpleGroup The SimpleGroup object inside which the map field is present
     * @param fieldName   The name of the map field
     * @return true, if the map structure follows the spec and false otherwise.
     */
    public static boolean checkIsLegacySimpleGroupMap(SimpleGroup simpleGroup, String fieldName) {
        if (!(simpleGroup.getType().getType(fieldName) instanceof GroupType)) {
            return false;
        }
        GroupType nestedMapGroupType = simpleGroup.getType().getType(fieldName).asGroupType();
        return nestedMapGroupType.isRepetition(Type.Repetition.REPEATED)
                && nestedMapGroupType.getFieldCount() == 2
                && nestedMapGroupType.containsField("key")
                && nestedMapGroupType.containsField("value");
    }

    /**
     * This method checks if the map field inside the simple group is
     * serialized using this standard parquet map specification:
     * {@code
     * <pre>
     *         &lt;repetition-type&gt; group &lt;name&gt; (MAP) {
     *           repeated group key_value {
     *               required &lt;data-type&gt; key;
     *               &lt;repetition-type&gt; &lt;data-type&gt; value;
     *           }
     *          }
     * </pre>
     * }
     * The validation checks below follow the <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps">Apache Parquet LogicalTypes Specification</a>for Maps.
     *
     * @param simpleGroup The SimpleGroup object inside which the map field is present
     * @param fieldName   The name of the map field
     * @return true, if the map structure follows the spec and false otherwise.
     */
    public static boolean checkIsStandardSimpleGroupMap(SimpleGroup simpleGroup, String fieldName) {
        return applyMapFieldValidations(simpleGroup, fieldName)
                && applyNestedKeyValueFieldValidations(simpleGroup, fieldName);
    }

    private static boolean applyMapFieldValidations(SimpleGroup simpleGroup, String fieldName) {
        Type mapType = simpleGroup.getType().getType(fieldName);
        if (mapType instanceof GroupType) {
            GroupType mapGroupType = mapType.asGroupType();
            return (mapGroupType.getRepetition().equals(OPTIONAL)
                    || mapGroupType.isRepetition(REQUIRED))
                    && mapGroupType.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.mapType())
                    && mapGroupType.getFieldCount() == 1;
        }
        return false;
    }

    private static boolean applyNestedKeyValueFieldValidations(SimpleGroup simpleGroup, String fieldName) {
        GroupType mapGroupType = simpleGroup.getType().getType(fieldName).asGroupType();
        if (mapGroupType.containsField("key_value")) {
            Type nestedKeyValueType = mapGroupType.getType("key_value");
            if (nestedKeyValueType instanceof GroupType) {
                GroupType nestedKeyValueGroupType = nestedKeyValueType.asGroupType();
                return nestedKeyValueGroupType.isRepetition(REPEATED)
                        && nestedKeyValueGroupType.containsField("key")
                        && nestedKeyValueGroupType.getType("key").isRepetition(REQUIRED);
            }
        }
        return false;
    }
}
