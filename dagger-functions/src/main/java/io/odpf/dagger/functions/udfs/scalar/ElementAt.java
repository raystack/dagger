package io.odpf.dagger.functions.udfs.scalar;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.elementAt.MessageReader;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * The ElementAt udf.
 */
public class ElementAt extends ScalarUdf {
    private LinkedHashMap<String, String> protos;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private StencilClient stencilClient;


    /**
     * Instantiates a new Element at.
     *
     * @param protos                    the protos
     * @param stencilClientOrchestrator the stencil client orchestrator
     */
    public ElementAt(LinkedHashMap<String, String> protos, StencilClientOrchestrator stencilClientOrchestrator) {
        this.protos = protos;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        if (stencilClient == null) {
            stencilClient = getStencilClient();
        }
    }

    /**
     * finds out the element at a given index and a given path in an array of complex Datatypes.
     *
     * @param array       the array
     * @param pathOfArray the path of array
     * @param index       the index
     * @param path        the path on element in the array
     * @return the value
     * @author gaurav.s
     * @team DE
     */
    public String eval(Row[] array, String pathOfArray, int index, String path) {
        try {
            if (array == null || array.length <= index) {
                return "";
            }
            String protoClassName = protos.entrySet().iterator().next().getValue();
            MessageReader messageReader = new MessageReader(array[index], protoClassName, pathOfArray, stencilClient);
            Object value = messageReader.read(path);
            return String.valueOf(value);

        } catch (Exception ignored) {

        }
        return "";
    }

    /**
     * for the given table, finds out the element at a given index and a given path in an array of complex Datatypes.
     *
     * @param array       the array
     * @param pathOfArray the path of array
     * @param index       the index
     * @param path        the path on element in the array
     * @param tableName   name of the table to apply udf on (in case of multiple streams / JOINS)
     * @return the value
     * @author mayur.gubrele
     * @team DE
     */
    public String eval(Row[] array, String pathOfArray, int index, String path, String tableName) {
        try {
            if (array == null || array.length <= index) {
                return "";
            }
            String protoClassName = protos.get(tableName);
            MessageReader messageReader = new MessageReader(array[index], protoClassName, pathOfArray, stencilClient);
            Object value = messageReader.read(path);
            return String.valueOf(value);

        } catch (Exception ignored) {

        }
        return "";
    }

    /**
     * finds the element at a given position for an object ArrayList.
     *
     * @param arrayList the array list
     * @param index     the index
     * @return the value
     * @author mayur.gubrele
     * @team DE
     */
    public Object eval(ArrayList<Object> arrayList, int index) {
        return eval(arrayList.toArray(), index);
    }

    /**
     * finds the element at a given position for an object Array. supports -ve index
     *
     * @param array the array
     * @param index the index
     * @return the value
     * @author arujit
     * @team DE
     */
    public Object eval(Object[] array, int index) {
        try {
            if (array == null || array.length <= index) {
                return null;
            }
            int arrayLength = array.length;
            if (index < 0) {
                int absoluteIndex = arrayLength + index;
                if (absoluteIndex < 0) {
                    return null;
                }
                return array[arrayLength + index];
            }
            return array[index];
        } catch (Exception ignored) {

        }
        return null;
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
}
