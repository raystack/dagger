package io.odpf.dagger.functions.udfs.scalar.longbow.array.processors;

import io.odpf.dagger.functions.udfs.scalar.longbow.array.LongbowArrayType;
import io.odpf.dagger.functions.udfs.scalar.longbow.array.expression.Expression;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.JexlBuilder;

import java.util.Arrays;
import java.util.stream.BaseStream;

/**
 * The Abstract class of Array processor.
 */
public abstract class ArrayProcessor {
    private JexlContext jexlContext;
    private JexlScript jexlScript;
    private JexlEngine jexlEngine;
    private Expression expression;

    /**
     * Instantiates a new Array processor.
     *
     * @param expression the expression
     */
    public ArrayProcessor(Expression expression) {
        this.jexlContext = new MapContext();
        this.jexlEngine = new JexlBuilder().create();
        this.expression = expression;
    }

    /**
     * Instantiates a new Array processor with specified jexl values.
     *
     * @param jexlEngine  the jexl engine
     * @param jexlContext the jexl context
     * @param jexlScript  the jexl script
     * @param expression  the expression
     */
    public ArrayProcessor(JexlEngine jexlEngine, JexlContext jexlContext, JexlScript jexlScript, Expression expression) {
        this.jexlEngine = jexlEngine;
        this.jexlContext = jexlContext;
        this.jexlScript = jexlScript;
        this.expression = expression;
    }

    /**
     * Gets jexl context.
     *
     * @return the jexl context
     */
    protected JexlContext getJexlContext() {
        return jexlContext;
    }

    /**
     * Gets jexl script.
     *
     * @return the jexl script
     */
    protected JexlScript getJexlScript() {
        return jexlScript;
    }

    /**
     * Init jexl.
     *
     * @param dataType   the data type
     * @param inputArray the input array
     */
    public void initJexl(LongbowArrayType dataType, Object[] inputArray) {
        this.jexlScript = jexlEngine.createScript(expression.getExpressionString());
        BaseStream baseStream = dataType.getInputCastingFunction().apply(Arrays.stream(inputArray));
        jexlContext.set(Expression.BASE_STRING, baseStream);
    }

    /**
     * Process object.
     *
     * @return the object
     */
    public abstract Object process();
}
