package org.raystack.dagger.functions.udfs.scalar;

import org.raystack.dagger.common.udfs.ScalarUdf;

import java.util.Arrays;

/**
 * The List contains udf.
 */
public class ListContains extends ScalarUdf {
    /**
     * checks if a list contains a given item.
     *
     * @param inputList the input list
     * @param item      the item
     * @return true if list has the desired element or false
     * @author prakhar.m
     * @team DE
     */
    public boolean eval(String[] inputList, String item) {
        if (inputList == null) {
            return false;
        }
        return Arrays.asList(inputList).contains(item);
    }
}
