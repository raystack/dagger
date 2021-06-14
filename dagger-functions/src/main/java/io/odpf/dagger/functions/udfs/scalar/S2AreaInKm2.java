package io.odpf.dagger.functions.udfs.scalar;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import io.odpf.dagger.common.udfs.ScalarUdf;

/**
 * The type S2AreaInKm2 udf.
 */
public class S2AreaInKm2 extends ScalarUdf {

    private static final long TOTAL_EARTH_AREA_KM2 = 510072000;
    private static final long FACTOR = 4;

    /**
     * compute area in km2 for a specific s2 cell.
     *
     * @param s2id s2id of the cell
     * @return area in km2
     * @author leyi.seah
     * @team DS Marketplace Pricing
     */
    public double eval(String s2id) {
        long id = Long.parseLong(s2id);
        S2Cell s2Cell = getS2CellfromId(id);
        return (s2Cell.exactArea() * TOTAL_EARTH_AREA_KM2) / (FACTOR * Math.PI);
    }

    private S2Cell getS2CellfromId(long id) {
        return new S2Cell(new S2CellId(id));
    }
}
