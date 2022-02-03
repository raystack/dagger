package io.odpf.dagger.functions.udfs.scalar;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import io.odpf.dagger.common.udfs.ScalarUdf;

/**
 * The type S2id udf.
 */
public class S2Id extends ScalarUdf {
    /**
     * computes s2id for a given lat, long and level.
     *
     * @param latitude  the latitude
     * @param longitude the longitude
     * @param level     the level of s2 cells
     * @return s2id string
     * @author gaurav.s
     * @team DE
     */
    public String eval(Double latitude, Double longitude, Integer level) {
        S2LatLng s2LatLng = S2LatLng.fromDegrees(latitude, longitude);
        return Long.toString(S2CellId.fromLatLng(s2LatLng).parent(level).id());
    }
}
