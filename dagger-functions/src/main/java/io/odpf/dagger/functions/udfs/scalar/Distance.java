package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

/**
 * The Distance udf.
 */
public class Distance extends ScalarUdf {

    private static final int RADIUS_OF_EARTH = 6371;
    private static final int DEGREE_TO_RADIAN_DIVISOR = 180;

    /**
     * calculates distance between two points with latitude and longitude.
     *
     * @param latitude1  the latitude 1
     * @param longitude1 the longitude 1
     * @param latitude2  the latitude 2
     * @param longitude2 the longitude 2
     * @return the distance in KM
     * @author ashwin
     * @team DE
     */
    public Double eval(Double latitude1, Double longitude1, Double latitude2, Double longitude2) {
        double latDistance = degreeToRadian(latitude2 - latitude1);
        double lonDistance = degreeToRadian(longitude2 - longitude1);

        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(degreeToRadian(latitude1)) * Math.cos(degreeToRadian(latitude2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return RADIUS_OF_EARTH * c;
    }

    private static Double degreeToRadian(Double value) {
        return value * Math.PI / DEGREE_TO_RADIAN_DIVISOR;
    }
}
