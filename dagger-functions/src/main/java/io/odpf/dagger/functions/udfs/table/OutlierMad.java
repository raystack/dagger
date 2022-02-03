package io.odpf.dagger.functions.udfs.table;

import io.odpf.dagger.common.udfs.TableUdf;
import io.odpf.dagger.functions.udfs.table.outlier.mad.Mad;
import io.odpf.dagger.functions.udfs.table.outlier.mad.Point;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.annotation.DataTypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The class responsible for Outlier mad udf.
 */
public class OutlierMad extends TableUdf<Tuple5<Timestamp, Double, Double, Double, Boolean>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutlierMad.class.getName());
    private static final long MILLI_SECONDS_IN_MINUTE = 60000;
    private static final double HUNDRED = 100D;

    /**
     * determines outliers for a given time series on the basis of threshold, observation window and tolerance provided.
     *
     * @param values                     list of values
     * @param localDateTimeArray         list of timestamps of given data value
     * @param windowStartTime            start time of the window
     * @param windowLengthInMinutes      length of the window in minutes
     * @param observationPeriodInMinutes time for which the outliers should be looked in minutes
     * @param tolerance                  permitted deviation
     * @param outlierPercentage          the minimum threshold percentage of outliers in the observation window to consider                                   whole window as outlier
     */
    public void eval(@DataTypeHint(value = "RAW", bridgedTo = ArrayList.class) ArrayList<Double> values, @DataTypeHint(value = "RAW", bridgedTo = ArrayList.class) ArrayList<LocalDateTime> localDateTimeArray, LocalDateTime windowStartTime, Integer windowLengthInMinutes,
                     Integer observationPeriodInMinutes, Integer tolerance, Integer outlierPercentage) {
        List<Timestamp> timestampsArray = localDateTimeArray.stream().map(Timestamp::valueOf).collect(Collectors.toList());
        ArrayList<Point> points = new ArrayList<>(Collections.nCopies(values.size(), Point.EMPTY_POINT));
        IntStream.range(0, values.size()).forEach(index -> points.set(index, new Point(timestampsArray.get(index), values.get(index), false)));
        ArrayList<Point> orderedPoints = getOrderedValues(Timestamp.valueOf(windowStartTime), points, windowLengthInMinutes, observationPeriodInMinutes);
        Mad mad = new Mad(orderedPoints, tolerance);
        try {
            List<Point> outliers = mad.getOutliers();
            List<Point> observablePoints = orderedPoints.stream().filter(Point::isObservable).collect(Collectors.toList());
            boolean hasOutliers = (outliers.size() * HUNDRED / observablePoints.size()) >= outlierPercentage;
            observablePoints.forEach(point -> collect(new Tuple5<>(point.getTimestamp(), point.getValue(), point.getUpperBound(),
                    point.getLowerBound(), hasOutliers)));
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
        }
    }

    private ArrayList<Point> getOrderedValues(Timestamp windowStartTime, ArrayList<Point> points, int windowLengthInMinutes, Integer observationPeriodInMinutes) {
        points.sort((p1, p2) -> (int) (p1.getTimestamp().getTime() - p2.getTimestamp().getTime()));
        ArrayList<Point> orderedValues = new ArrayList<>(Collections.nCopies(points.size(), Point.EMPTY_POINT));
        IntStream.range(0, points.size()).forEach(index -> {
            double value = points.get(index).getValue();
            Timestamp valueStartTime = points.get(index).getTimestamp();
            boolean isObservable = valueStartTime.getTime() >= ((windowStartTime.getTime() + (windowLengthInMinutes * MILLI_SECONDS_IN_MINUTE))
                    - (observationPeriodInMinutes * MILLI_SECONDS_IN_MINUTE));
            orderedValues.set(index, new Point(valueStartTime, value, isObservable));
        });
        return orderedValues;
    }
}
