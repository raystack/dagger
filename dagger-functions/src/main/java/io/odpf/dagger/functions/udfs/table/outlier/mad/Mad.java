package io.odpf.dagger.functions.udfs.table.outlier.mad;

import io.odpf.dagger.functions.exceptions.MadZeroException;
import io.odpf.dagger.functions.exceptions.MedianNotFound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.nCopies;
import static java.util.Collections.sort;

/**
 * The Mad for OutlierMad udf.
 */
public class Mad {
    private static final Logger LOGGER = LoggerFactory.getLogger(Mad.class.getName());
    private List<Point> points;
    private final Integer tolerance;

    /**
     * Instantiates a new Mad.
     *
     * @param points    the points
     * @param tolerance the tolerance
     */
    public Mad(List<Point> points, Integer tolerance) {
        this.points = points;
        this.tolerance = tolerance;
    }

    /**
     * Gets outliers.
     *
     * @return the outliers
     */
    public List<Point> getOutliers() {
        ArrayList<Double> doubleMAD;
        try {
            doubleMAD = getDoubleMAD();
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            return Collections.emptyList();
        }

        setDistanceFromMAD(doubleMAD);
        for (Point point : points) {
            if (point.isObservable() && point.getDistanceFromMad() > this.tolerance) {
                point.setOutlier(true);
            }
        }
        return points.stream().filter(Point::isOutlier).collect(Collectors.toList());
    }

    private void setDistanceFromMAD(ArrayList<Double> doubleMAD) {
        Double median = getMedian(points.stream().map(Point::getValue).collect(Collectors.toList()));
        for (Point point : points) {
            Double value = point.getValue();
            Double mad = (value <= median) ? doubleMAD.get(0) : doubleMAD.get(1);
            point.setDistanceFromMad(Math.abs(value - median) / mad);
            point.setUpperBound(median + (this.tolerance * mad));
            point.setLowerBound(median - (this.tolerance * mad));
        }
    }

    private ArrayList<Double> getDoubleMAD() {
        ArrayList<Point> valuesLessThanMedian = new ArrayList<>();
        ArrayList<Point> valuesGreaterThanMedian = new ArrayList<>();
        Double median = getMedian(points.stream().map(Point::getValue).collect(Collectors.toList()));
        points.forEach(point -> {
            if (point.getValue() <= median) {
                valuesLessThanMedian.add(point);
            }
            if (point.getValue() >= median) {
                valuesGreaterThanMedian.add(point);
            }
        });

        ArrayList<Double> doubleMad = new ArrayList<>();
        doubleMad.add(getMAD(valuesLessThanMedian));
        doubleMad.add(getMAD(valuesGreaterThanMedian));

        return doubleMad;
    }

    private static Double getMAD(List<Point> points) {
        Double median = getMedian(points.stream().map(Point::getValue).collect(Collectors.toList()));
        List<Double> absoluteDistancesFromMedian =
                getAbsoluteDistance(points
                        .stream()
                        .map(Point::getValue)
                        .collect(Collectors.toList()), median);
        Double absoluteDistancesMedian = getMedian(absoluteDistancesFromMedian);
        if (absoluteDistancesMedian == 0) {
            throw new MadZeroException("MAD is ZERO, outlier cannot be detected");
        }
        return absoluteDistancesMedian;

    }

    private static List<Double> getAbsoluteDistance(List<Double> values, Double value) {
        ArrayList<Double> absoluteDistances = new ArrayList<>(nCopies(values.size(), 0d));
        for (int index = 0; index < values.size(); index++) {
            absoluteDistances.set(index, Math.abs(values.get(index) - value));
        }
        return absoluteDistances;
    }

    private static Double getMedian(List<Double> values) {
        sort(values);
        int pointValueSize = values.size();
        if (pointValueSize == 0) {
            throw new MedianNotFound("To calculate median we need at least 1 element");
        }
        if (pointValueSize % 2 == 0) {
            return ((values.get(pointValueSize / 2 - 1) + values.get(pointValueSize / 2)) / 2);
        }
        return values.get(pointValueSize / 2);
    }

}
