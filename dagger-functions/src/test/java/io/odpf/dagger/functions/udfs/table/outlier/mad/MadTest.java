package io.odpf.dagger.functions.udfs.table.outlier.mad;

import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MadTest {

    @Test
    public void shouldReturnOutliersWhichCrossesThreshold() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 1D, true));
        points.add(new Point(new Timestamp(2L), 4D, true));
        points.add(new Point(new Timestamp(3L), 4D, true));
        points.add(new Point(new Timestamp(4L), 4D, true));
        points.add(new Point(new Timestamp(5L), 5D, true));
        points.add(new Point(new Timestamp(6L), 5D, true));
        points.add(new Point(new Timestamp(7L), 5D, true));
        points.add(new Point(new Timestamp(8L), 5D, true));
        points.add(new Point(new Timestamp(9L), 7D, true));
        points.add(new Point(new Timestamp(10L), 7D, true));
        points.add(new Point(new Timestamp(11L), 8D, true));
        points.add(new Point(new Timestamp(12L), 10D, true));
        points.add(new Point(new Timestamp(13L), 16D, true));
        points.add(new Point(new Timestamp(14L), 30D, true));
        Mad mad = new Mad(points, 3);

        List<Point> outliers = mad.getOutliers();
        assertEquals(3, outliers.size());
        assertEquals(1D, outliers.get(0).getValue(), 0d);
        assertEquals(16D, outliers.get(1).getValue(), 0d);
        assertEquals(30D, outliers.get(2).getValue(), 0d);
    }


    @Test
    public void shouldOutliersWhichCrossesThresholdContainingDecimalValues() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 1D, true));
        points.add(new Point(new Timestamp(2L), 2D, true));
        points.add(new Point(new Timestamp(3L), 3D, true));
        points.add(new Point(new Timestamp(4L), 3D, true));
        points.add(new Point(new Timestamp(5L), 4D, true));
        points.add(new Point(new Timestamp(6L), 4D, true));
        points.add(new Point(new Timestamp(7L), 4D, true));
        points.add(new Point(new Timestamp(8L), 5D, true));
        points.add(new Point(new Timestamp(9L), 5.5D, true));
        points.add(new Point(new Timestamp(10L), 6D, true));
        points.add(new Point(new Timestamp(11L), 6D, true));
        points.add(new Point(new Timestamp(12L), 6.50D, true));
        points.add(new Point(new Timestamp(13L), 7D, true));
        points.add(new Point(new Timestamp(14L), 7D, true));
        points.add(new Point(new Timestamp(15L), 7.5D, true));
        points.add(new Point(new Timestamp(16L), 8D, true));
        points.add(new Point(new Timestamp(17L), 9D, true));
        points.add(new Point(new Timestamp(18L), 12D, true));
        points.add(new Point(new Timestamp(19L), 52D, true));
        points.add(new Point(new Timestamp(20L), 90D, true));
        Mad mad = new Mad(points, 5);

        List<Point> outliers = mad.getOutliers();
        assertEquals(2, outliers.size());
        assertEquals(52D, outliers.get(0).getValue(), 0d);
        assertEquals(90D, outliers.get(1).getValue(), 0d);
    }

    @Test
    public void shouldReturnEmptyListIfMadZero() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 1D, true));
        points.add(new Point(new Timestamp(2L), 4D, true));
        points.add(new Point(new Timestamp(3L), 4D, true));
        points.add(new Point(new Timestamp(4L), 4D, true));
        points.add(new Point(new Timestamp(5L), 5D, true));
        points.add(new Point(new Timestamp(6L), 5D, true));
        points.add(new Point(new Timestamp(7L), 5D, true));
        points.add(new Point(new Timestamp(8L), 5D, true));
        points.add(new Point(new Timestamp(9L), 5D, true));
        points.add(new Point(new Timestamp(10L), 5D, true));
        points.add(new Point(new Timestamp(11L), 5D, true));
        points.add(new Point(new Timestamp(12L), 5D, true));
        points.add(new Point(new Timestamp(13L), 5D, true));
        points.add(new Point(new Timestamp(14L), 5D, true));
        Mad mad = new Mad(points, 3);
        List<Point> outliers = mad.getOutliers();
        assertEquals(0, outliers.size());
    }

    @Test
    public void shouldReturnEmptyListIfMedianCannotBeCalculatedZero() {
        ArrayList<Point> points = new ArrayList<>();
        Mad mad = new Mad(points, 3);
        List<Point> outliers = mad.getOutliers();
        assertEquals(0, outliers.size());
    }

    @Test
    public void shouldDetectOutliersOnlyFromObservablePointsIfAllOutliersBelongToObservable() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 3D, false));
        points.add(new Point(new Timestamp(2L), 4D, false));
        points.add(new Point(new Timestamp(3L), 4D, false));
        points.add(new Point(new Timestamp(4L), 4D, false));
        points.add(new Point(new Timestamp(5L), 5D, false));
        points.add(new Point(new Timestamp(6L), 5D, false));
        points.add(new Point(new Timestamp(7L), 5D, false));
        points.add(new Point(new Timestamp(8L), 5D, false));
        points.add(new Point(new Timestamp(9L), 7D, false));
        points.add(new Point(new Timestamp(10L), 7D, false));
        points.add(new Point(new Timestamp(11L), 8D, false));
        points.add(new Point(new Timestamp(12L), 10D, true));
        points.add(new Point(new Timestamp(13L), 16D, true));
        points.add(new Point(new Timestamp(14L), 30D, true));
        Mad mad = new Mad(points, 5);

        List<Point> outlierPoints = mad.getOutliers();

        assertEquals(2, outlierPoints.size());
        assertEquals(16D, outlierPoints.get(0).getValue(), 0d);
        assertEquals(30D, outlierPoints.get(1).getValue(), 0d);
    }

    @Test
    public void shouldDetectOutliersOnlyFromObservablePointsIfNotAllOutliersBelongToObservable() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 50D, false));
        points.add(new Point(new Timestamp(2L), 4D, false));
        points.add(new Point(new Timestamp(3L), 4D, false));
        points.add(new Point(new Timestamp(4L), 4D, false));
        points.add(new Point(new Timestamp(5L), 5D, false));
        points.add(new Point(new Timestamp(6L), 5D, false));
        points.add(new Point(new Timestamp(8L), 5D, false));
        points.add(new Point(new Timestamp(9L), 7D, false));
        points.add(new Point(new Timestamp(10L), 7D, false));
        points.add(new Point(new Timestamp(11L), 8D, false));
        points.add(new Point(new Timestamp(12L), 10D, true));
        points.add(new Point(new Timestamp(13L), 16D, true));
        points.add(new Point(new Timestamp(14L), 30D, true));
        points.add(new Point(new Timestamp(14L), 40D, true));
        Mad mad = new Mad(points, 3);

        List<Point> outlierPoints = mad.getOutliers();

        assertEquals(2, outlierPoints.size());
        assertEquals(30D, outlierPoints.get(0).getValue(), 0d);
        assertEquals(40D, outlierPoints.get(1).getValue(), 0d);
    }

    @Test
    public void shouldDetectOutliersOnlyFromObservablePointsIfNonObeservableAreLargeValues() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 50D, false));
        points.add(new Point(new Timestamp(2L), 40D, false));
        points.add(new Point(new Timestamp(3L), 41D, false));
        points.add(new Point(new Timestamp(4L), 42D, false));
        points.add(new Point(new Timestamp(5L), 50D, false));
        points.add(new Point(new Timestamp(6L), 51D, false));
        points.add(new Point(new Timestamp(8L), 52D, false));
        points.add(new Point(new Timestamp(9L), 70D, false));
        points.add(new Point(new Timestamp(10L), 70D, false));
        points.add(new Point(new Timestamp(11L), 80D, false));
        points.add(new Point(new Timestamp(12L), 50D, true));
        points.add(new Point(new Timestamp(13L), 6D, true));
        points.add(new Point(new Timestamp(14L), 3D, true));
        points.add(new Point(new Timestamp(14L), 4D, true));
        Mad mad = new Mad(points, 3);

        List<Point> outlierPoints = mad.getOutliers();

        assertEquals(3, outlierPoints.size());
        assertEquals(6D, outlierPoints.get(0).getValue(), 0d);
        assertEquals(3D, outlierPoints.get(1).getValue(), 0d);
        assertEquals(4D, outlierPoints.get(2).getValue(), 0d);
    }

    @Test
    public void shouldDetectOutliersOnlyFromObservablePointsForLargeDataPoints() {
        ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(new Timestamp(1L), 1666D, false));
        points.add(new Point(new Timestamp(2L), 1682D, false));
        points.add(new Point(new Timestamp(3L), 1674D, false));
        points.add(new Point(new Timestamp(4L), 1713D, false));
        points.add(new Point(new Timestamp(5L), 1756D, false));
        points.add(new Point(new Timestamp(6L), 1679D, false));
        points.add(new Point(new Timestamp(8L), 1700D, false));
        points.add(new Point(new Timestamp(9L), 1723D, false));
        points.add(new Point(new Timestamp(10L), 1735D, false));
        points.add(new Point(new Timestamp(11L), 1700D, false));
        points.add(new Point(new Timestamp(12L), 1558D, false));
        points.add(new Point(new Timestamp(13L), 1821D, false));
        points.add(new Point(new Timestamp(14L), 1707D, false));
        points.add(new Point(new Timestamp(15L), 1869D, false));
        points.add(new Point(new Timestamp(16L), 1777D, false));
        points.add(new Point(new Timestamp(17L), 1723D, false));
        points.add(new Point(new Timestamp(18L), 1729D, false));
        points.add(new Point(new Timestamp(19L), 1713D, false));
        points.add(new Point(new Timestamp(20L), 1719D, false));
        points.add(new Point(new Timestamp(21L), 1739D, true));
        points.add(new Point(new Timestamp(22L), 1826D, true));
        points.add(new Point(new Timestamp(23L), 2790D, true));
        points.add(new Point(new Timestamp(24L), 1933D, true));
        Mad mad = new Mad(points, 5);

        List<Point> outlierPoints = mad.getOutliers();

        assertEquals(1, outlierPoints.size());
        assertEquals(2790D, outlierPoints.get(0).getValue(), 0d);
    }

}
