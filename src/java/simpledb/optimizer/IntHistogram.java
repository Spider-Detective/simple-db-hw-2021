package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int[] hist;
    private final int min;
    private final int max;
    private final int width;
    private int count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.hist = new int[Math.min(buckets, (max - min))];
        this.min = min;
        this.max = max;
        this.width = (max - min) / hist.length;
        this.count = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        hist[getPos(v)]++;
        count++;
    }

    private int getPos(int v) {
        int pos = (v - min) / width;
        if (pos == hist.length) {
            pos--;
        }
        return pos;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double selectivity = -1.0;
        switch (op) {
            case EQUALS:
                selectivity = getSelectivityForEquals(v);
                break;
            case NOT_EQUALS:
                selectivity = 1.0 - getSelectivityForEquals(v);
                break;
            case GREATER_THAN:
                selectivity = getSelectivityForGreaterThan(v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = getSelectivityForGreaterThanOrEqual(v);
                break;
            case LESS_THAN:
                selectivity = getSelectivityForLessThan(v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = getSelectivityForLessThanOrEqual(v);
                break;
            case LIKE:
                selectivity = 0.5;
                break;
        }
        return selectivity;
    }

    private double getSelectivityForEquals(int v) {
        if (v < min || v > max) {
            return 0.0;
        }
        int h = hist[getPos(v)];
        return 1.0 * h / width / count;
    }

    private double getSelectivityForGreaterThan(int v) {
        if (v < min) {
            return 1.0;
        } else if (v >= max) {
            return 0.0;
        } else {
            int pos = getPos(v);
            int right = min + width * (pos + 1);
            double rightFrac = (right - v) * 1.0 / width * hist[pos] / count;
            for (int i = pos + 1; i < hist.length; i++) {
                rightFrac += hist[i] * 1.0 / count;
            }
            return rightFrac;
        }
    }

    private double getSelectivityForGreaterThanOrEqual(int v) {
        if (v <= min) {
            return 1.0;
        } else if (v > max) {
            return 0.0;
        } else {
            int pos = getPos(v);
            int right = min + width * (pos + 1);
            double rightFrac = (right - v + 1) * 1.0 / width * hist[pos] / count;
            for (int i = pos + 1; i < hist.length; i++) {
                rightFrac += hist[i] * 1.0 / count;
            }
            return rightFrac;
        }
    }

    private double getSelectivityForLessThan(int v) {
        if (v <= min) {
            return 0.0;
        } else if (v > max) {
            return 1.0;
        } else {
            int pos = getPos(v);
            int left = min + width * pos;
            double leftFrac = (v - left) * 1.0 / width * hist[pos] / count;
            for (int i = 0; i < pos; i++) {
                leftFrac += hist[i] * 1.0 / count;
            }
            return leftFrac;
        }
    }

    private double getSelectivityForLessThanOrEqual(int v) {
        if (v < min) {
            return 0.0;
        } else if (v >= max) {
            return 1.0;
        } else {
            int pos = getPos(v);
            int left = min + width * pos;
            double leftFrac = (v - left + 1) * 1.0 / width * hist[pos] / count;
            for (int i = 0; i < pos; i++) {
                leftFrac += hist[i] * 1.0 / count;
            }
            return leftFrac;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return this.getClass().getSimpleName() + "-{" + Arrays.toString(hist) +
                ",min=" + min + ", max=" + max + ", count=" + count + ", width=" + width + "}";
    }
}
