package com.yizhao.miniudcu.util.OtherUtils;

import com.yizhao.miniudcu.util.GenericObjectUtils.Pair;

import java.util.Random;

/**
 * Created by yzhao on 4/15/17.
 */
public class RandomUtil {

    private static Random random = new Random();

    public static int randomIntegerInRange( int min, int max ){
        int diff = max - min;
        return min + Math.round( random.nextFloat() * diff );
    }

    public static int randomIntegerInRange( Pair<Integer,Integer> range ){
        int min = range.getFirst();
        int max = range.getSecond();
        return randomIntegerInRange( min,max );
    }

    public static long randomLongInRange( long min, long max ){
        long diff = max - min;
        return min + Math.round( random.nextDouble() * diff );
    }

    public static long randomLongInRange( Pair<Long,Long> range ){
        long min = range.getFirst();
        long max = range.getSecond();
        return randomLongInRange( min, max );
    }

    /**
     * gets a random boolean value, where probability of the value being 'true' = pTrue
     * @param pTrue
     * @return
     */
    public static boolean randomBooleanByProbability(double pTrue) {
        return random.nextDouble() >= (1.0 - pTrue);
    }

    /**
     * Generate a decimal string representation of a random number within the
     * supplied bounds.
     * @param lowerBound
     *            the lower bound, inclusive
     * @param upperBound
     *            the upper bound, inclusive
     * @param decimalPlaces
     *            the decimal places of the result
     *
     * @return the formatted string
     */
    public static String randomDoubleStringWithDecimalPrecision(final int lowerBound,
                                                                final int upperBound,
                                                                final int decimalPlaces){

        if(lowerBound < 0 || upperBound <= lowerBound || decimalPlaces < 0){
            throw new IllegalArgumentException("Invalid input!");
        }

        final double dbl = (random.nextDouble() * (upperBound - lowerBound))  + lowerBound;

        return String.format("%." + decimalPlaces + "f", dbl);

    }
}
