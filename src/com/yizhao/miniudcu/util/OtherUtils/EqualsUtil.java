package com.yizhao.miniudcu.util.OtherUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

/**
 * Created by yzhao on 4/15/17.
 */
public class EqualsUtil {

    /**
     * Compares two objects for equality.
     * (If both are null, that is considered equal)
     *
     * WARNING: does not work for Arrays.  Use Arrays.equals() to compare arrays.
     */
    static public boolean equals(Object a, Object b){
        return a == null ? b == null : a.equals( b );
    }

    /**
     * Compares two longs
     * Also handles int/short/byte, by implicit casting
     */
    static public boolean equals(long a, long b){
        return a == b;
    }

    /**
     * Compares two doubles
     * Also handles floats, by implicit casting
     */
    static public boolean equals(double a, double b){
        return a == b;
    }

    /**
     * Compares two Double objects.
     *
     * First performs NULL checks.
     *
     * If both are NOT NULL, then convert to double and compare to the specified precision ( NOT decimal places )
     * For rounding, uses the "HALF_UP" rounding mode
     *
     * @param a
     * @param b
     * @param precision precision of the comparison ( NOT decimal places )
     */
    static public boolean equals( Double a, Double b, int precision ){
        return a == null ? b == null : equals( a.doubleValue(), b.doubleValue(), precision );
    }

    /**
     * Compares two doubles, to the specified precision ( NOT decimal places )
     * For rounding, uses the "HALF_UP" rounding mode
     *
     * Also handles floats, by implicit casting
     * @param a
     * @param b
     * @param precision precision of the comparison ( NOT decimal places )
     */
    static public boolean equals( double a, double b, int precision ){
        MathContext mc = new MathContext( precision, RoundingMode.HALF_UP );

        BigDecimal bdA = new BigDecimal( a );
        BigDecimal bdB = new BigDecimal( b );

        return bdA.round( mc ).doubleValue() == bdB.round( mc ).doubleValue();
    }

    /**
     * Compares two booleans
     */
    static public boolean equals(boolean a, boolean b){
        return a == b;
    }

    /**
     * Compares two chars
     */
    static public boolean equals(char a, char b){
        return a == b;
    }

    /**
     * Compare 2 dates, subject to resolution.
     * E.g. if resolution = 1000, then the dates are truncated to SECONDS
     * before the comparison
     *
     * @param a
     * @param b
     * @param resolutionMs
     * @return
     */
    public static boolean equals(Date a, Date b, long resolutionMs ){
        return a == b ||
                ( a != null && b != null &&
                        ( TimeUtil.truncate( a.getTime(), resolutionMs ) == TimeUtil.truncate( b.getTime(), resolutionMs ) )  );
    }

    /**
     * Two collections are equal if they contain the exact same objects.
     * Order does NOT matter.
     * @param a
     * @param b
     * @return true if collections are equal
     */
    static public <T> boolean equalsUnordered(Collection<T> a, Collection<T> b ){
        return new HashSet<T>( a ).equals( new HashSet<T>( b ) );
    }

    /**
     * Two collections are equal if they contain the exact same objects,
     * in the exact same order.
     * @param a
     * @param b
     * @return true if collections are equal
     */
    static public <T> boolean equalsOrdered( Collection<T> a, Collection<T> b ){
        return new ArrayList<T>( a ).equals( new ArrayList<T>( b ) );
    }
}
