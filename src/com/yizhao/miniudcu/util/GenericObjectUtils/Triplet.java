package com.yizhao.miniudcu.util.GenericObjectUtils;

import com.yizhao.miniudcu.util.OtherUtils.EqualsUtil;

import java.io.Serializable;

/**
 * Generic class to represent a a triplet of objects
 * The objects need not have the same type.
 *
 * @param <A>
 * @param <B>
 * @param <C>
 */
public final class Triplet<A, B, C> implements Serializable {
    private static final long serialVersionUID = 1L;

    private A first;
    private B second;
    private C third;

    public void setFirst(A first) {
        this.first = first;
    }

    public void setSecond(B second) {
        this.second = second;
    }

    public void setThird(C third) {
        this.third = third;
    }

    public Triplet(A first, B second, C third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    /**
     * @return the third
     */
    public final C getThird() {
        return third;
    }

    public String toString() {
        return "(" + first + "," + second + "," + third + ")";
    }

    public boolean equals(Object obj) {
        if( obj instanceof Triplet ){
            Triplet other = (Triplet) obj;
            return EqualsUtil.equals( this.first, other.first )
                    && EqualsUtil.equals( this.second, other.second )
                    && EqualsUtil.equals( this.third, other.third );
        }

        return false;
    }

    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + ( first == null ? 0 : first.hashCode() );
        hash = 31 * hash + ( second == null ? 0 : second.hashCode() );
        hash = 31 * hash + ( third == null ? 0 : third.hashCode() );
        return hash;
    }


}
