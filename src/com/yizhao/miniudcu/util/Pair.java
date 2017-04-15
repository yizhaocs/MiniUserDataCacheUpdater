package com.yizhao.miniudcu.util;

import java.io.Serializable;

/**
 * Created by yzhao on 4/15/17.
 */
public class Pair<A, B> implements Serializable {

    private A first;
    private B second;

    public Pair(A first, B second){
        this.first = first;
        this.second = second;
    }

    public A getFirst(){
        return first;
    }

    public B getSecond(){
        return second;
    }

    public String toString(){
        return "(" + first + "," + second + ")";
    }

    public boolean equals(Object obj) {
        if( obj instanceof Pair ){
            Pair other = (Pair) obj;
            return EqualsUtil.equals( this.first, other.first )
                    && EqualsUtil.equals( this.second, other.second );
        }

        return false;
    }

    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + ( first == null ? 0 : first.hashCode() );
        hash = 31 * hash + ( second == null ? 0 : second.hashCode() );
        return hash;
    }

}

