package com.yizhao.miniudcu.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by yzhao on 4/15/17.
 */
public class CollectionUtil {
    public static boolean empty( Object[] arr ){
        return arr == null || arr.length == 0;
    }

    public static <T> boolean empty( Collection<T> c ){
        return c == null || c.size() == 0;
    }

    /**
     * Collect objects into a single collection.
     * @param <T>
     * @param objs
     * @return
     */
    public static <T> List<T> collect(T... objs ){
        if( objs == null ){
            return null;
        }

        List<T> retval = new ArrayList<T>();
        for( T t : objs ){
            retval.add( t );
        }

        return retval;
    }

    private static Random random = new Random();

    /**
     * Pick random element from a list
     * @param <T>
     * @param elements
     * @return
     */
    public static <T> T randomElement( List<T> elements ){
        return elements.get( random.nextInt( elements.size() ) );
    }

    /**
     * Pick random element from an array
     * @param <T>
     * @param elements
     * @return
     */
    public static <T> T randomElement( T[] elements ){
        return randomElement( Arrays.asList( elements ) );
    }

    /**
     * Returns between minCount and maxCount random elements from a list
     *
     * Warning: will shuffle the original list in-place
     *
     * @param <T>
     * @param elements
     * @param minCount
     * @param maxCount
     * @return
     */
    public static <T> List<T> randomElements( List<T> elements, int minCount, int maxCount ){
        int count = Math.min( elements.size(), RandomUtil.randomIntegerInRange( minCount, maxCount ) );
        Collections.shuffle( elements );
        return elements.subList( 0, count );
    }

    /**
     * Combine multiple collections into a single one
     * @param <T>
     * @param collections
     * @return
     */
    public static <T> Collection<T> combine( Collection<T>... collections ){
        if( collections == null ){
            return null;
        }

        Collection<T> retval = new ArrayList<T>();
        for( Collection<T> collection : collections ){
            if( collection != null ){
                retval.addAll( collection );
            }
        }

        return retval;
    }

    /**
     * Compute the Set intersection of multiple collections
     * @param <T>
     * @param collections
     * @return
     */
    public static <T> Set<T> intersect(Collection<T>... collections ){
        Set<T> retval = null;

        for( Collection<T> collection : collections ){
            if( retval == null ){
                //initialize the retval with the 1st collection ( copy! )
                retval = new HashSet<T>( collection );
            }else{
                //intersect each subsequent collection
                retval.retainAll( collection );
            }
        }

        return retval;
    }

    /**
     * Do 2 collections intersect?  ( share at least one common element )
     * non-modifying: treats inputs as READ-ONLY
     * @param <T> a collection
     * @param <T> b collection
     * @return true if there's at least one common element
     */
    public static  <T> boolean containsAny( Collection<T> a, Collection<T> b ){

        //no overlap possible if either set is empty or null
        if( a == null || b == null || a.size() == 0 || b.size() == 0 ){
            return false;
        }

        for( T elt : a ){
            if( b.contains( elt ) ){
                return true;
            }
        }

        return false;
    }


    /**
     * Convert collection of objects to collection of strings
     * PREREQ: each object is NOT NULL
     *
     * @param objects
     * @return
     */
    public static Collection<String> convertToStrings( Collection<? extends Object> objects ){

        if( objects == null ){
            return null;
        } else {
            Collection<String> retval = new ArrayList<String>();
            for( Object input : objects ){
                retval.add( input.toString() );
            }

            return retval;
        }

    }

    /**
     * Convert a collection of strings into a collection of Integers
     * PREREQ: each object is NOT NULL
     *
     * @param strs
     * @return
     */
    public static Collection<Integer> convertToIntegers( Collection<String> strs ){
        if( strs == null ){
            return null;
        } else {
            Collection<Integer> retval = new ArrayList<Integer>();
            for( String str : strs ){
                retval.add( new Integer( str ) );
            }

            return retval;
        }
    }

    /**
     * Convert a collection of strings into a collection of Long
     * PREREQ: each object is NOT NULL
     *
     * @param strs
     * @return
     */
    public static Collection<Long> convertToLongs( Collection<String> strs ){
        if( strs == null ){
            return null;
        } else {
            Collection<Long> retval = new ArrayList<Long>();
            for( String str : strs ){
                retval.add( new Long( str ) );
            }

            return retval;
        }
    }

    /**
     * Sort a map based on the value of each entry
     * Sort order: ascending
     *
     * @param unsortedMap
     * @param <A>
     * @param <B>
     * @return
     */
    public static <A,B extends Comparable<? super B>> Map<A,B> sortMapByValues(Map<A,B> unsortedMap) {
        return sortMapByValues(unsortedMap, false);
    }


    /**
     * Sort a map based on the value of each entry and the sort order specified
     *
     * @param unsortedMap
     * @param reverseSort
     * @param <A>
     * @param <B>
     * @return
     */
    public static <A,B extends Comparable<? super B>> Map<A,B> sortMapByValues( Map<A,B> unsortedMap, final boolean reverseSort){
        List<Map.Entry<A, B>> mapEntries = new LinkedList<Map.Entry<A, B>>(unsortedMap .entrySet());

        Collections.sort(mapEntries, new Comparator<Map.Entry<A, B>>() {
            public int compare(Map.Entry<A, B> e1, Map.Entry<A, B> e2) {
                if (reverseSort) {
                    return (e2.getValue()).compareTo(e1.getValue());
                }
                return (e1.getValue()).compareTo(e2.getValue());
            }
        });

        // preserve the order of insertion
        Map<A, B> sortedMap = new LinkedHashMap<A, B>();
        for (Map.Entry<A, B> entry : mapEntries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    /**
     * Sort a map's keys based on the corresponding values
     * - ascending
     *
     * E.g. if Map<Object,Date>, then call this to sort the Objects by Date
     *
     * @param <A>
     * @param <B>
     * @param map
     * @return
     */
    public static <A,B> List<A> sortKeysByValues( Map<A,B> map ){
        List<A> aList = new ArrayList<A>();
        List<B> bList = new ArrayList<B>();

        for( A a : map.keySet() ){
            aList.add( a );
            bList.add( map.get(a) );
        }

        return sort( aList, bList );
    }

    /**
     * sort list A based on values in list B
     * - ascending
     *
     * @param <A>
     * @param <B>
     * @param aList
     * @param bList
     */
    public static <A,B> List<A> sort( List<A> aList, List<B> bList ){
        if( aList.size() != bList.size() ){
            throw new IllegalArgumentException( "list sizes must be equal" );
        }

        //map of B values to corresponding A's
        SortedMap<B,List<A>> bSortedMap = new TreeMap<B,List<A>>();
        for( int i = 0; i < aList.size(); ++i ){
            A a = aList.get(i);
            B b = bList.get(i);

            List<A> list = bSortedMap.get( b );
            if( list == null ){
                list = new ArrayList<A>();
                bSortedMap.put( b, list );
            }

            list.add( a );
        }

        //
        // final collection ( in ascending order of B )
        //
        List<A> retval = new ArrayList<A>();
        for( List<A> as : bSortedMap.values() ){
            retval.addAll( as );
        }

        return retval;
    }

    /**
     * Split a list into sublists of the specified size
     * ( The last list may be smaller )
     *
     * @param <T>
     * @param list the original full list
     * @param entriesPerList size of each sublist
     * @return list of sublists
     */
    public static <T> List<List<T>> split( List<T> list, int entriesPerList ){
        if( entriesPerList < 1 ){
            throw new IllegalArgumentException("entriesPerList must be > 0" );
        }

        List<List<T>> retval = new ArrayList<List<T>>();
        //optimization if list size already <= entriesPerList
        if( list.size() <= entriesPerList ){
            retval.add( list );
            return retval;
        }

        List<T> subList = new ArrayList<T>();
        for( T entry : list ){
            subList.add( entry );

            if( subList.size() == entriesPerList ){
                //add the subList to retval
                retval.add( subList );

                //allocate new subList
                subList = new ArrayList<T>();
            }

        }

        if( subList.size() > 0 ){
            retval.add( subList );
            subList = new ArrayList<T>();
        }

        return retval;
    }

}