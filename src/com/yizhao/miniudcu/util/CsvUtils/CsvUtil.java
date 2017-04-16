package com.yizhao.miniudcu.util.CsvUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by yzhao on 4/15/17.
 */
public class CsvUtil {

    private static String COMMA = ",";
    public static String addToCsv( String csv, String value ){
        return addToCsv( csv, value, COMMA );
    }
    public static String removeFromCsv( String csv, String value ){
        return removeFromCsv( csv, value, COMMA );
    }
    public static boolean csvContains( String csv, String value ){
        return csvContains( csv, value, COMMA );
    }
    public static List<String> csvToCollection(String csv ){
        return csvToCollection( csv, COMMA );
    }
    public static String collectionToCsv( Collection<String> collection ){
        return collectionToCsv( collection, COMMA );
    }
    public static String sort( String csv ){
        return sort( csv, COMMA );
    }

    public static String addToCsv( String csv, String value, String delimiter ){
        Collection<String> collection = CsvUtil.csvToCollection( csv, delimiter );

        if( value != null && value.trim().length() > 0 ) {
            collection.add( value );
        }

        //performs deduping of values
        return CsvUtil.collectionToCsv( collection, delimiter );
    }

    public static String removeFromCsv( String csv, String value, String delimiter ){
        Collection<String> collection = CsvUtil.csvToCollection( csv, delimiter );

        if( value != null && value.trim().length() > 0 ) {
            collection.remove( value );
        }

        //performs deduping of values
        return CsvUtil.collectionToCsv( collection, delimiter );
    }

    public static boolean csvContains( String csv, String value, String delimiter ){
        if( csv != null && value != null){
            Collection<String> collection = csvToCollection( csv, delimiter );
            return collection.contains( value.trim() );
        }
        return false;
    }


    public static String sort( String csv, String delimiter ){
        List<String> sortedList = new ArrayList<String>( csvToCollection( csv, delimiter ) );
        Collections.sort( sortedList );

        return collectionToCsv( sortedList );
    }

    public static String quoteCsv(String csv) {
        return quoteCsv(csv, "'");
    }

    public static String quoteCsv(String csv, String quoteChar) {
        if(csv == null) {
            return null;
        }
        if(csv.length() == 0) {
            return csv;
        }
        quoteChar = quoteChar==null?"'":quoteChar;
        List<String> quotedC = new ArrayList<String>();
        List<String> c = csvToCollection(csv);
        for(String a: c) {
            quotedC.add(quoteChar + a.trim() + quoteChar);
        }
        return collectionToCsv(quotedC);
    }

    /**
     * Converts a CSV to a Collection of strings
     * - removes duplicate values
     * - removes empty strings
     * - maintains the original order
     *
     * Example:
     * csvToCollection( "A,B,,C,,C,B" ) -> ["A","B","C"]
     */
    public static List<String> csvToCollection( String csv, String delimiter ){
        //use arraylist to preserve ordering
        List<String> retval = new ArrayList<String>();

        if(csv != null){

            //dedup: seen contains previously encountered values
            Set<String> seen = new HashSet<String>();

            String[] values = csv.split(Pattern.quote( delimiter ));
            for(int i = 0; i < values.length; ++i){
                String value = values[i].trim();

                if( !seen.contains( value ) ){
                    seen.add( value );

                    if( value.length() > 0 ){
                        retval.add( value );
                    }
                }
            }
        }

        return retval;
    }

    //performs deduping of values
    public static String collectionToCsv( Collection<String> collection, String delimiter ){
        StringBuffer retval = new StringBuffer();

        if(collection != null){

            //dedup: seen contains previously encountered values
            Set<String> seen = new LinkedHashSet<String>();

            for(String value : collection){
                if (value == null) {
                    continue;
                }
                value = value.trim();
                if(value.length() > 0){
                    if( !seen.contains( value ) ){
                        seen.add( value );

                        if(retval.length() > 0){
                            retval.append( delimiter );
                        }
                        retval.append(value);
                    }
                }
            }
        }

        return retval.toString();
    }

    public static String collectionToCsvNoDedup( Collection<String> collection){
        return collectionToCsvNoDedup(collection, COMMA);
    }

    public static String collectionToCsvNoDedup( Collection<String> collection, String delimiter ){
        StringBuffer retval = new StringBuffer();

        if(collection != null){

            for(String value : collection){
                if (value == null) {
                    continue;
                }
                value = value.trim();
                if(value.length() > 0){
                    if(retval.length() > 0){
                        retval.append( delimiter );
                    }
                    retval.append(value);
                }
            }
        }

        return retval.toString();
    }

}
