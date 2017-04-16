package com.yizhao.miniudcu.util.CsvUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * CSV utility functions. Kept separate from {@link CsvUtil} functions for
 * minimal impact, although we may (and should) consider merging functionality.
 *
 * @author Julius
 *
 */
public class CsvUtil2 {

    // standard delimiter
    private static final String COMMA = ",";

    /**
     * Converts a collection of objects into a String of values separated by the
     * specified delimiter, using the object's toString() method. Returns an
     * empty string if the specified collection is null or empty.
     *
     * @param coll
     * @param delim
     * @return
     */
    public static <T extends Object> String collectionToCsv(Collection<T> coll, String delim) {
        StringBuilder sb = new StringBuilder();
        if (null != coll) {
            Set<String> seen = new HashSet<String>();
            for (T item : coll) {
                String sval = item.toString();
                if (null != sval) {
                    sval = sval.trim();
                }
                // null check is probably ultra-conservative
                if (null != sval && sval.length() > 0) {
                    if (!seen.contains(sval)) {
                        if (sb.length() > 0) {
                            sb.append(delim);
                        }
                        sb.append(sval);
                        seen.add(sval);
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * Converts a collection of objects into a String of values separated by the
     * default comma delimiter, using each object's toString() method. Returns
     * an empty string if the specified collection is null or empty.
     *
     * @param coll
     * @return
     */
    public static <T extends Object> String collectionToCsv(Collection<T> coll) {
        return collectionToCsv(coll, COMMA);
    }

    /**
     * Converts the specified delimiter-separated String into a collection of
     * objects, using the specified converter routine. Returns an empty list if
     * either the specified csv is null or empty, or if the csv cannot be split
     * meaningfully by the specified delimiter.
     *
     * @param csv
     * @param converter
     * @param delim
     * @return
     */
    public static <T extends Object> Collection<T> csvToCollection(String csv,
                                                                   Converter<T> converter, String delim) {
        Collection<T> retval = new ArrayList<T>();

        if (null != csv && csv.length() > 0) {
            String[] values = csv.split(Pattern.quote(delim));
            if (null != values && values.length > 0) {
                Set<String> seen = new HashSet<String>();
                for (String value : values) {
                    if (!seen.contains(value)) {
                        T obj = converter.convert(value);
                        if (null != obj) {
                            retval.add(obj);
                            seen.add(value);
                        }
                    }
                }
            }
        }

        return retval;
    }

    /**
     * Same as above, assumes comma delimiter.
     *
     * @param csv
     * @param converter
     * @return
     */
    public static <T extends Object> Collection<T> csvToCollection(String csv,
                                                                   Converter<T> converter) {
        return csvToCollection(csv, converter, COMMA);
    }

    /**
     * Interface for classes that convert from a String object to type T.
     *
     * @author Julius
     *
     * @param <T>
     */
    public static interface Converter<T extends Object> {

        /**
         * Converts the specified string into an object of type T, or returns
         * null if no such conversion can be performed.
         *
         * @param s
         * @return
         */
        public T convert(String s);
    }

    /**
     * Convenience method for splitting a string into a collection of strings, a
     * common operation.
     *
     * @param csv
     * @param delim
     * @return
     */
    public static Collection<String> csvToCollection(String csv, String delim) {
        return csvToCollection(csv, STRING_CONVERTER, delim);
    }

    /**
     * Convenience method for splitting a string into a collection of strings
     * using the default comma delimiter.
     *
     * @param csv
     * @return
     */
    public static Collection<String> csvToCollection(String csv) {
        return csvToCollection(csv, STRING_CONVERTER, COMMA);
    }

    public static final StringConverter STRING_CONVERTER = new StringConverter();

    /**
     * Implementation of the {@link Converter} interface for strings. Basically
     * a pass-through.
     *
     * @author Julius
     *
     */
    public static class StringConverter implements Converter<String> {
        public String convert(String s) {
            return s;
        }
    }


    public static interface Stringifier<T> {
        public String getString(T obj);
    }

    /**
     * Returns a CSV string for the specified collection of objects, using the specified Stringifier class to convert said
     * object into some String representation. Returns null if either the collection or Stringifier is null, or the
     * collection is empty.
     *
     * @param col
     * @param s
     * @return
     */
    public static <T extends Object> String toCsv(Collection<T> col, Stringifier<T> s) {
        if (null != col && col.size() > 0 && null != s) {
            StringBuilder sb = new StringBuilder();
            for (T obj : col) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(s.getString(obj));
            }
            return sb.toString();
        }
        return null;
    }


}
