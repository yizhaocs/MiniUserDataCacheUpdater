package com.yizhao.miniudcu.util.StringUtils;


import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * @author hmei
 */
public class StringUtil {
    private static final Logger logger = Logger.getLogger(StringUtil.class);
    // replace spaces and . with
    private static Pattern trailingPeriodsPattern = Pattern.compile("[ \\.]+$");

    public static String removeTrailingPeriods(String sentence) {
        return trailingPeriodsPattern.matcher(sentence).replaceAll("");
    }

    // default characters for generateString
    // alphanumerics, punctuations, and spaces
    private static String DEFAULT_CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" +
                    "          " + //increase chance of spaces
                    "!.,?";

    private static Random rand = new Random();

    public static String generateString(int length) {
        return generateString(DEFAULT_CHARS, length);
    }

    public static String generateString(String passwordChars, int length) {
        // generate random string
        StringBuffer newPwd = new StringBuffer();
        for (int i = 0; i < length; ++i) {
            newPwd.append(
                    passwordChars.charAt( rand.nextInt( passwordChars.length() )) );
        }
        return newPwd.toString();
    }

    /**
     * A consistent split function that always returns N+1 strings, where
     * N is the number of delimiters.
     * Examples:
     * consistentSplit( "a,b,c" , "," ) --> {"a" "b" "c"}
     * consistentSplit( ",," , "," ) --> {"" "" ""}
     *
     * This utility is needed because java.lang.String.split()
     * can return a variable number of strings
     *
     * @input input string to split
     * @delim char delimiter
     * @return substrings split by delimiter
     */
    public static List<String> consistentSplit(String input, char delim) {
        List<String> retval = new ArrayList<String>();
        if (input != null) {
            int currIdx = 0;
            int nextDelimIdx = -1;
            while (-1 != (nextDelimIdx = input.indexOf(delim, currIdx))) {
                retval.add(input.substring(currIdx, nextDelimIdx));
                currIdx = nextDelimIdx + 1;
            }
            // add the last string
            retval.add(input.substring(currIdx));
        } else {
            return null;
        }
        return retval;
    }

    /**
     * Splits the input string using the specified delimiter.
     * Also allows you to specify an escape character, which is
     * used to escape the delimiter, and also itself.
     * i.e.
     *  escape-delim -> delim
     *  escape-escape -> escape
     *
     * Sample call:
     * split( 'dog|\\\||cat|', '|', '\' ) --> { 'dog', '\|', 'cat', '' }
     *
     * @param input
     * @param delim
     * @param escape character used to escape a delimiter or escape char
     * @return
     */
    public static List<String> split(String input, char delim, char escape) {
        List<String> retval = new ArrayList<String>();
        if (input != null) {

            StringBuilder currentString = new StringBuilder();

            boolean escapeMode = false;
            for (char curr : input.toCharArray()) {
                if (escapeMode) {
                    if (curr == escape || curr == delim) {
                        // accumulate this char
                        currentString.append(curr);
                    } else {
                        // accumulate both the previous escape char and this char
                        currentString.append(escape);
                        currentString.append(curr);
                    }
                    // always reset the escape state
                    escapeMode = false;
                } else {
                    // prev char was not an escape char
                    if (curr == escape) {
                        escapeMode = true;
                    } else if (curr == delim) {
                        // finish the current string
                        retval.add(currentString.toString());
                        // reset the currentString buffer
                        currentString.setLength(0);
                    } else {
                        // keep accumulating...
                        currentString.append(curr);
                    }
                }
            }

            // add the last string
            retval.add(currentString.toString());
        } else {
            return null;
        }
        return retval;
    }


    /**
     * Removes the escape character from an input string
     *
     * replaces:
     *  escape-delim -> delim
     *  escape-escape -> escape
     *
     * @param input
     * @param delim
     * @param escape character used to escape a delimiter or escape char
     * @return
     */
    public static String unescape(String input, char delim, char escape) {
        if (input != null) {
            StringBuilder retval = new StringBuilder();

            boolean escapeMode = false;
            for (char curr : input.toCharArray()) {
                if (escapeMode) {
                    // previous char was an escape string, so accumulate this char
                    if (curr == escape || curr == delim) {
                        // accumulate this char, skip the prev escap char
                        retval.append(curr);
                    } else {
                        // accumulate both the previous escape char and this
                        // char
                        retval.append(escape);
                        retval.append(curr);
                    }
                    // always reset the escape state
                    escapeMode = false;
                } else {
                    // prev char was not an escape char
                    if (curr == escape) {
                        // don't accumalate escape string
                        escapeMode = true;
                    } else {
                        retval.append(curr);
                    }
                }
            }

            return retval.toString();
        } else {
            return null;
        }
    }

    /**
     * Truncate the input string, if necessary
     * @param origStr
     * @param maxLength
     * @return
     */
    public static String truncate(String origStr, int maxLength) {
        return truncate(origStr, maxLength, null);
    }

    /**
     * Truncate the input string, if necessary
     * @param origStr
     * @param maxLength
     * @name - name of the "entity" - typically column name
     * @return
     */
    public static String truncate(String origStr, int maxLength, String name) {
        String retval = origStr; // by default, just return origStr

        if (origStr != null && origStr.length() > maxLength) {
            if (name != null) {
                if(logger.isDebugEnabled()){
                    logger.debug("TRUNCATE warning: truncating " + name + " whose value length = "
                            + origStr.length() + " exceeds max length " + maxLength);
                }
            }
            retval = origStr.substring(0, maxLength);
        }

        return retval;
    }

    /**
     * Truncate a csv string to <= the specified length
     *
     * does not break up any words between the delimiter
     *
     * default delimiter is comma
     *
     * @param origCsv
     * @param maxLength
     * @return
     */
    public static String csvTruncate(String origCsv, int maxLength) {
        return csvTruncate(origCsv, maxLength, ',');
    }

    /**
     * Truncate a csv string to <= the specified length
     *
     * does not break up any words between the delimiter
     *
     * @param origCsv
     * @param maxLength
     * @param delim
     * @return
     */
    public static String csvTruncate(String origCsv, int maxLength, char delim) {
        String retval = origCsv; // by default just return origStr

        if (origCsv != null && origCsv.length() > maxLength) {
            int endpoint = origCsv.lastIndexOf(delim, maxLength);
            if (endpoint == -1) {
                return ""; // no commas to break up!
            } else {
                return origCsv.substring(0, endpoint);
            }
        }

        return retval;
    }

    /**
     * Remove all control characters (\x00-\x1F\x7F) from the specified string.
     * @param s
     * @return
     */
    public static final String removeControlCharacters(String s) {
        if (null == s) { return null; }

    /*
    StringBuilder sb = new StringBuilder();
    int slen = s.length();
    for (int i = 0; i < slen; ++i) {
      char ch = s.charAt(i);
      if (ch > '\u001f' && ch != '\u007f') {
        sb.append(ch);
      }
    }
    */

    /*
    StringBuilder sb = new StringBuilder();
    StringCharacterIterator it = new StringCharacterIterator(s);
    for (char ch = it.first(); ch != CharacterIterator.DONE; ch = it.next()) {
      if (ch > '\u001f' && ch != '\u007f') {
        sb.append(ch);
      }
    }
    */

        StringBuilder sb = new StringBuilder();
        for (char ch : s.toCharArray()) {
            if (ch > '\u001f' && ch != '\u007f') {
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    /**
     * return the larger of 2 strings
     * @param a
     * @param b
     * @return
     */
    public static String max( String a, String b ){
        if( a == null ) return b;
        if( b == null ) return a;

        return a.compareTo(b) > 0 ? a : b;
    }

    /**
     * Check whether two String objects are equal, in the condition that they can be null
     */
    public static boolean isStringEqual(String a, String b){
        return (a == b) || (a != null && a.equals(b));
    }
}
