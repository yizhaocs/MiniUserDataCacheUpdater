package com.yizhao.miniudcu.util.OtherUtils;

import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yzhao on 4/15/17.
 */
public class TimeUtil {
    private static final Logger log = Logger.getLogger(TimeUtil.class);

    public static final long MS_PER_SEC = 1000;
    public static final long MS_PER_MIN = 60 * MS_PER_SEC;
    public static final long MS_PER_HOUR = 60 * MS_PER_MIN;
    public static final long MS_PER_DAY = 24 * MS_PER_HOUR;

    //approx value
    public static final long MS_PER_YEAR = 365 * MS_PER_DAY;

    //format: [int][ms/s/m/h/d]
    // e.g. 5d = 5 days
    //      100ms = 5 milliseconds
    private static Pattern timePattern = Pattern.compile( "(\\d+)(ms|s|m|h|d)" );

    /**
     * Converts a query
     * format: [integer][ms/s/m/h/d]
     * e.g. 5d = 5 days, 10m = 10 minutes
     * @param timeStr
     * @return epochTime (ms)
     */
    public static Long parse( String timeStr ){
        try{
            Matcher m = timePattern.matcher( timeStr.trim().toLowerCase() );
            if( m.matches() ){
                Long value = Long.parseLong( m.group( 1 ) );
                String unit = m.group( 2 );
                if( unit.equals( "ms" ) ){
                    //no multiplier
                }else if( unit.equals( "s" ) ){
                    value *= MS_PER_SEC;
                }else if( unit.equals( "m") ){
                    value *= MS_PER_MIN;
                }else if( unit.equals( "h") ){
                    value *= MS_PER_HOUR;
                }else if( unit.equals( "d") ){
                    value *= MS_PER_DAY;
                }

                return value;
            }
        }catch(Exception e){
            log.error( e );
        }

        return null;
    }

    /**
     * Round the epochTime down to the nearest multiple of the specified interval.
     *
     * WARNING: In practice, might not want to use for any interval > HOUR, since
     * it does not consider TimeZone specific features like daylight savings time.
     *
     * @param epochTime
     * @param interval
     * @return
     */
    public static long truncate( long epochTime, long interval ){
        return epochTime - ( epochTime % interval );
    }
}
