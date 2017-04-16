package com.yizhao.miniudcu.util.ThreadUtils;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by yzhao on 4/10/17.
 */
public class ThreadSafeDateFormat extends ThreadLocal<DateFormat> {

    private String format = null;
    private TimeZone tz = null;

    public ThreadSafeDateFormat(String format, TimeZone tz) {
        this.format = format;
        this.tz = tz;
    }

    /* (non-Javadoc)
     * @see java.lang.ThreadLocal#initialValue()
     */
    @Override
    protected DateFormat initialValue() {
        DateFormat df = new SimpleDateFormat(format);
        if (tz != null) {
            df.setTimeZone(tz);
        }
        return df;
    }
}