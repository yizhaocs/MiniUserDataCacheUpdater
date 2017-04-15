package com.yizhao.miniudcu.clog;

/**
 * Created by yzhao on 4/15/17.
 */
public interface CentralLogger{

    /**
     * The logger name, used to identify the logger
     */
    public String getName();

    /**
     * Log the object.
     *
     * @param loggable - loggable object.
     */
    public void log(Loggable loggable);

    /**
     * Is logging enabled
     */
    public boolean isEnabled();

}