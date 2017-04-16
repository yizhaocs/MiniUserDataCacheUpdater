package com.yizhao.miniudcu.clog;


import org.apache.log4j.Logger;

/**
 * This will be a private member of BackupRollingAppender.
 *   Essentially, in the constructor, it is handed a handle
 *   to the BackupRollingAppender it will perform rollovers for.
 *   A ScheduledExecutorService within BackupRollingAppender
 *   will kickoff the run() method of this class at the appropriate
 *   intervals (which will execute appender.rollover() )
 *
 * @author Anit Agarwal
 *
 */
public class RolloverExecutor implements Runnable {

    private BackupRollingAppender appender;

    /**
     * Logger for this class.
     */
    private static final Logger logger = Logger.getLogger(RolloverExecutor.class);

    /**
     *
     * The purpose of this class is to pass in a handle
     * to a BackupRollingAppender, which we will then
     * execute rollover() on, every rolloverInterval
     * seconds (as scheduled by the ScheduledExecutorService
     * 			instance in BackupRollingAppender )
     *
     * @param appender -- a handle to the appender which we
     * 					will periodically force a rollover on
     *
     */
    public RolloverExecutor(BackupRollingAppender appender)
    {
        this.appender = appender;
    }

    public void run() {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing a rollover for BackupRollingAppender, into "
                    + " backup directory " + appender.getBackupDir() );
        }
        appender.rollOver();
    }

}
