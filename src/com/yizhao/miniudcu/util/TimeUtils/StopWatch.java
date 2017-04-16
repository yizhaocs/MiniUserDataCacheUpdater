package com.yizhao.miniudcu.util.TimeUtils;

import java.util.concurrent.TimeUnit;

/**
 * Provides "Stop watch" functionality to time the execution time of any piece
 * of code.
 *
 * @author rmenon
 */

public final class StopWatch {

    /** Start time of the event to be timed. */
    private long startTs = 0;

    // ------------------- PUBLIC METHODS ---------------------//
    /**
     * Returns the value of a {@link StopWatch} object.
     *
     * @return the value of a {@link StopWatch} object.
     */
    public static StopWatch getInstance() {
        return new StopWatch();
    }

    /**
     * Start the clock. Also stores the start time in milliseconds to find out
     * elapsed time relative to this start time later.
     *
     * @return the current time in milliseconds.
     */
    public final long startClock() {
        return (startTs = System.currentTimeMillis());
    }

    /**
     * What is the current elapsed time? ( relative to startTime )
     *
     * @return the elapsed time in milliseconds
     */
    public final long currentElapsed() {
        return System.currentTimeMillis() - startTs;
    }

    /**
     * What is the current elapsed time? ( relative to startTime ) Also reset the
     * start time
     *
     * @return the elapsed time in milliseconds
     */
    public final long currentElapsedResetStart() {
        long current = System.currentTimeMillis();
        long elapsed = current - startTs;
        startTs = current;
        return elapsed;
    }

    /**
     * Stop the clock.
     *
     * @return the current time in milliseconds.
     */
    public final long stopClock() {
        if (startTs == 0)
            return 0;
        long elapsed = System.currentTimeMillis() - startTs;
        startTs = 0;
        return elapsed;
    }

    /**
     * Return a String description of elapsed time in milliseconds.
     *
     * @return a String description of elapsed time in milliseconds.
     */
    public final String stopClockStr() {
        return stopClockStr(TimeUnit.MILLISECONDS);
    }

    /**
     * Return String format of elapsed time in given time unit.
     *
     * @param timeUnit the unit of time in which the elapsed time should be
     *          returned.
     * @return a String description of elapsed time in given time unit.
     */
    public final String stopClockStr(TimeUnit timeUnit) {
        return "Elapsed: " + timeUnit.convert(stopClock(), timeUnit) + " "
                + timeUnit.toString();
    }

    // ------------------- PROTECTED METHODS ------------------//
    // --------------- PACKAGE-PRIVATE METHODS ----------------//
    // ------------------- PRIVATE METHODS --------------------//
    /** Private Constructor to prevent instantiation. */
    private StopWatch() {
    }
}
