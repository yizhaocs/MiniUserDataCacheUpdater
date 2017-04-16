package com.yizhao.miniudcu.util.ThreadUtils;

import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created by yzhao on 4/11/17.
 */
public class ThreadUtil {
    private static final Logger logger = Logger.getLogger(ThreadUtil.class);

    /**
     * Force shutdown (if required) an executor service after waiting for the
     * given amount of time.
     *
     * @param executorService
     * @param serviceDescription
     * @param waitTime
     * @param waitTimeUnit
     * @return list of runnable threads that remained after a forced shutdown.
     */
    public static List<Runnable> forceShutdownAfterWaiting(ExecutorService executorService,
                                                           String serviceDescription, long waitTime, TimeUnit waitTimeUnit) {
        List<Runnable> remainingThreads = Collections.<Runnable>emptyList();
        if (executorService != null && !executorService.isTerminated()) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(waitTime, waitTimeUnit);
            } catch (InterruptedException e) {
                logger.debug("The service <" + serviceDescription + "> was interrupted while waiting termination");
            }
            if (!executorService.isTerminated()) {
                remainingThreads = executorService.shutdownNow();
                if (remainingThreads.size() > 0) {
                    logger.debug("Threads remaining after a forced shut down for " + serviceDescription + ": " + remainingThreads);
                }
            }
        }
        return remainingThreads;
    }

    public static void shutdown(ExecutorService executorService, String serviceDescription) {
        if (executorService != null && !executorService.isTerminated()) {
            logger.info("shutting down executor " + serviceDescription);
            executorService.shutdown();
        }
    }
}