package com.yizhao.miniudcu.util;

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * Created by yzhao on 4/15/17.
 */
public class RetryUtil {
    private static final Logger log = Logger.getLogger(RetryUtil.class);

    //By default - wait 5 seconds after failure to retry
    private static final long DEFAULT_RETRY_WAIT_MS = 5000L;

    /**
     * Keep calling the function until it is successful.
     * @param <T> return value of function
     * @param func Callable to call()
     * @param warnMsg message to log if a call threw an exception
     * @return
     */
    public static <T> T repeatUntilSuccessful(Callable<T> func, String warnMsg ){
        return repeatUntilSuccessful( func, warnMsg, DEFAULT_RETRY_WAIT_MS );
    }

    /**
     * Keep calling the function until it is successful.
     * @param <T> return value of function
     * @param func Callable to call()
     * @param warnMsg message to log if a call threw an exception
     * @return
     */
    public static <T> T repeatUntilSuccessful( Callable<T> func, String warnMsg, long retryWaitMs ){
        while( true ){
            try{
                return func.call();
            }catch(Exception e){
                log.warn( warnMsg + ", sleep(" + retryWaitMs + ")", e );
                try {
                    Thread.sleep( retryWaitMs );
                }catch(InterruptedException ie){}
            }
        }
    }

    /**
     * Make a method call catching exceptions and retrying up to the maxRetries count
     * If the maxRetries is exceeded then the exception is thrown back to the caller to handle.
     * The retries will occur at an interval specified by the retryWaitMs parameter.
     */
    public static <T> T withRetries( Callable<T> func, String warnMsg, long retryWaitMs, long maxRetries ) throws Exception{
        int tries = 0;

        while(true){
            try{
                return func.call();
            }catch(Exception ex){
                log.warn( warnMsg + ", sleep(" + retryWaitMs + ")", ex );
                if(++tries > maxRetries){
                    throw ex;
                }
                try {
                    Thread.sleep( retryWaitMs );
                }catch(InterruptedException ie){}
            }
        }
    }
}
