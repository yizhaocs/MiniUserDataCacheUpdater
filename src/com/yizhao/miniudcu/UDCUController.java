package com.yizhao.miniudcu;

import com.yizhao.miniudcu.dataprocessor.UDCUDataProcessor;
import com.yizhao.miniudcu.dataprocessor.UDCUDataProcessorFactory;
import com.yizhao.miniudcu.fileprocessor.UDCUFileProcessor;
import com.yizhao.miniudcu.cache.UserDataCache;
import com.yizhao.miniudcu.util.ThreadUtils.ThreadUtil;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yzhao on 4/10/17.
 */
public class UDCUController {
    private static final Logger log = Logger.getLogger(UDCUController.class);
    private static ScheduledThreadPoolExecutor executor;
    private Map<String, UDCUDataProcessor> dataProcessorMap = null;
    private UDCUHelper udcuHelper;
    private UserDataCache userDataCache = null;

    /**
     * This is the real entry point
     */
    public void init() {
        //
        // we do initialization here, if it fails, we won't continue
        //
        int workerWaitMilliseconds = -1;
        try {
            for (String key : dataProcessorMap.keySet()) {
                UDCUDataProcessorFactory.getInstance().registerDataProcessor(key, dataProcessorMap.get(key));
            }

            // get the wait period for executor from the config file
            workerWaitMilliseconds = udcuHelper.getWorkerWaitMilliseconds();
        } catch (Exception e) {
            log.error("can't initialize the UDCU controller, abort");
            e.printStackTrace();
            return;
        }
        Set<String> noOrderingHostsSet = null;

        // we schedule the worker to process the files
        final UDCUFileProcessor worker = new UDCUFileProcessor(udcuHelper);
        FileProcessTask mFileProcessTask = new FileProcessTask(worker);
        executor = new ScheduledThreadPoolExecutor(1);
        /**
         *  scheduleAtFixedRate()和scheduleWithFixedDelay方法参数是一样的。
         *  第一个参数是任务实例，第二个参数是延迟时间，第三个是间隔时间，第四个是时间单元。
         *  这两个方法的不同之处在方法名也能看得出来：
         *  scheduleAtFixedRate:
         *      scheduleAtFixedRate方法是按照固定频率去执行任务的。
         *      The scheduleAtFixedRate() method lets execute a task periodically after a fixed delay.
         *      The following block of code will execute a task after an initial delay of 10 seconds, and after that, it will execute the same task every 60 seconds. If the processor needs more time to execute an assigned task than the period parameter of the scheduleAtFixedRate() method, the ScheduledExecutorService will wait until the current task is completed before starting the next:
         *      Code:
         *          Try adding a Thread.sleep(1000); call within your run() method... Basically it's the difference between scheduling something based on when the previous execution ends and when it (logically) starts.
         *          For example, suppose I schedule an alarm to go off with a fixed rate of once an hour, and every time it goes off, I have a cup of coffee, which takes 10 seconds. Suppose that starts at midnight, I'd have:
         *          service.scheduleAtFixedRate(callableTask, 10, 60, TimeUnit.SECONDS);
         *      Output:
         *          00:00: Start making coffee
         *          00:10: Finish making coffee
         *          01:00: Start making coffee
         *          01:10: Finish making coffee
         *          02:00: Start making coffee
         *          02:10: Finish making coffee
         *
         *  scheduleWithFixedDelay:
         *      scheduleWithFixedDelay方法则是按照固定的延迟去执行任务:
         *      If it is necessary to have a fixed length delay between iterations of the task, scheduleWithFixedDelay() should be used. For example, the following code will guarantee a 60 seconds pause between the end of the current execution and the start of another one.
         *      Code:
         *          If I schedule with a fixed delay of 60 seconds, I'd have:
         *          service.scheduleWithFixedDelay(task, 10, 60, TimeUnit.SECONDS);
         *      Output:
         *          00:00: Start making coffee
         *          00:10: Finish making coffee
         *          01:10: Start making coffee
         *          01:20: Finish making coffee
         *          02:20: Start making coffee
         *          02:30: Finish making coffee
         *
         */
        executor.scheduleAtFixedRate(mFileProcessTask, 0L, workerWaitMilliseconds, TimeUnit.MILLISECONDS);
    }

    public void destroy() {
        ThreadUtil.forceShutdownAfterWaiting(executor, "UDCUController", 10L, TimeUnit.SECONDS);
        if (userDataCache != null) {
            userDataCache.destroy();
        }

        Map<String, ExecutorService> threadPools = udcuHelper.getThreadPools();
        for (String poolName : threadPools.keySet()) {
            ThreadUtil.forceShutdownAfterWaiting(threadPools.get(poolName), "UDCUController", 5000L, TimeUnit.MILLISECONDS);
        }

    }

    public UserDataCache getUserDataCache() {
        return userDataCache;
    }

    public void setUserDataCache(UserDataCache userDataCache) {
        this.userDataCache = userDataCache;
    }

    public Map<String, UDCUDataProcessor> getDataProcessorMap() {
        return dataProcessorMap;
    }

    public void setDataProcessorMap(Map<String, UDCUDataProcessor> dataProcessorMap) {
        this.dataProcessorMap = dataProcessorMap;
    }

    public UDCUHelper getUdcuHelper() {
        return udcuHelper;
    }

    public void setUdcuHelper(UDCUHelper udcuHelper) {
        this.udcuHelper = udcuHelper;
    }

    public class FileProcessTask implements Runnable {
        private UDCUFileProcessor worker;

        public FileProcessTask(UDCUFileProcessor worker) {
            this.worker = worker;
        }

        @SuppressWarnings("deprecation")
        public void run() {
            worker.process();
        }
    }
}