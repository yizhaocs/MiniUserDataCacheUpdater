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
         *  scheduleAtFixedRate方法是按照固定频率去执行任务的。
         *  而scheduleWithFixedDelay方法则是按照固定的延迟去执行任务。
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

    public class FileProcessTask implements Runnable{
        private UDCUFileProcessor worker;

        public FileProcessTask(UDCUFileProcessor worker){
            this.worker = worker;
        }

        @SuppressWarnings("deprecation")
        public void run() {
            worker.process();
        }
    }
}