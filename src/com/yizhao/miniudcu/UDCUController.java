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
    private String noOrderingHosts = null;

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
                UDCUDataProcessorFactory.getInstance().registerDataProcessor(
                        key,
                        dataProcessorMap.get(key));
            }

            // get the wait period for executor from the config file
            workerWaitMilliseconds = udcuHelper.getWorkerWaitMilliseconds();
        } catch (Exception e) {
            log.error("can't initialize the UDCU controller, abort");
            e.printStackTrace();
            return;
        }
        Set<String> noOrderingHostsSet = null;
        if (noOrderingHosts != null && noOrderingHosts.length() > 0) {
            noOrderingHostsSet = new HashSet<String>();
            log.info("no ordering for the following source : " + noOrderingHostsSet);
            String[] noOrderings = noOrderingHosts.split(",");
            for (String noOrdering : noOrderings) {
                noOrderingHostsSet.add(noOrdering.trim().toLowerCase());
            }
        }

        // we schedule the worker to process the files
        final UDCUFileProcessor worker = new UDCUFileProcessor(udcuHelper, noOrderingHostsSet);

        executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                worker.process();
            }
        }, 0L, workerWaitMilliseconds, TimeUnit.MILLISECONDS);
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

    public String getNoOrderingHosts() {
        return noOrderingHosts;
    }

    public void setNoOrderingHosts(String noOrderingHosts) {
        this.noOrderingHosts = noOrderingHosts;
    }
}