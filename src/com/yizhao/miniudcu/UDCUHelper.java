package com.yizhao.miniudcu;

import com.yizhao.miniudcu.util.ConstantUtil.Constants;
import com.yizhao.miniudcu.util.DatabaseUtil.DatabaseRefreshUtil;
import com.yizhao.miniudcu.util.FileUtils.FileCreateUtil;
import com.yizhao.miniudcu.util.GenericObjectUtils.Triplet;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by yzhao on 4/10/17.
 */
public class UDCUHelper {
    private static final Logger log = Logger.getLogger(UDCUHelper.class);
    // config properties files
    private static Properties configProperties;
    private static Map<String, Long> statusMap;
    private final ScheduledExecutorService refreshThread = Executors.newSingleThreadScheduledExecutor();
    private Map<String, ExecutorService> threadPools;
    // config files locations, we need to load them in constructor
    private String configFile = null; //"/opt/opinmind/conf/udcuv2/config.properties";
    private int refreshInterval = 60; // seconds
    private DataSource dataSource = null;

    private static Set<Integer>	ekvKeys = new HashSet<Integer>();
    private static Set<Integer>	ckvKeys = new HashSet<Integer>();
    private static Set<Integer> bidgenKeys = new HashSet<Integer>();

    public void init() throws Exception {
        // Read properties files.
        (configProperties = new Properties()).load(new FileInputStream(configFile));

        statusMap = new HashMap<String, Long>();

        // create all data directories ( if not exist ).
        createDataDirectories();
        refresh();

        refreshThread.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                Thread.currentThread().setName("UDCUFileHelper");
                try {
                    refresh();
                } catch (Exception e) {
                    log.error("UDCUFileHelper.refresh:", e);
                }

            }
        }, refreshInterval, refreshInterval, TimeUnit.SECONDS);
        threadPools = new HashMap<String, ExecutorService>();
    }


    public void destroy() {
        refreshThread.shutdownNow();
    }

    public void refresh() throws SQLException {
        // populate the event keys and the netezza only keys
        Triplet<Set<Integer>, Set<Integer>, Set<Integer>> keySets = DatabaseRefreshUtil.populateDataProviderKeys(dataSource);
        // populate the netezza only keys set
        ekvKeys = keySets.getFirst();
        ckvKeys = keySets.getSecond();
        bidgenKeys = keySets.getThird();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public Map<String, ExecutorService> getThreadPools() {
        return threadPools;
    }

    /**
     * get the period between the worker gets scheduled to process data files
     *
     * @return the period value
     */
    public int getWorkerWaitMilliseconds() {
        int waitMilliseconds = -1;

        if (configProperties != null) {
            waitMilliseconds = Integer.valueOf(configProperties.getProperty(
                    Constants._WORKER_WAIT_MILLISECONDS).trim());
        }

        return waitMilliseconds;
    }

    private void createDataDirectories() throws IOException {
        File inbox = new File(configProperties.getProperty(Constants._INBOX_DIR));
        File archive = new File(configProperties.getProperty(Constants._ARCHIVE_DIR));
        File error = new File(configProperties.getProperty(Constants._ERROR_DIR));

        File[] directories = { inbox, archive, error };

        FileCreateUtil.createDirectories(directories);
    }

}