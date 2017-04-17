package com.yizhao.miniudcu;

import com.yizhao.miniudcu.util.ConstantUtil.Constants;
import com.yizhao.miniudcu.util.DatabaseUtil.DatabaseRefreshUtil;
import com.yizhao.miniudcu.util.FileUtils.FileCreateUtil;
import com.yizhao.miniudcu.util.GenericObjectUtils.Triplet;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
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
    private static final String[] fileExts = new String[]{".csv", ".force"};

    private static Set<Integer>	ekvKeys = new HashSet<Integer>();

    public void init() throws Exception {
        // Read properties files.
        (configProperties = new Properties()).load(new FileInputStream(configFile));

        statusMap = new HashMap<String, Long>();

        // create all data directories ( if not exist ).
        createDataDirectories();
        refresh();

        DBRefreshTask mDBRefreshTask = new DBRefreshTask();
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
        refreshThread.scheduleWithFixedDelay(mDBRefreshTask, refreshInterval, refreshInterval, TimeUnit.SECONDS); //等待refreshInterval后执行mDBRefreshTask，3s后任务结束，再等待2s（间隔时间-消耗时间），如果有空余线程时，再次执行该任务
        threadPools = new HashMap<String, ExecutorService>();
    }


    public void destroy() {
        refreshThread.shutdownNow();
    }

    public void refresh() throws SQLException {
        // populate the event keys and the netezza only keys
        ekvKeys = DatabaseRefreshUtil.populateDataProviderKeys(dataSource);
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
            waitMilliseconds = Integer.valueOf(configProperties.getProperty(Constants._WORKER_WAIT_MILLISECONDS).trim());
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

    public class DBRefreshTask implements Runnable{

        @SuppressWarnings("deprecation")
        public void run() {
            Thread.currentThread().setName("UDCUFileHelper");
            try {
                refresh();
            } catch (Exception e) {
                log.error("UDCUFileHelper.refresh:", e);
            }

        }
    }



    /**
     * get the next file.
     * <p>
     * we define the next file to be the file with the smallest timestamp in the
     * data dir
     * <p>
     * we assume the file name follows a naming convention:
     * [PIXELTIMESTAMP].[PIXELSERVERID].[SEQUENCE_NUMBER].[EXTRA].csv
     *
     * @return the qualified file null if we can't find one
     */
    public File getNextFile(Map<String, Boolean> processingFiles) {
        File nextFile = null;

        // we only process .csv files and .force files
        // .csv files are data files
        // .force files are manually moved back from the 'error' dir by
        // operation
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                for (String fileExt : fileExts) {
                    if (name.endsWith(fileExt)) {
                        return true;
                    }
                }

                return false;
            }
        };

        File directory = new File(configProperties.getProperty(Constants._INBOX_DIR).trim());

        synchronized (processingFiles) {
            File[] files = directory.listFiles(filter);

            // if inbox directory doesn't exist or is empty, we will return null
            if (files != null) {
                if (files.length > 0) {

                    // sort the dir by their timestamp
                    Arrays.sort(files, new Comparator<File>() {
                        public int compare(File f1, File f2) {
                            return f1.getName().compareTo(f2.getName());
                        }
                    });

                    // we will always get the first non-processing file to
                    // process
                    for (File file : files) {
                        if (processingFiles.get(file.getName()) == null) {
                            nextFile = file;

                            processingFiles.put(file.getName(), Boolean.TRUE);

                            break;
                        }
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("directory might be empty : " + directory);
                    }
                }
            } else {
                log.info("directory might not exist : " + directory);
            }
        }

        return nextFile;
    }
}