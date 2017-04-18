package com.yizhao.miniudcu.fileposter;

import com.yizhao.miniudcu.util.ArgumentUtils.ArgumentsUtil;
import com.yizhao.miniudcu.util.PropertiesUtils.PropertiesUtil;
import com.yizhao.miniudcu.util.ThreadUtils.ThreadUtil;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Try to pick a Url based on pre-defined preferences
 * This routing only works for ETL files ( extracts table from the filename )
 * @author heng
 */
public class PreferenceUrlRouter implements UrlRouter {

    private static final Logger log = Logger.getLogger(PreferenceUrlRouter.class);

    //settable properties
    //configFile: properties files that contain the etl.loader.* properties
    private List<String> configFiles;
    //how often to refresh the url & preference settings
    private Integer refreshSeconds;


    // Derived from configFiles:
    // etl.loader.hosts -> urlList
    // etl.loader.hosts.downtime.threshold.seconds -> downtimeThresholdSeconds
    // etl.loader.hosts.preference.table.TABLE_NAME -> index of preferred host ( starting with 0 )
    //
    private List<String> urlList;
    //the downtime threshold.  Don't be too aggressive in designating a url as DOWN.
    private Integer downtimeThresholdSeconds;
    //map of table name to urlPreference
    // e.g: cookie_key_value -> loader1url,  impression -> loader2url, ...
    private Map<String, String> tableNameToUrlPreference;


    // For hosts on a current streak of failures,
    // this map records the timestamp when the failures started.
    // - on any successful transmission, will reset ( remove the entry )
    // - on any failed transmission, ADD entry, only if doesn't exist yet
    // Consider a host down if it's failed for more than "downtimeThresholdSeconds"
    //
    private final Map<String, Long> urlCurrentFailureTs = new ConcurrentHashMap<String, Long>();

    //single threaded scheduler to run refresh task
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public void init() throws Exception {
        ArgumentsUtil.validateDefined(configFiles);

        //load properties from configFile
        //recompute the etl.loader.hosts and table preference maps, and downtime threshold
        refreshUrlPreferences();

        ArgumentsUtil.validateTrue(urlList != null && urlList.size() > 0);
        ArgumentsUtil.validateTrue(refreshSeconds > 0);
        ArgumentsUtil.validateTrue(downtimeThresholdSeconds > 0);

        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                Thread.currentThread().setName("PreferenceUrlRouter.refreshUrlPreferences()");
                try {
                    refreshUrlPreferences();
                } catch (Exception e) {
                    log.error("PreferenceUrlRouter.refreshUrlPreferences()", e);
                }
            }
        }, refreshSeconds, refreshSeconds, TimeUnit.SECONDS);

        log.info("PreferenceUrlRouter.init() completed: " + this.toString());
    }

    public void destroy() {
        ThreadUtil.forceShutdownAfterWaiting(this.scheduler, "DiskMonitor Scheduler", 1, TimeUnit.SECONDS);
    }

    /**
     * Refresh URL host related properties
     * <p>
     * etl.loader.hosts -> urlList
     * etl.loader.hosts.downtime.threshold.seconds -> downtimeThresholdSeconds
     * etl.loader.hosts.preference.table.TABLE_NAME -> index of preferred host ( starting with 0 )
     */
    private void refreshUrlPreferences() throws Exception {

        List<String> newUrlList = null;
        Integer newDowntimeThresholdSeconds = null;
        Map<String, String> newTableNameToUrlPreference = new HashMap<String, String>();

        //first read the url list property, the table name preference depends on it
        for (String configFile : configFiles) {
            Properties props = PropertiesUtil.loadProperties(configFile);

            {
                String tmp = props.getProperty("etl.loader.hosts");
                if (tmp != null) {
                    newUrlList = splitAndTrim(tmp);
                }
            }

            {
                String tmp = props.getProperty("etl.loader.hosts.downtime.threshold.seconds");
                if (tmp != null) {
                    newDowntimeThresholdSeconds = Integer.parseInt(tmp);
                }
            }
        }

        if (newUrlList == null) {
            throw new Exception("no urls found. check etl.loader.hosts property in your config files");
        }

        // now read the preference by table
        // etl.loader.hosts.preference.table.TABLE_NAME -> index of preferred host
        //
        for (String configFile : configFiles) {
            Properties props = PropertiesUtil.loadProperties(configFile);

            for (Object p : props.keySet()) {
                String key = (String) p;
                if (key.startsWith("etl.loader.hosts.preference.table.")) {
                    String tableName = key.replace("etl.loader.hosts.preference.table.", "");
                    int hostIdx = Integer.parseInt(props.getProperty(key));
                    newTableNameToUrlPreference.put(tableName, newUrlList.get(hostIdx));
                }
            }

        }

        this.urlList = newUrlList;
        this.downtimeThresholdSeconds = newDowntimeThresholdSeconds;
        this.tableNameToUrlPreference = newTableNameToUrlPreference;

        if (log.isDebugEnabled()) {
            log.debug("PreferenceUrlRouter: refreshUrlPreferences() completed: " + this.toString());
        }
    }


    /* (non-Javadoc)
     * @see com.opinmind.webapp.fileposter.UrlRouter#chooseUrl()
     */
    public String chooseUrl(String fileName) {
        /*//extract tableName from fileName
        //expect "etl" style filename: TABLENAME_netezza.*.csv
        //
        fileName = fileName.toLowerCase();
        String tableName = EtlUtil.getTableName(fileName);
        if (tableName != null && tableName.length() > 0) {
            String preferredUrl = tableNameToUrlPreference.get(tableName);
            if (preferredUrl != null) {
                if (isUrlOk(preferredUrl)) {
                    return preferredUrl;
                } else {
                    log.debug("host NOT OK: " + preferredUrl + ". failed since: " + urlCurrentFailureTs.get(preferredUrl));
                }
            }
        } else {
            log.warn("Unable to extract tableName from fileName: " + fileName);
        }

        //by default, randomly pick a url
        return CollectionUtil.randomElement(urlList);*/
        return null;
    }

    public void recordSuccess(String url) {
        //success, remove from the failed map
        urlCurrentFailureTs.remove(url);
    }

    public void recordFailure(String url) {
        //
        // Record a failure by adding to the list ( if not already there )
        //
        if (urlCurrentFailureTs.containsKey(url)) {
            //already noted as failed, keep the existing timestamp
        } else {
            //new failure, note the current timestamp
            urlCurrentFailureTs.put(url, System.currentTimeMillis());
        }

    }

    /**
     * Is a url OK?
     * <p>
     * If latest attempts on this URL have all failed AND it's been failing for more than "downtimeThresholdSeconds"
     * -> NOT OK
     * else:
     * -> OK
     *
     * @param url
     * @return
     */
    private boolean isUrlOk(String url) {
        Long firstFailTs = urlCurrentFailureTs.get(url);
        if (firstFailTs != null &&
                firstFailTs < System.currentTimeMillis() - (downtimeThresholdSeconds * 1000L)) {
            //if first failed more than downtimeThresholdSeconds seconds ago
            // with no intervening successes ( success would've removed the entry )
            return false;
        }

        return true;
    }

    private static final List<String> splitAndTrim(String urlCsv) {
        String[] urls = urlCsv.split(",");
        //Note: didn't use CsvUtil.split() because we don't want to dedup, perfectly fine for a host to be repeated in the list
        List<String> retval = new ArrayList<String>(urls.length);
        for (String url : urls) {
            retval.add(url.trim());
        }
        return retval;
    }

    public void setRefreshSeconds(int refreshSeconds) {
        this.refreshSeconds = refreshSeconds;
    }

    public void setConfigFiles(List<String> configFiles) {
        this.configFiles = configFiles;
    }

    @Override
    public String toString() {
        return "PreferenceUrlRouter [refreshSeconds=" + refreshSeconds + ", urlList="
                + urlList + ", downtimeThresholdSeconds=" + downtimeThresholdSeconds + ", tableNameToUrlPreference="
                + tableNameToUrlPreference + ", urlCurrentFailureTs=" + urlCurrentFailureTs
                + ", configFiles=" + configFiles + "]";
    }

}