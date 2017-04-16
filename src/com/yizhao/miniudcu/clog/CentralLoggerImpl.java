package com.yizhao.miniudcu.clog;

import com.yizhao.miniudcu.util.OtherUtils.ArgumentsUtil;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by yzhao on 4/15/17.
 */
public class CentralLoggerImpl implements CentralLogger {
    private static final Logger log = Logger.getLogger(CentralLoggerImpl.class);

    // our log file appender
    private BackupRollingAppender appender;

    private String  logfilePath;            // where the log files are going
    private String  rolloverPath;           // where the rolled logs will go
    private String  maxFileSize;            // the max log file size
    private int     rollOverInterval;       // how often we roll the logs
    private String  extension     = "csv";  // the extension to use on the logfile names (default=csv)
    private String  layoutPattern = "%m%n"; // the log message pattern, %m%n (message/n)

    private String loggerName;          // the name of this logger
    private boolean enabled;            // is the logger enabled

    private ClogDecorator clogDecorator = null;

    public void init(){
        ArgumentsUtil.validateDefined( loggerName, logfilePath, rolloverPath, maxFileSize );

        //by default, behave like ETL clogging
        if( clogDecorator == null ){
            clogDecorator = new ETLClogDecorator();
        }
        log.info( loggerName + " clog: using " + clogDecorator );

        // create our appender
        appender = addAppender(loggerName);
        log.info(this.toString());
    }

    // create the custom rolling appender
    private BackupRollingAppender addAppender(String loggerName){

        String activeFileName = loggerName + "." + extension;

        BackupRollingAppender appender =
                createRollingAppender(loggerName, new File(logfilePath), activeFileName, new PatternLayout(layoutPattern));

        Logger newLog = Logger.getLogger(loggerName);
        newLog.setAdditivity(false);
        newLog.setLevel(Level.ALL);

        newLog.addAppender(appender);

        return appender;
    }

    private BackupRollingAppender createRollingAppender(String name, File directory, String active, Layout layout) {

        BackupRollingAppender appender = new BackupRollingAppender( clogDecorator );

        String dirpath = directory.getAbsolutePath()+"/";

        appender.setBackupDir(rolloverPath);
        appender.setBackupIntervalSeconds(rollOverInterval);

        // Set up a rolling file appender with our layout
        appender.setName            (name);
        appender.setLayout          (layout         );
        appender.setMaxFileSize     (maxFileSize    );
        appender.setFile            (dirpath+active );
        appender.setMaxBackupIndex  (0              );

        appender.activateOptions();

        return appender;
    }

    /**
     * log a message
     */
    public void log(String message){
        if(enabled){
            Logger.getLogger(loggerName).log(Level.ALL, message);
        }
    }

    /**
     * log the loggable object
     */
    public void log(Loggable loggable) {
        if(enabled){
            Collection<Object> permanentAttributes = new ArrayList<Object>();
            Logger.getLogger(loggerName).log(
                    Level.ALL,
                    StringUtil.removeControlCharacters(loggable.toCsv(permanentAttributes)) );
        }
    }

    public void destroy(){
        // close our appender and flush remaining writes if buffered
        if(appender != null){
            appender.close();
        }
    }

    @Override
    public String toString() {
        return "CentralLoggerImpl [appender=" + appender + ", logfilePath="
                + logfilePath + ", rolloverPath=" + rolloverPath + ", maxFileSize="
                + maxFileSize + ", rollOverInterval=" + rollOverInterval
                + ", loggerName=" + loggerName + ", enabled=" + enabled + "]";
    }


    // DI setters

    public void setMaxFileSize(String maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public void setLogfilePath(String logfilePath) {
        this.logfilePath = logfilePath;
    }

    public void setRolloverPath(String rolloverPath) {
        this.rolloverPath = rolloverPath;
    }

    public void setRollOverInterval(int rollOverInterval) {
        this.rollOverInterval = rollOverInterval;
    }

    public void setLoggerName(String loggerName) {
        this.loggerName = loggerName;
    }

    public void setExtension(String extension) {
        if(extension.startsWith("."))
            this.extension = extension.substring(1);
        else
            this.extension = extension;
    }

    public void setLayoutPattern(String layoutPattern) {
        this.layoutPattern = layoutPattern;
    }

    public String getName() {
        return loggerName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    //add getters ( mainly for testing purposes )
    public String getLogfilePath() {
        return logfilePath;
    }

    public String getRolloverPath() {
        return rolloverPath;
    }

    public int getRollOverInterval() {
        return rollOverInterval;
    }

    /**
     * @param clogDecorator the clogDecorator to set
     */
    public final void setClogDecorator(ClogDecorator clogDecorator) {
        this.clogDecorator = clogDecorator;
    }

}
