package com.yizhao.miniudcu.clog;

import com.yizhao.miniudcu.util.FileUtils.FileRenameUtil;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by yzhao on 4/15/17.
 */
public final class BackupRollingAppender extends RollingFileAppender {

    private static final Logger logger = Logger.getLogger(BackupRollingAppender.class);

    private String backupDir;
    private int backupIntervalSeconds;
    private RolloverExecutor rExecutor = null;
    private static ScheduledExecutorService schedExecutorService = null;
    private static final int threadPoolSize = 1;
    private final ClogDecorator clogDecorator;


    public BackupRollingAppender( ClogDecorator clogDecorator ){
        this.clogDecorator = clogDecorator;
    }

    /**
     * @return the backupDir
     */
    public final String getBackupDir() {
        return backupDir;
    }

    /**
     * @param backupIntervalSeconds the backupInteval to set (in seconds)
     *
     *  when backup Interval is set, a RolloverExecutor will be instantiated with
     *  a handle to this BackupRollingAppender object and set to force a rollover
     *  every backupIntervalSeconds seconds.
     *
     *  The rExecutor will be managed by ScheduledThreadPoolExecutor stpExecutor
     */
    public final void setBackupIntervalSeconds(int backupIntervalSeconds) {
        if (backupIntervalSeconds > 0) {
            this.backupIntervalSeconds = backupIntervalSeconds;
            logger.info("Starting RolloverExecutor with backupIntervalSeconds = " + backupIntervalSeconds
                    + " backupDir = "  + backupDir);

            synchronized(BackupRollingAppender.class) {
                if (schedExecutorService == null) {
                    schedExecutorService = Executors.newScheduledThreadPool(threadPoolSize);
                }

                if (rExecutor == null) {
                    rExecutor = new RolloverExecutor(this);
                    schedExecutorService.scheduleAtFixedRate(rExecutor, backupIntervalSeconds, backupIntervalSeconds,
                            SECONDS);
                } else {
                    logger.error("Error -- trying to instantiate a RolloverExecutor when one "
                            + "already exists for this appender.  Cancelling operation.");
                }
            }
        }
    }

    /**
     * @return the backupIntervalSeconds
     */
    public final int getBackupIntervalSeconds() {
        return backupIntervalSeconds;
    }

    /**
     * @param backupDir the backupDir to set
     */
    public final void setBackupDir(String backupDir) {
        this.backupDir = backupDir;
    }

    /*
     * Bug 5534: move the file instead of copy.  Close file beforehand.
     * (non-Javadoc)
     * @see org.apache.log4j.RollingFileAppender#rollOver()
     */
    @Override
    public synchronized final void rollOver() {
        File file = new File(this.getFile());
        if (file.length() > 0) {
            // flush/close the log file being written to
            this.closeFile();

            try {
                //use clogDecorator to generate the rollover filename
                File dstFile = new File(backupDir, clogDecorator.generateRolloverFileName( file ));

                logger.info( "CLOG rollOver(), BEFORE RENAME, src: " + file + ", dst: " + dstFile + ", srcFileSize:" + file.length() );
                // move the file, instead of copy
                FileRenameUtil.renameFile(file, dstFile, true );
                logger.info( "CLOG rollOver(), AFTER RENAME, src: " + file + ", dst: " + dstFile + ", dstFileSize: " + dstFile.length() );

            } catch (IOException e) {
//    		throw new RuntimeException(String.format(
//    				"Can not back up file %s before rolling over", file), e);
                logger.error( "CLOG rollover(): Unable to rename file: " + file, e );
            } finally {

                // still need to call super.rollover(), which will recreate the file for new clogging.
                super.rollOver();

            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.RollingFileAppender#close()
     */
    @Override
    public void close() {
        logger.info("Closing BackupRollingAppender with backupDir = " + backupDir + "...");
        synchronized(BackupRollingAppender.class) {
            if (schedExecutorService != null) {
                schedExecutorService.shutdown();
                schedExecutorService = null;
            }
            super.close();
        }
        rollOver();
        logger.info("Done closing BackupRollingAppender with backupDir = " + backupDir + "...");
    }

}
