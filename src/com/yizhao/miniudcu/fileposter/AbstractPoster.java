package com.yizhao.miniudcu.fileposter;

import com.yizhao.miniudcu.util.CsvUtils.CsvUtil;
import com.yizhao.miniudcu.util.FileUtils.FileCreateUtil;
import com.yizhao.miniudcu.util.FileUtils.FileDeleteUtil;
import com.yizhao.miniudcu.util.ThreadUtils.ThreadUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.log4j.Logger.getLogger;

/**
 * Created by yzhao on 4/15/17.
 */
public abstract class AbstractPoster implements Poster {
    private static final Logger logger = getLogger(AbstractPoster.class);

    private ScheduledExecutorService posterScheduler = Executors.newScheduledThreadPool(1);
    private ExecutorService workerScheduler; /* Scheduler that schedules a worker thread for a given table. */

    private Set<String> filePrefixes;

    private File inboxDir;

    private File errorDir;

    private PosterWorker posterWorker;

    private int scanIntervalSeconds;

    private int maxConcurrentPosters;

    private Semaphore threadThrottler;

    private final Set<File> filesBeingPosted = new LinkedHashSet<File>();

    protected abstract File[] getFiles(File dir) throws Exception;

    /* (non-Javadoc)
     * @see com.opinmind.webapp.etlext.Poster#donePosting(java.io.File)
     */
    public void donePosting(File file) throws Exception {
        synchronized (filesBeingPosted) {
            filesBeingPosted.remove(file);
        }
        threadThrottler.release();
    }

    /* (non-Javadoc)
     * @see com.opinmind.webapp.etlext.Poster#getPendingFiles()
     */
    public final Collection<File> getPendingFiles() throws Exception {
        Collection<File> files = new ArrayList<File>();
        File[] inboxFiles = getFiles(inboxDir);
        for (File file : inboxFiles) {
            files.add(file);
        }
        File[] errorFiles = getFiles(errorDir);
        for (File file : errorFiles) {
            files.add(file);
        }
        return files;
    }

    public void init() throws Exception {
        logger.info("Initializing PosterImpl...inboxDir: " + inboxDir + ", errorDir: " + errorDir);

        threadThrottler = new Semaphore(maxConcurrentPosters);
        workerScheduler = Executors.newScheduledThreadPool(maxConcurrentPosters);
        posterScheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    scanAndPostFilesInBackground();
                } catch (Exception e) {
                    logger.error(String.format("failed to post files"), e);
                } finally {
                    logger.trace("end of posting files...");
                }
            }
        }, 0, scanIntervalSeconds, TimeUnit.SECONDS);
    }

    public void destroy() throws Exception {
        logger.info("Destroying PosterImpl...");
        // we do not force shut down so all pending files are posted.
        ThreadUtil.shutdown(workerScheduler, "Poster worker Sceduler");
        ThreadUtil.shutdown(posterScheduler, "Poster Sceduler");
        // we need to release the semaphore since we do not use the interruptible acquire
        int retries = 0;
        while(threadThrottler.hasQueuedThreads()){
            threadThrottler.release();
            if(retries++ > 10){
                break;
            }
            Thread.sleep(1000);
        }
    }

    /**
     * @param filePrefixes the filePrefixes to set
     */
    public final void setFilePrefixes(String filePrefixes) {
        this.filePrefixes = new LinkedHashSet<String>(CsvUtil.csvToCollection(filePrefixes, "|"));
    }

    /**
     * @param filePrefixes the filePrefixes to set
     */
    public final void setFilePrefixes(Set<String> filePrefixes) {
        if (filePrefixes != null) {
            this.filePrefixes = filePrefixes;
        } else {
            this.filePrefixes = new LinkedHashSet<String>();
        }
    }

    /**
     * @param inboxDir the inboxDir to set
     */
    public final void setInboxDir(String inboxDir) throws Exception {
        this.inboxDir = new File(inboxDir);
        FileCreateUtil.createDirectory(this.inboxDir);
    }

    /**
     * @param posterWorker the posterWorker to set
     */
    public final void setPosterWorker(PosterWorker posterWorker) {
        this.posterWorker = posterWorker;
    }

    /**
     * @param scanIntervalSeconds the scanIntervalSeconds to set
     */
    public final void setScanIntervalSeconds(int scanIntervalSeconds) {
        this.scanIntervalSeconds = scanIntervalSeconds;
    }

    /**
     * @param maxConcurrentPosters the maxConcurrentPosters to set
     */
    public final void setMaxConcurrentPosters(int maxConcurrentPosters) {
        this.maxConcurrentPosters = maxConcurrentPosters;
    }

    /**
     * @param errorDir the errorDir to set
     */
    public final void setErrorDir(String errorDir) throws Exception {
        this.errorDir = new File(errorDir);
        FileCreateUtil.createDirectory(this.errorDir);
    }

    protected Set<String> getFilePrefixes() {
        return filePrefixes;
    }

    private void scanAndPostFilesInBackground() throws Exception {
        Collection<File> filesToBePosted = getFilesToBePosted();
        if (!filesToBePosted.isEmpty()) {
            for (final File file : filesToBePosted) {
                logger.info(String.format("waiting to acquire semaphore for posting file %s", file));
                threadThrottler.acquire();
                logger.info(String.format("acquired semaphore for posting file %s", file));
                this.workerScheduler.submit(new Runnable() {
                    public void run() {
                        try {
                            posterWorker.post(file);
                        } catch (Exception e) {
                            logger.error(String.format("Failed to post file %s - will retry after "
                                    + scanIntervalSeconds + " seconds", e));
                        }
                    }
                });
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("No tables to be processed...");
            }
        }
    }

    private Collection<File> getFilesToBePosted() throws Exception {
        List<File> files = new ArrayList<File>();
        File[] inboxFiles = getFiles(inboxDir);
        File[] errorFiles = getFiles(errorDir);
        addFiles(inboxFiles, files);
        addFiles(errorFiles, files);
        return files;
    }

    private void addFiles(File[] filesToBeAdded, List<File> files) throws Exception {
        for (File file : filesToBeAdded) {
            if (file.length() > 0) {
                synchronized (filesBeingPosted) {
                    if (!filesBeingPosted.contains(file)) {
                        files.add(file);
                        filesBeingPosted.add(file);
                    }
                }
            } else {
                logger.info("Deleting zero length file " + file);
                FileDeleteUtil.deleteFile(file);
            }
        }
    }
}
