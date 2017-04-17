package com.yizhao.miniudcu.fileprocessor;


import com.yizhao.miniudcu.UDCUHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by yzhao on 4/11/17.
 */
public class UDCUFileProcessor {
    private static final Logger log = Logger.getLogger(UDCUFileProcessor.class);

    private Map<String, Boolean> processingFiles = new HashMap<String, Boolean>();
    private AtomicLong fileSeq = new AtomicLong(0);
    private ExecutorService validatorThreadPool = null;
    private UDCUHelper helper;

    public UDCUFileProcessor() {
    }

    public UDCUFileProcessor(UDCUHelper helper) {
        this.helper = helper;

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("validator-%d")
                .daemon(true)
                .priority(Thread.NORM_PRIORITY - 1)
                .build();
        validatorThreadPool = Executors.newSingleThreadExecutor(factory);
    }


    /**
     * This method keep processing the CSV files in INBOX dir until it's empty
     * - normal files, after processing, move it to the ARCHIVE dir
     * - out of sequence files, without processing, directly move it to the ERROR dir
     * <p>
     * After all files are done, it will return.
     */
    public void process() {
        try{
            File file;
            while ( (file=helper.getNextFile(processingFiles)) != null) {
                Map<String, ExecutorService> threadPools = helper.getThreadPools();
                ExecutorService threadPool = threadPools.get("yizhaolocalfile");
                if (threadPool == null) {
                    BasicThreadFactory factory = new BasicThreadFactory.Builder()
                            .namingPattern("yizhaolocalfile"+"-%d")
                            .daemon(true)
                            .priority(Thread.NORM_PRIORITY)
                            .build();
                    threadPool = Executors.newSingleThreadExecutor(factory);
                    threadPools.put("yizhaolocalfile", threadPool);
                }


                threadPool.execute(new FileProcessingTask(file, helper));
            }

        }catch(Exception e){
            // at this time, not sure what we can do with the Exception. log it
            log.error("UDCUFileProcessor failed to get the next file and process", e);
        }
    }


    public class FileProcessingTask implements Runnable {
        private File file;
        private UDCUHelper helper;
        private Boolean isValidator;
        private Boolean shouldWriteClog;

        private StrTokenizer pipeTokenizer;

        public FileProcessingTask(File file, UDCUHelper helper) {
            this.file = file;
            this.helper = helper;

            pipeTokenizer = StrTokenizer.getCSVInstance();
            pipeTokenizer.setDelimiterChar('|');
        }
        public void run() {

        }
    }
}