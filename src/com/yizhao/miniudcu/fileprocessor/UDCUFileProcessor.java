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
    private UDCUHelper helper;

    public UDCUFileProcessor() {
    }

    public UDCUFileProcessor(UDCUHelper helper) {
        this.helper = helper;
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
                    /**
                     * 实现ThreadFactory接口生成自定义线程.
                     * 工厂模式是最常用的模式之一，在创建线程的时候，我们当然也能使用工厂模式来生产Thread，这样就能替代默 认的new Thread，而且在自定义工厂里面，我们能创建自定义化的Thread，并且计数，或则限制创建Thread的数量， 给每个Thread设置对应的好听的名字，或则其他的很多很多事情，总之就是很爽
                     */
                    BasicThreadFactory factory = new BasicThreadFactory.Builder()
                            .namingPattern("yizhaolocalfile"+"-%d")
                            .daemon(true)
                            .priority(Thread.NORM_PRIORITY)
                            .build();
                    /**
                     * 执行者框架（Executor framework）是一种机制，它允许你将线程的创建与执行分离。它是基于Executor、ExecutorService接口和实现这两个接口的ThreadPoolExecutor类。
                     * 在执行者框架（Executor framework）的内部，它提供一个ThreadFactory接口来创建线程，这是用来产生新的线程。
                     */
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