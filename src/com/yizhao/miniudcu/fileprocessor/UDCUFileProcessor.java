package com.yizhao.miniudcu.fileprocessor;


import com.yizhao.miniudcu.UDCUHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yzhao on 4/11/17.
 */
public class UDCUFileProcessor {
    private static final Logger log = Logger.getLogger(UDCUFileProcessor.class);

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

    }
}